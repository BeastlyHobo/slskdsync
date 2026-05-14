import os
import re
import sqlite3
import threading
import time
import shutil
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

_log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, _log_level, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("slskdsync")
logger.info(f"Log level: {_log_level} (override with LOG_LEVEL env var)")

import requests
from flask import Flask, render_template, request, redirect, session, url_for, flash, jsonify, send_from_directory
from werkzeug.security import check_password_hash, generate_password_hash
from dotenv import load_dotenv

load_dotenv()

APP_DIR = Path(__file__).resolve().parent
DATA_DIR = APP_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "app.db"

AUDIO_EXTS = {".flac", ".mp3", ".m4a", ".ogg", ".aac", ".wav", ".aif", ".aiff", ".opus", ".wma"}

# Fallback Monochrome/hifi-api instances tried in order when the configured one returns 403
MONOCHROME_FALLBACK_URLS = [
    "https://hifi.geeked.wtf",
    "https://monochrome-api.samidy.com",
    "https://api.monochrome.tf",
]


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.executescript("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS import_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            source_type TEXT NOT NULL,
            source_url TEXT NOT NULL,
            nav_playlist INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL,
            artist TEXT,
            album TEXT,
            title TEXT,
            track_number INTEGER,
            source_id TEXT,
            cover_url TEXT,
            download_source TEXT DEFAULT 'slskd',
            slskd_search_id TEXT,
            slskd_state TEXT DEFAULT 'pending',
            slskd_error TEXT,
            local_path TEXT,
            FOREIGN KEY(job_id) REFERENCES import_jobs(id)
        );
    """)
    # Migrate existing databases
    existing_cols = {row[1] for row in cur.execute("PRAGMA table_info(tracks)")}
    for col, ddl in [
        ("cover_url", "TEXT"),
        ("download_source", "TEXT DEFAULT 'slskd'"),
        ("slskd_search_id", "TEXT"),
    ]:
        if col not in existing_cols:
            cur.execute(f"ALTER TABLE tracks ADD COLUMN {col} {ddl}")

    defaults = {
        "library_path": "/music",
        "slskd_url": "http://slskd:5030",
        "slskd_user": "",
        "slskd_pass": "",
        "slskd_api_key": "",
        "monochrome_url": "https://hifi.geeked.wtf",
        "navidrome_url": "http://navidrome:4533",
        "navidrome_user": "",
        "navidrome_pass": "",
        "quality": "lossless",
        "replace_existing": "0",
        "folder_template": "{artist}/{album}/{title}{ext}",
        "download_watch_path": "/downloads",
        # auth stored in DB (empty = not configured yet → first-run setup)
        "app_username": "",
        "app_password_hash": "",
    }
    for k, v in defaults.items():
        cur.execute("INSERT OR IGNORE INTO settings(key,value) VALUES (?,?)", (k, v))
    conn.commit()
    conn.close()


def get_setting(key: str) -> str:
    conn = get_conn()
    row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    conn.close()
    return row[0] if row else ""


def set_setting(key: str, value: str):
    conn = get_conn()
    conn.execute(
        "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )
    conn.commit()
    conn.close()


def is_first_run() -> bool:
    """True when no password has been configured in DB and no env override is present."""
    if get_setting("app_password_hash"):
        return False
    if os.getenv("APP_PASSWORD_HASH"):
        return False
    pw = os.getenv("APP_PASSWORD", "admin")
    # "admin" unchanged from default → still needs setup
    return pw == "admin"


def get_auth_credentials() -> tuple[str, str]:
    """Return (username, password_hash) from DB if set, else from env."""
    db_hash = get_setting("app_password_hash")
    db_user = get_setting("app_username")
    if db_hash and db_user:
        return db_user, db_hash
    env_hash = os.getenv("APP_PASSWORD_HASH") or generate_password_hash(os.getenv("APP_PASSWORD", "admin"))
    env_user = os.getenv("APP_USER", "admin")
    return env_user, env_hash


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class TrackMeta:
    artist: str
    album: str
    title: str
    track_number: int = 0
    source_id: str = ""
    cover_url: str = ""


# ---------------------------------------------------------------------------
# Music source providers (URL import)
# ---------------------------------------------------------------------------

class SpotifyProvider:
    name = "spotify"

    def __init__(self):
        self.client = None
        cid = os.getenv("SPOTIFY_CLIENT_ID")
        secret = os.getenv("SPOTIFY_CLIENT_SECRET")
        if cid and secret:
            import spotipy
            from spotipy.oauth2 import SpotifyClientCredentials
            self.client = spotipy.Spotify(
                auth_manager=SpotifyClientCredentials(client_id=cid, client_secret=secret)
            )

    def supports(self, url: str) -> bool:
        return "open.spotify.com" in url

    def parse(self, url: str) -> tuple[str, list[TrackMeta]]:
        if not self.client:
            raise RuntimeError("Spotify credentials missing — set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET.")

        if "/playlist/" in url:
            pid = url.split("/playlist/")[1].split("?")[0]
            data = self.client.playlist_items(pid, additional_types=["track"])
            tracks = []
            for item in data.get("items", []):
                t = item.get("track") or {}
                artist = ", ".join(a["name"] for a in t.get("artists", [])) or "Unknown Artist"
                album = (t.get("album") or {}).get("name") or "Unknown Album"
                cover = ((t.get("album") or {}).get("images") or [{}])[0].get("url", "")
                tracks.append(TrackMeta(artist=artist, album=album, title=t.get("name") or "Unknown",
                                        track_number=t.get("track_number") or 0,
                                        source_id=t.get("id") or "", cover_url=cover))
            return "playlist", tracks

        if "/album/" in url:
            aid = url.split("/album/")[1].split("?")[0]
            album = self.client.album(aid)
            album_name = album.get("name") or "Unknown Album"
            cover = (album.get("images") or [{}])[0].get("url", "")
            tracks = []
            for t in album.get("tracks", {}).get("items", []):
                artist = ", ".join(a["name"] for a in t.get("artists", [])) or "Unknown Artist"
                tracks.append(TrackMeta(artist=artist, album=album_name, title=t.get("name") or "Unknown",
                                        track_number=t.get("track_number") or 0,
                                        source_id=t.get("id") or "", cover_url=cover))
            return "album", tracks

        if "/track/" in url:
            tid = url.split("/track/")[1].split("?")[0]
            t = self.client.track(tid)
            artist = ", ".join(a["name"] for a in t.get("artists", [])) or "Unknown Artist"
            album = (t.get("album") or {}).get("name") or "Unknown Album"
            cover = ((t.get("album") or {}).get("images") or [{}])[0].get("url", "")
            return "track", [TrackMeta(artist=artist, album=album, title=t.get("name") or "Unknown",
                                       track_number=t.get("track_number") or 0,
                                       source_id=t.get("id") or "", cover_url=cover)]

        if "/artist/" in url:
            aid = url.split("/artist/")[1].split("?")[0]
            top = self.client.artist_top_tracks(aid)
            tracks = []
            for t in top.get("tracks", []):
                artist = ", ".join(a["name"] for a in t.get("artists", [])) or "Unknown Artist"
                album = (t.get("album") or {}).get("name") or "Unknown Album"
                cover = ((t.get("album") or {}).get("images") or [{}])[0].get("url", "")
                tracks.append(TrackMeta(artist=artist, album=album, title=t.get("name") or "Unknown",
                                        track_number=t.get("track_number") or 0,
                                        source_id=t.get("id") or "", cover_url=cover))
            return "artist", tracks

        raise RuntimeError("Unsupported Spotify URL type")


class AppleProvider:
    name = "apple"

    def supports(self, url: str) -> bool:
        return "music.apple.com" in url

    def parse(self, url: str) -> tuple[str, list[TrackMeta]]:
        song_match = re.search(r"[?&]i=(\d+)", url)
        if song_match:
            sid = song_match.group(1)
            data = requests.get("https://itunes.apple.com/lookup", params={"id": sid}, timeout=20).json()
            result = (data.get("results") or [{}])[0]
            cover = result.get("artworkUrl100", "").replace("100x100bb", "300x300bb")
            return "track", [TrackMeta(
                artist=result.get("artistName", "Unknown Artist"),
                album=result.get("collectionName", "Unknown Album"),
                title=result.get("trackName", "Unknown Title"),
                track_number=result.get("trackNumber", 0),
                source_id=str(result.get("trackId", "")),
                cover_url=cover,
            )]
        raise RuntimeError("Apple Music album/playlist import requires an Apple Music API token (not configured).")


class TidalProvider:
    name = "tidal"

    def supports(self, url: str) -> bool:
        return "tidal.com" in url

    def parse(self, url: str) -> tuple[str, list[TrackMeta]]:
        base = (get_setting("monochrome_url") or "https://hifi.geeked.wtf").rstrip("/")

        if "/track/" in url:
            m = re.search(r"/track/(\d+)", url)
            if not m:
                raise RuntimeError("Could not parse TIDAL track ID from URL")
            tid = m.group(1)
            r = requests.get(f"{base}/info/", params={"id": tid}, timeout=15)
            if r.status_code != 200:
                raise RuntimeError(f"Monochrome API error {r.status_code} — try setting Monochrome URL to https://hifi.geeked.wtf in Settings")
            d = r.json()
            return "track", [TrackMeta(
                artist=(d.get("artist") or {}).get("name", "Unknown Artist"),
                album=(d.get("album") or {}).get("title", "Unknown Album"),
                title=d.get("title", "Unknown Title"),
                track_number=d.get("trackNumber", 0),
                source_id=tid,
                cover_url=_tidal_cover_url((d.get("album") or {}).get("cover", "")),
            )]

        if "/album/" in url:
            m = re.search(r"/album/(\d+)", url)
            if not m:
                raise RuntimeError("Could not parse TIDAL album ID from URL")
            aid = m.group(1)
            # /album/?id=… returns combined album info + tracks
            r = requests.get(f"{base}/album/", params={"id": aid, "limit": 100}, timeout=15)
            if r.status_code != 200:
                raise RuntimeError(f"Monochrome API error {r.status_code}")
            d = r.json()
            album_name = d.get("title", "Unknown Album")
            cover = _tidal_cover_url(d.get("cover", ""))
            items = d.get("items") or (d.get("tracks") or {}).get("items", [])
            tracks = []
            for t in items:
                tracks.append(TrackMeta(
                    artist=(t.get("artist") or d.get("artist") or {}).get("name", "Unknown Artist"),
                    album=album_name,
                    title=t.get("title", "Unknown Title"),
                    track_number=t.get("trackNumber", 0),
                    source_id=str(t.get("id", "")),
                    cover_url=cover,
                ))
            return "album", tracks

        if "/playlist/" in url:
            m = re.search(r"/playlist/([a-f0-9-]+)", url)
            if not m:
                raise RuntimeError("Could not parse TIDAL playlist ID")
            pid = m.group(1)
            r = requests.get(f"{base}/playlist/", params={"id": pid, "limit": 100}, timeout=15)
            if r.status_code != 200:
                raise RuntimeError(f"Monochrome API error {r.status_code}")
            d = r.json()
            items = d.get("items") or (d.get("tracks") or {}).get("items", [])
            tracks = []
            for t in items:
                cover = _tidal_cover_url((t.get("album") or {}).get("cover", ""))
                tracks.append(TrackMeta(
                    artist=(t.get("artist") or {}).get("name", "Unknown Artist"),
                    album=(t.get("album") or {}).get("title", "Unknown Album"),
                    title=t.get("title", "Unknown Title"),
                    track_number=t.get("trackNumber", 0),
                    source_id=str(t.get("id", "")),
                    cover_url=cover,
                ))
            return "playlist", tracks

        raise RuntimeError("Unsupported TIDAL URL — paste a /track/, /album/, or /playlist/ link")


def _tidal_cover_url(cover_id: str) -> str:
    if not cover_id:
        return ""
    return f"https://resources.tidal.com/images/{cover_id.replace('-', '/')}/320x320.jpg"


# ---------------------------------------------------------------------------
# Deezer — free catalog search (no auth required)
# ---------------------------------------------------------------------------

class DeezerProvider:
    def _get(self, path: str, **params) -> dict:
        r = requests.get(f"https://api.deezer.com{path}", params=params, timeout=12)
        r.raise_for_status()
        return r.json()

    def search_tracks(self, query: str, limit: int = 25) -> list[dict]:
        try:
            data = self._get("/search", q=query, limit=limit)
            return [
                {
                    "id": str(i.get("id", "")),
                    "title": i.get("title", ""),
                    "artist": i.get("artist", {}).get("name", ""),
                    "artist_id": str(i.get("artist", {}).get("id", "")),
                    "album": i.get("album", {}).get("title", ""),
                    "album_id": str(i.get("album", {}).get("id", "")),
                    "cover": i.get("album", {}).get("cover_medium", ""),
                    "duration": i.get("duration", 0),
                    "type": "track",
                }
                for i in data.get("data", [])
            ]
        except Exception:
            return []

    def search_artists(self, query: str, limit: int = 20) -> list[dict]:
        try:
            data = self._get("/search/artist", q=query, limit=limit)
            return [
                {
                    "id": str(i.get("id", "")),
                    "name": i.get("name", ""),
                    "picture": i.get("picture_medium", ""),
                    "nb_album": i.get("nb_album", 0),
                    "type": "artist",
                }
                for i in data.get("data", [])
            ]
        except Exception:
            return []

    def search_albums(self, query: str, limit: int = 20) -> list[dict]:
        try:
            data = self._get("/search/album", q=query, limit=limit)
            return [
                {
                    "id": str(i.get("id", "")),
                    "title": i.get("title", ""),
                    "artist": i.get("artist", {}).get("name", ""),
                    "artist_id": str(i.get("artist", {}).get("id", "")),
                    "cover": i.get("cover_medium", ""),
                    "nb_tracks": i.get("nb_tracks", 0),
                    "type": "album",
                }
                for i in data.get("data", [])
            ]
        except Exception:
            return []

    def get_artist(self, artist_id: str) -> dict:
        artist = self._get(f"/artist/{artist_id}")
        albums_data = self._get(f"/artist/{artist_id}/albums", limit=50)
        return {
            "id": str(artist_id),
            "name": artist.get("name", ""),
            "picture": artist.get("picture_medium", ""),
            "nb_fan": artist.get("nb_fan", 0),
            "albums": [
                {
                    "id": str(a.get("id", "")),
                    "title": a.get("title", ""),
                    "cover": a.get("cover_medium", ""),
                    "release_date": a.get("release_date", ""),
                    "nb_tracks": a.get("nb_tracks", 0),
                    "type": "album",
                }
                for a in albums_data.get("data", [])
            ],
        }

    def get_album(self, album_id: str) -> dict:
        album = self._get(f"/album/{album_id}")
        tracks_data = self._get(f"/album/{album_id}/tracks", limit=100)
        album_artist = album.get("artist", {}).get("name", "")
        return {
            "id": str(album_id),
            "title": album.get("title", ""),
            "artist": album_artist,
            "artist_id": str(album.get("artist", {}).get("id", "")),
            "cover": album.get("cover_medium", ""),
            "release_date": album.get("release_date", ""),
            "tracks": [
                {
                    "id": str(t.get("id", "")),
                    "title": t.get("title", ""),
                    "artist": (t.get("artist") or {}).get("name", "") or album_artist,
                    "duration": t.get("duration", 0),
                    "track_position": t.get("track_position", 0),
                    "type": "track",
                }
                for t in tracks_data.get("data", [])
            ],
        }


# ---------------------------------------------------------------------------
# Monochrome — TIDAL proxy client
# ---------------------------------------------------------------------------

class MonochromeClient:
    """Client for the hifi-api / monochrome TIDAL proxy.

    Endpoint shape (verified against monochrome-music/hifi-api-workers):
      GET /search/?s=QUERY&limit=N        → {"data": {"items": [...]}}
      GET /info/?id=ID                    → TIDAL track metadata
      GET /track/?id=ID&quality=LOSSLESS  → TIDAL playbackinfo (base64 manifest)
      GET /album/?id=ID&limit=100         → album info + tracks combined
      GET /playlist/?id=UUID&limit=100    → playlist info + tracks combined
    """

    def __init__(self, base: str = None):
        self.base = (base or get_setting("monochrome_url") or "https://hifi.geeked.wtf").rstrip("/")

    def search_tracks(self, query: str, limit: int = 10) -> list[dict]:
        url = f"{self.base}/search/"
        try:
            r = requests.get(url, params={"s": query, "limit": limit}, timeout=12)
            logger.debug(f"[mono] GET {url}?s={query!r} → {r.status_code}")
            if r.status_code != 200:
                logger.warning(f"[mono] Search failed: {r.status_code} {r.text[:200]}")
                return []
            # Response is wrapped: {"version": "...", "data": {"items": [...]}}
            payload = r.json()
            items = (payload.get("data") or {}).get("items", [])
            # Some hifi-api deployments still use the legacy shape — fall back
            if not items:
                items = (payload.get("tracks") or {}).get("items", [])
            return items
        except Exception as ex:
            logger.warning(f"[mono] Search exception: {ex}")
            return []

    def find_tidal_id(self, artist: str, title: str) -> Optional[str]:
        items = self.search_tracks(f"{artist} {title}", limit=5)
        if not items:
            logger.info(f"[mono] No TIDAL results for '{artist} — {title}'")
            return None
        title_l = title.lower()
        artist_l = artist.lower().split(",")[0].strip()
        for item in items:
            if title_l in item.get("title", "").lower() and artist_l in (item.get("artist") or {}).get("name", "").lower():
                return str(item["id"])
        return str(items[0]["id"])

    def download_track(self, tidal_id: str, artist: str, title: str) -> tuple[bool, str]:
        quality_map = {"lossless": "LOSSLESS", "high": "HIGH", "normal": "HIGH", "low": "LOW"}
        quality = quality_map.get(get_setting("quality"), "LOSSLESS")
        track_url = f"{self.base}/track/"
        try:
            r = requests.get(track_url, params={"id": tidal_id, "quality": quality}, timeout=30)
            logger.debug(f"[mono] GET {track_url}?id={tidal_id}&quality={quality} → {r.status_code}")
            if r.status_code != 200:
                return False, f"Monochrome /track/ returned {r.status_code}: {r.text[:150]}"
            data = r.json()

            # Resolve a downloadable URL from the various response shapes
            stream_url = self._extract_stream_url(data, quality)
            if not stream_url:
                # Helpful diagnostic for the user
                keys = list(data.keys()) if isinstance(data, dict) else type(data).__name__
                return False, f"No downloadable URL in response (keys={keys}); HI_RES uses DRM, try LOSSLESS quality"

            ext = ".flac" if quality in ("LOSSLESS", "HI_RES_LOSSLESS") else ".m4a"
            watch = Path(get_setting("download_watch_path"))
            watch.mkdir(parents=True, exist_ok=True)
            safe = re.sub(r'[<>:"/\\|?*]', "", f"{artist} - {title}").strip()[:180]
            dest = watch / f"{safe}{ext}"

            logger.info(f"[mono] Streaming {stream_url[:80]}… → {dest.name}")
            with requests.get(stream_url, stream=True, timeout=300) as resp:
                resp.raise_for_status()
                with open(dest, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=65536):
                        f.write(chunk)
            return True, str(dest)
        except Exception as ex:
            logger.warning(f"[mono] Download exception: {ex}")
            return False, str(ex)

    @staticmethod
    def _extract_stream_url(data: dict, quality: str) -> Optional[str]:
        """Pull a usable URL out of a TIDAL playbackinfo response.

        Shapes seen in the wild:
          * {"url": "https://..."}                     (legacy)
          * {"urls": ["https://..."]}                  (legacy multi)
          * {"OriginalTrackUrl": "https://..."}
          * {"manifest": "<base64 JSON or MPD>", "manifestMimeType": "..."}
        """
        if not isinstance(data, dict):
            return None
        for key in ("url", "OriginalTrackUrl", "originalTrackUrl"):
            if data.get(key):
                return data[key]
        urls = data.get("urls")
        if isinstance(urls, list) and urls:
            return urls[0]

        manifest_b64 = data.get("manifest")
        mime = (data.get("manifestMimeType") or "").lower()
        if manifest_b64 and "vnd.tidal.bts" in mime:
            try:
                import base64, json as _json
                decoded = base64.b64decode(manifest_b64).decode("utf-8", "replace")
                bts = _json.loads(decoded)
                bts_urls = bts.get("urls") or []
                if bts_urls:
                    return bts_urls[0]
            except Exception as ex:
                logger.debug(f"[mono] Failed to decode bts manifest: {ex}")

        # DASH (HI_RES_LOSSLESS) is DRM-protected and can't be downloaded directly
        return None


# ---------------------------------------------------------------------------
# slskd client
# ---------------------------------------------------------------------------

class SlskdClient:
    def __init__(self):
        self.base = get_setting("slskd_url").rstrip("/")
        self.user = get_setting("slskd_user")
        self.password = get_setting("slskd_pass")
        self.api_key = get_setting("slskd_api_key")

    def _headers(self) -> dict:
        h = {"Content-Type": "application/json"}
        if self.api_key:
            h["X-API-Key"] = self.api_key
        return h

    def _auth(self):
        return (self.user, self.password) if self.user else None

    @staticmethod
    def _build_query(artist: str, title: str) -> str:
        # First artist only (before comma, &, feat.)
        artist = re.split(r',|&|\bfeat\.|\bft\.', artist, flags=re.IGNORECASE)[0].strip()
        # Strip parenthetical/bracketed suffixes — "(From X)", "[OST]", etc.
        title = re.sub(r'\s*[\(\[][^\)\]]*[\)\]]', '', title).strip()
        # Strip "feat." and everything after from title
        title = re.sub(r'\s*(feat\.|ft\.).*$', '', title, flags=re.IGNORECASE).strip()
        return ' '.join(f"{artist} {title}".split())[:100]

    def ping(self) -> tuple[bool, str]:
        for ep in ["/api/v0/application/version", "/api/v0/application", "/api/v1/application"]:
            url = f"{self.base}{ep}"
            try:
                r = requests.get(url, headers=self._headers(), auth=self._auth(), timeout=8)
                if r.status_code < 300:
                    try:
                        d = r.json()
                        ver = d if isinstance(d, str) else (d.get("version") or d.get("server", {}).get("version", ""))
                    except Exception:
                        ver = r.text.strip()[:30]
                    return True, f"Connected — slskd {ver}".strip(" —")
                if r.status_code == 401:
                    return False, "Auth required (set API key or username/password in settings)"
            except Exception as ex:
                last_err = str(ex)
        return False, locals().get("last_err", "Could not connect to slskd")

    def start_search(self, track: TrackMeta) -> tuple[bool, str, str]:
        query = self._build_query(track.artist, track.title)
        last_err = "Could not reach slskd"
        # slskd wants {"searchText": "..."} (camelCase) — not "query"
        body = {
            "searchText": query,
            "fileLimit": 10000,
            "filterResponses": True,
            "responseLimit": 100,
            "searchTimeout": 15000,
        }
        for ep in ["/api/v0/searches", "/api/v1/searches"]:
            url = f"{self.base}{ep}"
            try:
                r = requests.post(url, headers=self._headers(), auth=self._auth(),
                                  json=body, timeout=25)
                logger.debug(f"[slskd] POST {url} → {r.status_code} {r.text[:200]}")
                if r.status_code < 300:
                    return True, str(r.json().get("id", "")), "search started"
                last_err = f"HTTP {r.status_code} from {ep}: {r.text[:150]}"
            except Exception as ex:
                last_err = str(ex)
        return False, "", last_err

    def get_search_results(self, search_id: str) -> list[dict]:
        if not search_id:
            return []
        try:
            # First check if search is complete
            check_url = f"{self.base}/api/v0/searches/{search_id}"
            r = requests.get(check_url, headers=self._headers(), auth=self._auth(), timeout=15)
            logger.debug(f"[slskd] GET {check_url} → {r.status_code}")
            if r.status_code != 200:
                logger.warning(f"[slskd] Search status check failed: {r.status_code} {r.text[:200]}")
                return []
            search_obj = r.json()
            if not search_obj.get("isComplete"):
                return []  # still running
            # Fetch responses (NOT /files — that endpoint doesn't exist)
            resp_url = f"{self.base}/api/v0/searches/{search_id}/responses"
            r2 = requests.get(resp_url, headers=self._headers(), auth=self._auth(), timeout=15)
            logger.debug(f"[slskd] GET {resp_url} → {r2.status_code}")
            if r2.status_code != 200:
                logger.warning(f"[slskd] Failed to fetch responses: {r2.status_code} {r2.text[:200]}")
                return []
            flat = []
            for user_response in r2.json():
                username = user_response.get("username", "")
                has_slot = user_response.get("hasFreeUploadSlot", False)
                upload_speed = user_response.get("uploadSpeed", 0)
                for f in user_response.get("files", []):
                    flat.append({
                        "username": username,
                        "filename": f.get("filename", ""),
                        "size": f.get("size", 0),
                        "bitRate": f.get("bitRate", 0),
                        "length": f.get("length", 0),
                        "has_slot": has_slot,
                        "upload_speed": upload_speed,
                    })
            return flat
        except Exception as ex:
            logger.warning(f"[slskd] Exception fetching results: {ex}")
            return []

    def score_result(self, result: dict, track: TrackMeta) -> int:
        fn_l = result.get("filename", "").lower()
        basename_l = fn_l.replace("\\", "/").rsplit("/", 1)[-1]
        ext = basename_l.rsplit(".", 1)[-1] if "." in basename_l else ""
        if ext not in {"flac", "mp3", "m4a", "ogg", "aac", "wav", "aif", "aiff", "opus", "wma"}:
            return -100
        score = {"flac": 100, "wav": 80, "aif": 80, "aiff": 80, "m4a": 65, "ogg": 55, "opus": 55}.get(ext, 0)
        if ext == "mp3":
            br = result.get("bitRate", 0)
            score = 60 if br >= 320 else 50 if br >= 256 else 40 if br >= 192 else 30
        title_l = (track.title or "").lower()
        artist_l = (track.artist or "").lower().split(",")[0].strip()
        if title_l and title_l in basename_l:
            score += 30
        if artist_l and artist_l in fn_l:
            score += 20
        if result.get("has_slot"):
            score += 5
        return score

    def download_file(self, username: str, filename: str, size: int) -> tuple[bool, str]:
        url = f"{self.base}/api/v0/transfers/downloads/{username}"
        body = [{"filename": filename, "size": size}]
        try:
            r = requests.post(url, headers=self._headers(), auth=self._auth(),
                              json=body, timeout=25)
            logger.debug(f"[slskd] POST {url} → {r.status_code} body={body!r}")
            if r.status_code < 300:
                return True, "download queued"
            return False, f"HTTP {r.status_code}: {r.text[:200]}"
        except Exception as ex:
            return False, str(ex)


# ---------------------------------------------------------------------------
# File organizer
# ---------------------------------------------------------------------------

class Organizer:
    @staticmethod
    def target_path(track: sqlite3.Row, src_path: Path) -> Path:
        library_str = get_setting("library_path") or "/music"
        library = Path(library_str)
        artist = (track["artist"] or "Unknown Artist").strip().replace("/", "-")
        album = (track["album"] or "Unknown Album").strip().replace("/", "-")
        title = (track["title"] or src_path.stem).strip().replace("/", "-")
        ext = src_path.suffix
        track_num = track["track_number"] or 0

        tmpl = get_setting("folder_template") or ""
        if tmpl:
            try:
                rel = tmpl.format(
                    artist=artist, album=album, track_number=track_num,
                    title=title, ext=ext,
                )
                return library / rel
            except (KeyError, ValueError):
                pass  # fall through to default

        # Default: Artist/Album/NN - Title.ext  (omit number prefix when unknown)
        if track_num:
            filename = f"{track_num:02d} - {title}{ext}"
        else:
            filename = f"{title}{ext}"
        return library / artist / album / filename

    @staticmethod
    def move_file(src: Path, dst: Path) -> tuple[bool, str]:
        dst.parent.mkdir(parents=True, exist_ok=True)
        if dst.exists() and get_setting("replace_existing") != "1":
            return True, str(dst)  # already there — treat as success
        shutil.move(str(src), str(dst))
        return True, str(dst)


def discover_download_for_track(track: sqlite3.Row) -> Optional[Path]:
    watch = Path(get_setting("download_watch_path"))
    if not watch.exists():
        logger.warning(f"[discover] Watch path does not exist: {watch}")
        return None
    title = (track["title"] or "").lower().strip()
    artist = (track["artist"] or "").lower().split(",")[0].strip()

    # Collect all audio files once so we can log useful diagnostics
    audio_files = [f for f in watch.glob("**/*")
                   if f.is_file() and f.suffix.lower() in AUDIO_EXTS]
    if not audio_files:
        logger.warning(f"[discover] Watch path {watch} exists but contains no audio files")
        return None

    title_match = None
    path_match = None  # title found in full path but not filename
    for f in audio_files:
        n = f.name.lower()
        full_l = str(f).lower().replace("\\", "/")
        if title and title in n:
            if artist and artist in n:
                logger.debug(f"[discover] Exact match: {f.name}")
                return f
            if title_match is None:
                title_match = f
        elif title and title in full_l and path_match is None:
            path_match = f  # title is in a parent folder name

    best = title_match or path_match
    if best:
        logger.debug(f"[discover] Fuzzy match for '{title}': {best.name}")
        return best

    sample = [f.name for f in audio_files[:5]]
    logger.info(f"[discover] No match for '{title}' among {len(audio_files)} files. Sample: {sample}")
    return None


# ---------------------------------------------------------------------------
# Background worker
# ---------------------------------------------------------------------------

def run_worker(stop_event: threading.Event):
    logger.info("Worker started")
    while not stop_event.is_set():
        try:
            _worker_tick()
        except Exception as ex:
            logger.error(f"Worker tick error: {ex}")
        time.sleep(20)


def _worker_tick():
    conn = get_conn()
    slskd = SlskdClient()

    # pending slskd → start search
    for t in conn.execute(
        "SELECT * FROM tracks WHERE slskd_state='pending'"
        " AND (download_source='slskd' OR download_source IS NULL) LIMIT 10"
    ).fetchall():
        meta = TrackMeta(t["artist"] or "", t["album"] or "", t["title"] or "",
                         t["track_number"] or 0, t["source_id"] or "")
        query = SlskdClient._build_query(meta.artist, meta.title)
        logger.info(f"[slskd] Starting search: {query!r}")
        ok, search_id, msg = slskd.start_search(meta)
        if ok:
            logger.info(f"[slskd] Search queued (id={search_id}): {meta.title}")
            conn.execute(
                "UPDATE tracks SET slskd_state='queued', slskd_search_id=?, slskd_error=NULL WHERE id=?",
                (search_id, t["id"]),
            )
        else:
            logger.warning(f"[slskd] Search failed for '{meta.title}': {msg}")
            conn.execute(
                "UPDATE tracks SET slskd_state='failed', slskd_error=? WHERE id=?",
                (f"slskd: {msg}", t["id"]),
            )
        conn.commit()

    # queued slskd → poll results, auto-download best
    for t in conn.execute(
        "SELECT * FROM tracks WHERE slskd_state='queued' AND slskd_search_id IS NOT NULL LIMIT 10"
    ).fetchall():
        meta = TrackMeta(t["artist"] or "", t["album"] or "", t["title"] or "",
                         t["track_number"] or 0, t["source_id"] or "")
        results = slskd.get_search_results(t["slskd_search_id"])
        if not results:
            continue  # search still running
        logger.info(f"[slskd] {len(results)} results for '{meta.title}', selecting best")
        scored = sorted(
            ((slskd.score_result(r, meta), i, r) for i, r in enumerate(results)),
            key=lambda x: x[0],
            reverse=True,
        )
        if scored and scored[0][0] > 0:
            best = scored[0][2]
            logger.info(f"[slskd] Downloading from {best['username']}: {best['filename']}")
            ok, msg = slskd.download_file(best["username"], best["filename"], best.get("size", 0))
            if ok:
                conn.execute("UPDATE tracks SET slskd_state='downloading' WHERE id=?", (t["id"],))
            else:
                logger.warning(f"[slskd] Download request failed for '{meta.title}': {msg}")
                conn.execute("UPDATE tracks SET slskd_state='failed', slskd_error=? WHERE id=?",
                             (f"Download failed: {msg}", t["id"]))
        else:
            logger.warning(f"[slskd] No usable files found for '{meta.title}'")
            conn.execute(
                "UPDATE tracks SET slskd_state='failed', slskd_error='No usable files found in search results' WHERE id=?",
                (t["id"],),
            )
        conn.commit()

    # downloading → check watch folder, organize
    library = get_setting("library_path") or "/music"
    for t in conn.execute(
        "SELECT * FROM tracks WHERE slskd_state='downloading' LIMIT 20"
    ).fetchall():
        candidate = discover_download_for_track(t)
        if candidate:
            logger.info(f"[slskd] Found file for '{t['title']}': {candidate.name}")
            target = Organizer.target_path(t, candidate)
            logger.info(f"[slskd] Moving to library ({library}): {target}")
            ok, result = Organizer.move_file(candidate, target)
            logger.info(f"[slskd] Organized to: {result}")
            conn.execute("UPDATE tracks SET slskd_state='completed', local_path=? WHERE id=?",
                         (result, t["id"]))
            conn.commit()
        else:
            logger.info(f"[slskd] Still waiting for '{t['title']}' to appear in {get_setting('download_watch_path')}")

    # pending monochrome → lookup TIDAL ID if needed, then download with fallback instances
    mc = MonochromeClient()
    for t in conn.execute(
        "SELECT * FROM tracks WHERE slskd_state='pending' AND download_source='monochrome' LIMIT 5"
    ).fetchall():
        tidal_id = t["source_id"] or ""
        logger.info(f"[monochrome] Processing: {t['artist']} — {t['title']}")
        if not tidal_id:
            logger.info(f"[monochrome] Looking up TIDAL ID for '{t['title']}'")
            tidal_id = mc.find_tidal_id(t["artist"] or "", t["title"] or "") or ""
            if tidal_id:
                logger.info(f"[monochrome] Found TIDAL ID {tidal_id} for '{t['title']}'")
                conn.execute("UPDATE tracks SET source_id=? WHERE id=?", (tidal_id, t["id"]))
                conn.commit()
            else:
                logger.warning(f"[monochrome] Track not found on TIDAL: {t['artist']} — {t['title']}")
                # Fall back to slskd
                logger.info(f"[monochrome] Falling back to slskd for '{t['title']}'")
                conn.execute(
                    "UPDATE tracks SET download_source='slskd', slskd_state='pending',"
                    " slskd_error='TIDAL lookup failed; falling back to Soulseek' WHERE id=?",
                    (t["id"],),
                )
                conn.commit()
                continue

        conn.execute("UPDATE tracks SET slskd_state='downloading' WHERE id=?", (t["id"],))
        conn.commit()

        # Build ordered list of instances to try: configured first, then fallbacks
        configured = mc.base
        instances = [configured] + [u.rstrip("/") for u in MONOCHROME_FALLBACK_URLS if u.rstrip("/") != configured]

        ok, result = False, "No instances available"
        for instance_url in instances:
            if instance_url != configured:
                logger.info(f"[monochrome] Trying fallback instance: {instance_url}")
            mc_inst = MonochromeClient(base=instance_url)
            ok, result = mc_inst.download_track(tidal_id, t["artist"] or "", t["title"] or "")
            if ok:
                break
            logger.warning(f"[monochrome] Instance {instance_url} failed: {result[:100]}")
            # Only retry on upstream/auth errors (403, 401, 500); stop on DRM/format errors
            if not any(code in result for code in ("403", "401", "500", "Upstream", "upstream")):
                break

        if ok:
            src = Path(result)
            if src.exists():
                target = Organizer.target_path(t, src)
                move_ok, move_result = Organizer.move_file(src, target)
                final = move_result if move_ok else result
            else:
                final = result
            logger.info(f"[monochrome] Completed: {final}")
            conn.execute("UPDATE tracks SET slskd_state='completed', local_path=? WHERE id=?", (final, t["id"]))
        else:
            logger.warning(f"[monochrome] All instances failed for '{t['title']}': {result}")
            # Fall back to slskd
            logger.info(f"[monochrome] Falling back to slskd for '{t['title']}'")
            conn.execute(
                "UPDATE tracks SET download_source='slskd', slskd_state='pending',"
                " slskd_error='TIDAL download failed on all instances; falling back to Soulseek' WHERE id=?",
                (t["id"],),
            )
        conn.commit()

    conn.close()


# ---------------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------------

app = Flask(__name__)
app.secret_key = os.getenv("APP_SECRET", "change-me")

init_db()

_stop_event = threading.Event()
_worker = threading.Thread(target=run_worker, args=(_stop_event,), daemon=True)
_worker.start()

_providers = [SpotifyProvider(), AppleProvider(), TidalProvider()]
_deezer = DeezerProvider()


def is_authed() -> bool:
    return session.get("authed") is True


UNPROTECTED = {"/login", "/setup", "/sw.js", "/manifest.json"}


@app.before_request
def gate():
    if request.path.startswith("/static") or request.path in UNPROTECTED:
        return
    # First-run: redirect to setup before login is even possible
    if is_first_run() and request.path != "/setup":
        return redirect(url_for("setup"))
    if not is_authed():
        return redirect(url_for("login"))


# ---------------------------------------------------------------------------
# Auth & setup
# ---------------------------------------------------------------------------

@app.route("/setup", methods=["GET", "POST"])
def setup():
    # If already configured, only allow access when logged in
    if not is_first_run() and not is_authed():
        return redirect(url_for("login"))
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        confirm = request.form.get("confirm", "")
        if not username:
            flash("Username is required", "error")
        elif len(password) < 6:
            flash("Password must be at least 6 characters", "error")
        elif password != confirm:
            flash("Passwords do not match", "error")
        else:
            set_setting("app_username", username)
            set_setting("app_password_hash", generate_password_hash(password))
            flash("Password set — please sign in", "ok")
            return redirect(url_for("login"))
    return render_template("setup.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if is_first_run():
        return redirect(url_for("setup"))
    username, pw_hash = get_auth_credentials()
    if request.method == "POST":
        if (request.form.get("username") == username
                and check_password_hash(pw_hash, request.form.get("password", ""))):
            session["authed"] = True
            return redirect(url_for("search"))
        flash("Invalid credentials", "error")
    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# ---------------------------------------------------------------------------
# Static PWA files
# ---------------------------------------------------------------------------

@app.route("/sw.js")
def service_worker():
    return send_from_directory(app.static_folder, "sw.js", mimetype="application/javascript")


@app.route("/manifest.json")
def manifest():
    return send_from_directory(app.static_folder, "manifest.json", mimetype="application/manifest+json")


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    conn = get_conn()
    tracks = conn.execute("SELECT * FROM tracks ORDER BY id DESC LIMIT 100").fetchall()
    conn.close()
    stats = {"total": 0, "pending": 0, "downloading": 0, "completed": 0, "failed": 0}
    for t in tracks:
        s = t["slskd_state"] or "pending"
        stats["total"] += 1
        if s in ("pending", "queued"):
            stats["pending"] += 1
        elif s in stats:
            stats[s] += 1
    return render_template("index.html", tracks=tracks, stats=stats)


@app.route("/search")
def search():
    return render_template("search.html")


@app.route("/settings", methods=["GET", "POST"])
def settings():
    keys = [
        "library_path", "download_watch_path", "folder_template",
        "slskd_url", "slskd_user", "slskd_pass", "slskd_api_key",
        "monochrome_url",
        "navidrome_url", "navidrome_user", "navidrome_pass",
        "quality", "replace_existing",
        "app_username", "app_password_hash",
    ]
    if request.method == "POST":
        for k in keys:
            if k == "app_password_hash":
                # Only update password if a new one was typed
                new_pw = request.form.get("new_password", "").strip()
                if new_pw:
                    if len(new_pw) < 6:
                        flash("Password must be at least 6 characters", "error")
                        return redirect(url_for("settings"))
                    set_setting("app_password_hash", generate_password_hash(new_pw))
            else:
                set_setting(k, request.form.get(k, ""))
        flash("Settings saved", "ok")
        return redirect(url_for("settings"))
    return render_template("settings.html", settings={k: get_setting(k) for k in keys})


# ---------------------------------------------------------------------------
# Search API
# ---------------------------------------------------------------------------

@app.route("/api/search")
def api_search():
    q = request.args.get("q", "").strip()
    kind = request.args.get("type", "track")
    if not q:
        return jsonify([])
    if kind == "artist":
        return jsonify(_deezer.search_artists(q))
    if kind == "album":
        return jsonify(_deezer.search_albums(q))
    return jsonify(_deezer.search_tracks(q))


@app.route("/api/artist/<artist_id>")
def api_artist(artist_id):
    try:
        return jsonify(_deezer.get_artist(artist_id))
    except Exception as ex:
        return jsonify({"error": str(ex)}), 500


@app.route("/api/album/<album_id>")
def api_album(album_id):
    try:
        return jsonify(_deezer.get_album(album_id))
    except Exception as ex:
        return jsonify({"error": str(ex)}), 500


# ---------------------------------------------------------------------------
# Connection tests
# ---------------------------------------------------------------------------

@app.route("/api/test/slskd")
def test_slskd():
    ok, msg = SlskdClient().ping()
    return jsonify({"ok": ok, "message": msg})


@app.route("/api/test/monochrome")
def test_monochrome():
    mc = MonochromeClient()
    try:
        r = requests.get(f"{mc.base}/search/", params={"s": "daft punk", "limit": 1}, timeout=8)
        if r.status_code < 300:
            payload = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
            n = len((payload.get("data") or payload.get("tracks") or {}).get("items", []))
            return jsonify({"ok": True, "message": f"Connected — {mc.base} ({n} test results)"})
        return jsonify({"ok": False, "message": f"HTTP {r.status_code}: {r.text[:120]}"})
    except Exception as ex:
        return jsonify({"ok": False, "message": str(ex)})


@app.route("/api/debug/watch")
def debug_watch():
    watch = Path(get_setting("download_watch_path") or "/downloads")
    if not watch.exists():
        return jsonify({"error": f"Watch path does not exist: {watch}"})
    audio_files = sorted(
        [str(f.relative_to(watch)) for f in watch.glob("**/*")
         if f.is_file() and f.suffix.lower() in AUDIO_EXTS]
    )
    return jsonify({"watch": str(watch), "count": len(audio_files), "files": audio_files[:100]})


@app.route("/api/test/paths")
def test_paths():
    results = {}
    for key, label in [("download_watch_path", "downloads"), ("library_path", "library")]:
        path_str = get_setting(key) or ""
        if not path_str:
            results[label] = {"ok": False, "message": "Not configured"}
            continue
        p = Path(path_str)
        if not p.exists():
            try:
                p.mkdir(parents=True, exist_ok=True)
                results[label] = {"ok": True, "message": f"{p} — created (did not exist)"}
            except Exception as ex:
                results[label] = {"ok": False, "message": f"{p} — cannot create: {ex}"}
        elif not p.is_dir():
            results[label] = {"ok": False, "message": f"{p} — exists but is not a directory"}
        else:
            # Check write access
            test_file = p / ".slskdsync_write_test"
            try:
                test_file.touch()
                test_file.unlink()
                results[label] = {"ok": True, "message": f"{p} — exists, writable"}
            except Exception as ex:
                results[label] = {"ok": False, "message": f"{p} — not writable: {ex}"}
    return jsonify(results)


@app.route("/api/test/navidrome")
def test_navidrome():
    url = get_setting("navidrome_url").rstrip("/")
    user = get_setting("navidrome_user")
    pw = get_setting("navidrome_pass")
    try:
        r = requests.get(
            f"{url}/rest/ping",
            params={"u": user, "p": pw, "v": "1.16.1", "c": "slskdsync", "f": "json"},
            timeout=8,
        )
        d = r.json().get("subsonic-response", {})
        if d.get("status") == "ok":
            return jsonify({"ok": True, "message": f"Connected — Navidrome {d.get('serverVersion', '')}".strip()})
        err = d.get("error", {}).get("message", "Unknown error")
        return jsonify({"ok": False, "message": err})
    except Exception as ex:
        return jsonify({"ok": False, "message": str(ex)})


# ---------------------------------------------------------------------------
# Queue API & actions
# ---------------------------------------------------------------------------

@app.route("/api/tracks")
def api_tracks():
    conn = get_conn()
    rows = [dict(r) for r in conn.execute("SELECT * FROM tracks ORDER BY id DESC LIMIT 100").fetchall()]
    conn.close()
    return jsonify(rows)


@app.route("/api/download/album", methods=["POST"])
def api_download_album():
    data = request.get_json() or {}
    tracks = data.get("tracks", [])
    source = data.get("source", "slskd")
    if not tracks:
        return jsonify({"ok": False, "error": "no tracks"}), 400
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO import_jobs(source,source_type,source_url,nav_playlist,status) VALUES(?,?,?,?,?)",
        ("search", "album", "", 0, "queued"),
    )
    job_id = cur.lastrowid
    count = 0
    for t in tracks:
        title = (t.get("title") or "").strip()
        artist = (t.get("artist") or "").strip()
        if not title or not artist:
            continue
        source_id = "" if source == "monochrome" else (t.get("source_id") or "").strip()
        cur.execute(
            "INSERT INTO tracks(job_id,artist,album,title,track_number,source_id,cover_url,download_source)"
            " VALUES(?,?,?,?,?,?,?,?)",
            (job_id, artist, t.get("album", ""), title,
             t.get("track_number", 0), source_id, t.get("cover", ""), source),
        )
        count += 1
    conn.commit()
    conn.close()
    logger.info(f"[queue] Batch queued {count} tracks via {source}")
    return jsonify({"ok": True, "count": count, "message": f"Queued {count} tracks via {source}"})


@app.route("/api/download", methods=["POST"])
def api_download():
    data = request.get_json(force=True) or {}
    artist = (data.get("artist") or "").strip()
    album = (data.get("album") or "").strip()
    title = (data.get("title") or "").strip()
    cover_url = (data.get("cover") or "").strip()
    dl_source = data.get("source", "slskd")

    if not title or not artist:
        return jsonify({"ok": False, "error": "artist and title are required"}), 400

    # For monochrome, source_id must be a TIDAL ID — never pass a Deezer ID.
    # Leave it empty so the worker does a TIDAL lookup by artist+title.
    if dl_source == "monochrome":
        source_id = ""
    else:
        source_id = (data.get("source_id") or "").strip()

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO import_jobs(source,source_type,source_url,nav_playlist,status) VALUES(?,?,?,?,?)",
        ("search", "track", "", 0, "queued"),
    )
    job_id = cur.lastrowid
    cur.execute(
        "INSERT INTO tracks(job_id,artist,album,title,track_number,source_id,cover_url,download_source)"
        " VALUES(?,?,?,?,?,?,?,?)",
        (job_id, artist, album, title, 0, source_id, cover_url, dl_source),
    )
    conn.commit()
    conn.close()
    logger.info(f"[queue] User queued '{artist} — {title}' via {dl_source}")
    return jsonify({"ok": True, "message": f"Queued \"{title}\" via {dl_source}"})


@app.route("/import", methods=["POST"])
def import_url():
    url = (request.form.get("url") or "").strip()
    dl_source = request.form.get("download_source", "slskd")
    if not url:
        flash("URL is required", "error")
        return redirect(url_for("index"))

    provider = next((p for p in _providers if p.supports(url)), None)
    if not provider:
        flash("Unsupported URL — paste a Spotify, TIDAL, or Apple Music link.", "error")
        return redirect(url_for("index"))

    try:
        source_type, tracks = provider.parse(url)
    except Exception as ex:
        flash(str(ex), "error")
        return redirect(url_for("index"))

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO import_jobs(source,source_type,source_url,nav_playlist,status) VALUES(?,?,?,?,?)",
        (provider.name, source_type, url, 0, "queued"),
    )
    job_id = cur.lastrowid
    for t in tracks:
        cur.execute(
            "INSERT INTO tracks(job_id,artist,album,title,track_number,source_id,cover_url,download_source)"
            " VALUES(?,?,?,?,?,?,?,?)",
            (job_id, t.artist, t.album, t.title, t.track_number, t.source_id, t.cover_url, dl_source),
        )
    conn.commit()
    conn.close()
    flash(f"Queued {len(tracks)} tracks from {provider.name} ({source_type})", "ok")
    return redirect(url_for("index"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5035)

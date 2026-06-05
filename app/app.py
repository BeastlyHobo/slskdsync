import os
import re
import sqlite3
import threading
import time
import shutil
import logging
import base64
import collections
from datetime import datetime, timedelta
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import mutagen
import mutagen.flac
import mutagen.id3
import mutagen.mp4

_log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, _log_level, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("slskdsync")
logger.info(f"Log level: {_log_level} (override with LOG_LEVEL env var)")

# In-memory rolling log buffer — last 200 lines surfaced in Settings log viewer
_log_buffer: collections.deque = collections.deque(maxlen=200)
_log_buffer_lock = threading.Lock()

class _BufferHandler(logging.Handler):
    def emit(self, record):
        try:
            line = self.format(record)
            with _log_buffer_lock:
                _log_buffer.append(line)
        except Exception:
            pass

_buf_handler = _BufferHandler()
_buf_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"))
logging.getLogger().addHandler(_buf_handler)

# Silence Werkzeug access-log spam for the high-frequency polling endpoints
# (the Settings page hits these every couple of seconds).
_QUIET_PATHS = ("/api/logs", "/api/queue/status")

class _AccessLogFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        return not any(p in msg for p in _QUIET_PATHS)

logging.getLogger("werkzeug").addFilter(_AccessLogFilter())

import requests
import jwt as pyjwt
from flask import Flask, render_template, request, redirect, session, url_for, flash, jsonify, send_from_directory, Response, stream_with_context
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
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    conn.execute("PRAGMA journal_mode=WAL")
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
        CREATE TABLE IF NOT EXISTS library_index (
            id INTEGER PRIMARY KEY,
            artist TEXT,
            title TEXT,
            album TEXT,
            source TEXT DEFAULT 'scan',
            indexed_at TEXT DEFAULT (datetime('now')),
            path TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_library_index
            ON library_index(lower(trim(artist)), lower(trim(title)));
        CREATE TABLE IF NOT EXISTS playlist_tracks (
            id INTEGER PRIMARY KEY,
            job_id INTEGER NOT NULL,
            artist TEXT,
            title TEXT,
            album TEXT,
            track_number INTEGER DEFAULT 0,
            FOREIGN KEY(job_id) REFERENCES import_jobs(id)
        );
        CREATE INDEX IF NOT EXISTS idx_playlist_tracks_job ON playlist_tracks(job_id);
    """)
    # Migrate existing databases
    existing_cols = {row[1] for row in cur.execute("PRAGMA table_info(tracks)")}
    for col, ddl in [
        ("cover_url", "TEXT"),
        ("download_source", "TEXT DEFAULT 'slskd'"),
        ("slskd_search_id", "TEXT"),
        ("slskd_tried_users", "TEXT DEFAULT ''"),
        ("slskd_queued_at", "TEXT DEFAULT NULL"),
        ("force_overwrite", "INTEGER DEFAULT 0"),
        ("slskd_search_attempt", "INTEGER DEFAULT 0"),
        ("custom_search", "TEXT DEFAULT NULL"),
        ("slskd_download_user", "TEXT DEFAULT NULL"),
        ("slskd_download_filename", "TEXT DEFAULT NULL"),
        ("acoustid_score", "REAL DEFAULT NULL"),
    ]:
        if col not in existing_cols:
            cur.execute(f"ALTER TABLE tracks ADD COLUMN {col} {ddl}")

    existing_lib_cols = {row[1] for row in cur.execute("PRAGMA table_info(library_index)")}
    for col, ddl in [
        ("path", "TEXT"),
        ("user_rating", "INTEGER DEFAULT NULL"),
        ("cover_art_id", "TEXT DEFAULT NULL"),
    ]:
        if col not in existing_lib_cols:
            cur.execute(f"ALTER TABLE library_index ADD COLUMN {col} {ddl}")

    existing_job_cols = {row[1] for row in cur.execute("PRAGMA table_info(import_jobs)")}
    for col, ddl in [
        ("album_search_id", "TEXT DEFAULT NULL"),
        ("preferred_username", "TEXT DEFAULT NULL"),
        ("playlist_name", "TEXT DEFAULT NULL"),
    ]:
        if col not in existing_job_cols:
            cur.execute(f"ALTER TABLE import_jobs ADD COLUMN {col} {ddl}")

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
        "apple_team_id": "",
        "apple_key_id": "",
        "apple_private_key": "",
        "library_scan_interval": "24",
        "last_library_scan": "",
        "listenbrainz_username": "",
        # auth stored in DB (empty = not configured yet → first-run setup)
        "app_username": "",
        "app_password_hash": "",
    }
    for k, v in defaults.items():
        cur.execute("INSERT OR IGNORE INTO settings(key,value) VALUES (?,?)", (k, v))
    # Migrate old default folder template (had track_number prefix which renders as 00 when unknown)
    cur.execute(
        "UPDATE settings SET value=? WHERE key='folder_template' AND value=?",
        ("{artist}/{album}/{title}{ext}", "{artist}/{album}/{track_number:02d} - {title}{ext}"),
    )
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


_LIVE_RE = re.compile(r'\b(live|unplugged|in concert|acoustic)\b', re.IGNORECASE)

def _is_live(text: str) -> bool:
    return bool(_LIVE_RE.search(text or ""))


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


def _apple_jwt() -> str:
    team_id     = get_setting("apple_team_id").strip()
    key_id      = get_setting("apple_key_id").strip()
    private_key = get_setting("apple_private_key").strip()
    missing = [n for n, v in [("Team ID", team_id), ("Key ID", key_id), ("Private Key", private_key)] if not v]
    if missing:
        raise RuntimeError(f"Apple Music: missing {', '.join(missing)} — check Settings → Apple Music.")
    now = int(time.time())
    try:
        token = pyjwt.encode(
            {"iss": team_id, "iat": now, "exp": now + 15_777_000},
            private_key,
            algorithm="ES256",
            headers={"kid": key_id},
        )
    except Exception as ex:
        raise RuntimeError(
            f"Apple Music: could not sign JWT — {ex}. "
            "Make sure the private key field contains the full .p8 file contents "
            "including the BEGIN/END lines."
        )
    return token


class AppleProvider:
    name = "apple"

    def supports(self, url: str) -> bool:
        return "music.apple.com" in url

    def parse(self, url: str) -> tuple[str, list[TrackMeta]]:
        # Individual track link: music.apple.com/…/album/…?i=TRACKID
        song_match = re.search(r"[?&]i=(\d+)", url)
        if song_match:
            sid = song_match.group(1)
            data = requests.get("https://itunes.apple.com/lookup",
                                params={"id": sid}, timeout=20).json()
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

        # Album link: music.apple.com/…/album/…/ALBUMID
        # Uses the free iTunes lookup API — no key required.
        album_match = re.search(r"/album/[^/]+/(\d+)$", url.split("?")[0])
        if album_match:
            aid = album_match.group(1)
            data = requests.get(
                "https://itunes.apple.com/lookup",
                params={"id": aid, "entity": "song", "limit": 200},
                timeout=20,
            ).json()
            results = data.get("results") or []
            # First result is the album collection; rest are tracks
            collection = next((r for r in results if r.get("wrapperType") == "collection"), {})
            album_title = collection.get("collectionName", "Unknown Album")
            album_cover = collection.get("artworkUrl100", "").replace("100x100bb", "300x300bb")
            tracks = [
                TrackMeta(
                    artist=r.get("artistName", "Unknown Artist"),
                    album=album_title,
                    title=r.get("trackName", ""),
                    track_number=r.get("trackNumber", 0),
                    source_id=str(r.get("trackId", "")),
                    cover_url=album_cover,
                )
                for r in results
                if r.get("wrapperType") == "track" and r.get("trackName")
            ]
            if not tracks:
                raise RuntimeError("No tracks found for this Apple Music album.")
            return "album", tracks

        # Playlist link: music.apple.com/{storefront}/playlist/{name}/{pl.ID}
        playlist_match = re.search(r"music\.apple\.com/([a-z]{2})/playlist/[^/]+/(pl\.[^/?#]+)", url)
        if playlist_match:
            storefront = playlist_match.group(1)
            playlist_id = playlist_match.group(2)
            token = _apple_jwt()
            headers = {"Authorization": f"Bearer {token}"}
            tracks = []
            next_url: Optional[str] = (
                f"https://api.music.apple.com/v1/catalog/{storefront}"
                f"/playlists/{playlist_id}/tracks?limit=100"
            )
            while next_url:
                r = requests.get(next_url, headers=headers, timeout=20)
                if r.status_code == 401:
                    raise RuntimeError("Apple Music API: invalid token — check Team ID, Key ID, and private key.")
                if r.status_code != 200:
                    raise RuntimeError(f"Apple Music API error {r.status_code}: {r.text[:200]}")
                page = r.json()
                for song in page.get("data", []):
                    attrs = song.get("attributes", {})
                    art = attrs.get("artwork", {})
                    cover = art.get("url", "").replace("{w}", "400").replace("{h}", "400") if art else ""
                    tracks.append(TrackMeta(
                        artist=attrs.get("artistName", ""),
                        album=attrs.get("albumName", ""),
                        title=attrs.get("name", ""),
                        track_number=attrs.get("trackNumber", 0),
                        source_id=song.get("id", ""),
                        cover_url=cover,
                    ))
                nxt = page.get("next")
                next_url = ("https://api.music.apple.com" + nxt) if nxt else None
            if not tracks:
                raise RuntimeError("No tracks found in this Apple Music playlist.")
            return "playlist", tracks

        raise RuntimeError("Could not parse this Apple Music link. Paste an album, track, or playlist URL.")


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
                if i.get("record_type", "album") != "single"
            ]
        except Exception:
            return []

    def get_artist(self, artist_id: str) -> dict:
        artist = self._get(f"/artist/{artist_id}")
        albums_data = self._get(f"/artist/{artist_id}/albums", limit=50)
        top_data = self._get(f"/artist/{artist_id}/top", limit=10)
        return {
            "id": str(artist_id),
            "name": artist.get("name", ""),
            "picture": artist.get("picture_xl", "") or artist.get("picture_big", "") or artist.get("picture_medium", ""),
            "nb_fan": artist.get("nb_fan", 0),
            "nb_album": artist.get("nb_album", 0),
            "top_tracks": [
                {
                    "id": str(t.get("id", "")),
                    "title": t.get("title", ""),
                    "artist": artist.get("name", ""),
                    "artist_id": str(artist_id),
                    "album": (t.get("album") or {}).get("title", ""),
                    "album_id": str((t.get("album") or {}).get("id", "")),
                    "cover": (t.get("album") or {}).get("cover_medium", ""),
                    "duration": t.get("duration", 0),
                }
                for t in top_data.get("data", [])
            ],
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
                if a.get("record_type", "album") != "single"
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

    def get_charts(self, limit: int = 25) -> dict:
        try:
            data = self._get("/chart", limit=limit)
            tracks = [
                {
                    "id": str(t.get("id", "")),
                    "title": t.get("title", ""),
                    "artist": t.get("artist", {}).get("name", ""),
                    "artist_id": str(t.get("artist", {}).get("id", "")),
                    "album": t.get("album", {}).get("title", ""),
                    "album_id": str(t.get("album", {}).get("id", "")),
                    "cover": t.get("album", {}).get("cover_big") or t.get("album", {}).get("cover_medium", ""),
                    "duration": t.get("duration", 0),
                    "type": "track",
                }
                for t in (data.get("tracks") or {}).get("data", [])
            ]
            return {"tracks": tracks}
        except Exception:
            return {"tracks": []}

    def get_genres(self) -> list[dict]:
        try:
            data = self._get("/genre")
            return [
                {"id": str(g.get("id", "")), "name": g.get("name", "")}
                for g in data.get("data", [])
                if g.get("id") != 0  # skip "All" genre
            ]
        except Exception:
            return []

    def get_genre_charts(self, genre_id: str, limit: int = 25) -> dict:
        try:
            data = self._get(f"/editorial/{genre_id}/charts", limit=limit)
            tracks = [
                {
                    "id": str(t.get("id", "")),
                    "title": t.get("title", ""),
                    "artist": t.get("artist", {}).get("name", ""),
                    "artist_id": str(t.get("artist", {}).get("id", "")),
                    "album": t.get("album", {}).get("title", ""),
                    "album_id": str(t.get("album", {}).get("id", "")),
                    "cover": t.get("album", {}).get("cover_big") or t.get("album", {}).get("cover_medium", ""),
                    "duration": t.get("duration", 0),
                    "type": "track",
                }
                for t in (data.get("tracks") or {}).get("data", [])
            ]
            return {"tracks": tracks}
        except Exception:
            return {"tracks": []}

    def get_artist_radio(self, artist_id: str, limit: int = 25) -> list[dict]:
        try:
            data = self._get(f"/artist/{artist_id}/radio", limit=limit)
            return [
                {
                    "id": str(t.get("id", "")),
                    "title": t.get("title", ""),
                    "artist": t.get("artist", {}).get("name", ""),
                    "artist_id": str(t.get("artist", {}).get("id", "")),
                    "album": t.get("album", {}).get("title", ""),
                    "album_id": str(t.get("album", {}).get("id", "")),
                    "cover": t.get("album", {}).get("cover_big") or t.get("album", {}).get("cover_medium", ""),
                    "duration": t.get("duration", 0),
                    "type": "track",
                }
                for t in data.get("data", [])
            ]
        except Exception:
            return []


# ---------------------------------------------------------------------------
# Apple Music catalog client (charts / new releases)
# ---------------------------------------------------------------------------

class AppleMusicClient:
    BASE = "https://api.music.apple.com/v1"

    def _token(self) -> Optional[str]:
        try:
            return _apple_jwt()
        except Exception:
            return None

    def _headers(self, token: str) -> dict:
        return {"Authorization": f"Bearer {token}"}

    def get_charts(self, storefront: str = "us", limit: int = 25) -> list[dict]:
        token = self._token()
        if not token:
            return []
        try:
            r = requests.get(
                f"{self.BASE}/catalog/{storefront}/charts",
                headers=self._headers(token),
                params={"types": "songs", "limit": limit},
                timeout=12,
            )
            if r.status_code != 200:
                return []
            charts = r.json().get("results", {}).get("songs", [])
            songs = charts[0].get("data", []) if charts else []
            return [self._normalize(s) for s in songs]
        except Exception:
            return []

    def get_new_releases(self, storefront: str = "us", limit: int = 25) -> list[dict]:
        token = self._token()
        if not token:
            return []
        try:
            r = requests.get(
                f"{self.BASE}/catalog/{storefront}/charts",
                headers=self._headers(token),
                params={"types": "albums", "limit": limit},
                timeout=12,
            )
            if r.status_code != 200:
                return []
            charts = r.json().get("results", {}).get("albums", [])
            albums = charts[0].get("data", []) if charts else []
            result = []
            for a in albums:
                attrs = a.get("attributes", {})
                art = attrs.get("artwork", {})
                cover = art.get("url", "").replace("{w}", "400").replace("{h}", "400") if art else ""
                result.append({
                    "id": a.get("id", ""),
                    "title": attrs.get("name", ""),
                    "artist": attrs.get("artistName", ""),
                    "cover": cover,
                    "type": "album",
                })
            return result
        except Exception:
            return []

    @staticmethod
    def _normalize(song: dict) -> dict:
        attrs = song.get("attributes", {})
        art = attrs.get("artwork", {})
        cover = art.get("url", "").replace("{w}", "400").replace("{h}", "400") if art else ""
        return {
            "id": song.get("id", ""),
            "title": attrs.get("name", ""),
            "artist": attrs.get("artistName", ""),
            "album": attrs.get("albumName", ""),
            "cover": cover,
            "duration": round(attrs.get("durationInMillis", 0) / 1000),
            "type": "track",
        }


# ---------------------------------------------------------------------------
# ListenBrainz collaborative-filtering recommendations
# ---------------------------------------------------------------------------

class ListenBrainzClient:
    BASE = "https://api.listenbrainz.org/1"

    def get_recommendations(self, username: str, limit: int = 25) -> list[dict]:
        """Top recordings for the user this month (falls back to all-time)."""
        if not username:
            return []
        try:
            for range_ in ("month", "all_time"):
                r = requests.get(
                    f"{self.BASE}/stats/user/{username}/recordings",
                    params={"count": limit, "range": range_},
                    timeout=12,
                )
                if r.status_code == 200:
                    break
            else:
                return []
            recordings = (r.json().get("payload") or {}).get("recordings", [])
            result = []
            for rec in recordings:
                result.append({
                    "id": rec.get("recording_mbid", ""),
                    "title": rec.get("recording_name", ""),
                    "artist": rec.get("artist_name", ""),
                    "album": rec.get("release_name", ""),
                    "cover": "",
                    "duration": 0,
                    "type": "track",
                })
            return [item for item in result if item["title"] and item["artist"]]
        except Exception:
            return []

    def get_recent_listens(self, username: str, limit: int = 30) -> list[dict]:
        """Most recent scrobbles, deduplicated by (artist, title)."""
        if not username:
            return []
        try:
            r = requests.get(
                f"{self.BASE}/user/{username}/listens",
                params={"count": limit},
                timeout=12,
            )
            if r.status_code != 200:
                return []
            listens = (r.json().get("payload") or {}).get("listens", [])
            seen: set[tuple[str, str]] = set()
            result = []
            for listen in listens:
                meta = listen.get("track_metadata") or {}
                title = meta.get("track_name", "")
                artist = meta.get("artist_name", "")
                album = meta.get("release_name", "") or (meta.get("mbid_mapping") or {}).get("release_name", "")
                if not title or not artist:
                    continue
                key = (artist.lower(), title.lower())
                if key in seen:
                    continue
                seen.add(key)
                result.append({
                    "id": (meta.get("mbid_mapping") or {}).get("recording_mbid", ""),
                    "title": title,
                    "artist": artist,
                    "album": album,
                    "cover": "",
                    "duration": 0,
                    "type": "track",
                })
            return result
        except Exception:
            return []


# ---------------------------------------------------------------------------
# AcoustID audio fingerprint verification
# ---------------------------------------------------------------------------

def _acoustid_norm(s: str) -> str:
    return re.sub(r'[^a-z0-9]', '', (s or "").lower())


class AcoustIDClient:
    def verify(self, path: Path, artist: str, title: str) -> float | None:
        """
        Returns:
          None   — key not configured, or fingerprinting failed (don't store)
          -1.0   — fingerprinted but recording not found in AcoustID DB
          0.0    — identified but metadata doesn't match (wrong track)
          0–1.0  — AcoustID confidence score for a matching recording
        """
        api_key = get_setting("acoustid_api_key").strip()
        if not api_key:
            return None
        tag = f'"{title}" by {artist}'
        try:
            import acoustid
            na, nt = _acoustid_norm(artist), _acoustid_norm(title)
            results = list(acoustid.match(api_key, str(path), meta="recordings", parse=True, force_fpcalc=True))
            if not results:
                logger.info(f"[AcoustID] {tag} → not in database")
                return -1.0
            for score, _rid, rec_title, rec_artist in results:
                nrt = _acoustid_norm(rec_title or "")
                nra = _acoustid_norm(rec_artist or "")
                title_ok = nt and nrt and (nt in nrt or nrt in nt)
                artist_ok = not na or not nra or (na in nra or nra in na)
                if title_ok and artist_ok:
                    logger.info(f"[AcoustID] {tag} → {score:.0%} match")
                    return float(score)
            best_score, _, best_title, best_artist = results[0]
            logger.warning(f"[AcoustID] {tag} → wrong track ({best_score:.0%} match for \"{best_title}\" by {best_artist})")
            return 0.0
        except Exception as exc:
            logger.warning(f"[AcoustID] {tag} → fingerprint failed: {exc}")
            return None


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

    def start_search_raw(self, query: str) -> tuple[bool, str, str]:
        """Start an slskd search with a raw query string, skipping track-level cleanup."""
        body = {
            "searchText": query[:100],
            "fileLimit": 10000,
            "filterResponses": True,
            "responseLimit": 100,
            "searchTimeout": 15000,
        }
        last_err = "Could not reach slskd"
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

    def cancel_search(self, search_id: str) -> None:
        if not search_id:
            return
        for ep in ["/api/v0/searches", "/api/v1/searches"]:
            try:
                requests.delete(f"{self.base}{ep}/{search_id}",
                                headers=self._headers(), auth=self._auth(), timeout=8)
                return
            except Exception:
                pass

    def get_user_transfers(self, username: str) -> dict[str, str]:
        """Return a mapping of filename → state for all slskd downloads from username.
        States of interest: 'Errored', 'Cancelled', 'TimedOut', 'Completed', 'InProgress'."""
        if not username:
            return {}
        url = f"{self.base}/api/v0/transfers/downloads/{username}"
        try:
            r = requests.get(url, headers=self._headers(), auth=self._auth(), timeout=15)
            if r.status_code != 200:
                return {}
            data = r.json()
            result: dict[str, str] = {}
            # slskd v0 returns a list of directory objects each with a "files" array;
            # some builds return a flat list directly.
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and "files" in item:
                        for f in item["files"]:
                            fn = f.get("filename", "")
                            st = f.get("state", "")
                            if isinstance(st, dict):
                                st = st.get("name", "")  # {name: "Errored", …}
                            if fn:
                                result[fn] = str(st)
                    elif isinstance(item, dict) and "filename" in item:
                        fn = item.get("filename", "")
                        st = item.get("state", "")
                        if isinstance(st, dict):
                            st = st.get("name", "")
                        if fn:
                            result[fn] = str(st)
            return result
        except Exception as ex:
            logger.debug(f"[slskd] get_user_transfers({username}): {ex}")
            return {}

    def get_search_results(self, search_id: str) -> list[dict] | None:
        """Returns None if the search is still running, [] if complete with no results,
        or a populated list if results are available."""
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
                return None  # still running
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
                        "queue_length": user_response.get("queueLength", 0),
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

        # Format quality — primary factor, prefer lossless
        score = {"flac": 100, "wav": 80, "aif": 80, "aiff": 80, "m4a": 65, "ogg": 55, "opus": 55}.get(ext, 0)
        if ext == "mp3":
            br = result.get("bitRate", 0)
            score = 60 if br >= 320 else 50 if br >= 256 else 40 if br >= 192 else 30

        # Metadata match
        title_l = (track.title or "").lower()
        artist_l = (track.artist or "").lower().split(",")[0].strip()
        if title_l and title_l in basename_l:
            score += 30
        if artist_l and artist_l in fn_l:
            score += 20

        # Availability — free slot means download starts immediately (+15)
        if result.get("has_slot"):
            score += 15

        # Upload speed bonus (0–15 pts) — breaks ties between equal-quality sources
        speed_mbps = result.get("upload_speed", 0) / (1024 * 1024)
        if speed_mbps >= 5:
            score += 15
        elif speed_mbps >= 2:
            score += 10
        elif speed_mbps >= 0.5:
            score += 5

        # Queue length penalty — long queues mean slow starts
        queue_len = result.get("queue_length", 0)
        if queue_len > 10:
            score -= 10
        elif queue_len > 5:
            score -= 5

        # Live/studio preference — soft: penalise mismatch so correct type wins
        want_live = _is_live(track.title) or _is_live(track.album)
        has_live  = _is_live(basename_l)
        if want_live and not has_live:
            score -= 30   # wanted live, got studio
        elif not want_live and has_live:
            score -= 50   # wanted studio, got live (stronger — more jarring)

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
            if r.status_code == 409:
                # Already in slskd's download queue — treat as success
                return True, "already_queued"
            return False, f"HTTP {r.status_code}: {r.text[:200]}"
        except Exception as ex:
            return False, str(ex)


# ---------------------------------------------------------------------------
# Album-search helpers
# ---------------------------------------------------------------------------

def _score_user_album_coverage(user_files: list, album_tracks: list) -> int:
    """Count how many album tracks this user has a matching file for."""
    matched = 0
    for track in album_tracks:
        title_l = (track["title"] or "").lower()
        if not title_l:
            continue
        for f in user_files:
            basename = f["filename"].replace("\\", "/").rsplit("/", 1)[-1].lower()
            if title_l in basename:
                matched += 1
                break
    return matched


def _find_file_for_track(user_files: list, track: sqlite3.Row,
                          slskd_client: "SlskdClient") -> Optional[dict]:
    """Return the best audio file for a track from a specific user's file list.
    Title match is required — a bare format score is not sufficient."""
    meta = TrackMeta(track["artist"] or "", track["album"] or "",
                     track["title"] or "", track["track_number"] or 0)
    title_l = meta.title.lower()
    # Require the title to appear in the filename, same as _score_user_album_coverage
    candidates = [
        f for f in user_files
        if title_l
        and title_l in f["filename"].replace("\\", "/").rsplit("/", 1)[-1].lower()
        and slskd_client.score_result(f, meta) > 0
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda f: slskd_client.score_result(f, meta))


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
    def move_file(src: Path, dst: Path, force_overwrite: bool = False) -> tuple[bool, str]:
        dst.parent.mkdir(parents=True, exist_ok=True)
        if dst.exists() and not force_overwrite and get_setting("replace_existing") != "1":
            return True, str(dst)  # already there — treat as success
        shutil.copyfile(str(src), str(dst))
        try:
            src.unlink()
        except Exception as ex:
            logger.debug(f"[organizer] Could not remove source file (permission issue — OK): {ex}")
        return True, str(dst)


def _fetch_cover(url: str) -> Optional[bytes]:
    if not url:
        return None
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200 and r.content:
            return r.content
    except Exception as ex:
        logger.debug(f"[tag] Cover fetch failed: {ex}")
    return None


def _embed_cover(path: Path, data: bytes) -> None:
    mime = "image/png" if data[:4] == b"\x89PNG" else "image/jpeg"
    ext = path.suffix.lower()
    try:
        if ext == ".flac":
            audio = mutagen.flac.FLAC(path)
            pic = mutagen.flac.Picture()
            pic.type = 3
            pic.mime = mime
            pic.data = data
            audio.clear_pictures()
            audio.add_picture(pic)
            audio.save()
        elif ext == ".mp3":
            try:
                tags = mutagen.id3.ID3(path)
            except mutagen.id3.ID3NoHeaderError:
                tags = mutagen.id3.ID3()
            tags.delall("APIC")
            tags.add(mutagen.id3.APIC(encoding=3, mime=mime, type=3, desc="Cover", data=data))
            tags.save(path)
        elif ext in (".m4a", ".alac", ".aac"):
            audio = mutagen.mp4.MP4(path)
            fmt = mutagen.mp4.MP4Cover.FORMAT_PNG if mime == "image/png" else mutagen.mp4.MP4Cover.FORMAT_JPEG
            audio.tags["covr"] = [mutagen.mp4.MP4Cover(data, imageformat=fmt)]
            audio.save()
        elif ext in (".ogg", ".opus"):
            audio = mutagen.File(path)
            if audio is not None:
                pic = mutagen.flac.Picture()
                pic.type = 3
                pic.mime = mime
                pic.data = data
                pic.width = pic.height = pic.depth = pic.colors = 0
                audio["metadata_block_picture"] = [base64.b64encode(pic.write()).decode("ascii")]
                audio.save()
        else:
            logger.debug(f"[tag] Cover embedding not supported for {ext}")
    except Exception as ex:
        logger.debug(f"[tag] Cover embed failed for {path.name}: {ex}")


def tag_file(path: Path, track: sqlite3.Row) -> None:
    try:
        audio = mutagen.File(path, easy=True)
        if audio is None:
            logger.debug(f"[tag] Skipping unsupported format: {path.suffix}")
            return
        if audio.tags is None:
            audio.add_tags()
        if track["title"]:
            audio["title"] = [track["title"]]
        if track["artist"]:
            audio["artist"] = [track["artist"]]
        if track["album"]:
            audio["album"] = [track["album"]]
        if track["track_number"]:
            audio["tracknumber"] = [str(track["track_number"])]
        audio.save()
        logger.info(f"[tag] Tagged: {path.name} — {track['artist']} / {track['title']}")
        cover_data = _fetch_cover(track["cover_url"] or "")
        if cover_data:
            _embed_cover(path, cover_data)
    except Exception as ex:
        logger.warning(f"[tag] Failed to tag {path.name}: {ex}")


def discover_download_for_track(track: sqlite3.Row) -> Optional[Path]:
    watch = Path(get_setting("download_watch_path"))
    if not watch.exists():
        logger.warning(f"[discover] Watch path does not exist: {watch}")
        return None
    title = (track["title"] or "").lower().strip()
    artist = (track["artist"] or "").lower().split(",")[0].strip()

    def _norm(s: str) -> str:
        # Strip quote characters that differ between DB titles and peer filenames
        return s.replace('"', '').replace('“', '').replace('”', '').replace('‘', '').replace('’', '')

    title_norm = _norm(title)

    # Collect all audio files once so we can log useful diagnostics
    audio_files = [f for f in watch.glob("**/*")
                   if f.is_file() and f.suffix.lower() in AUDIO_EXTS]
    if not audio_files:
        logger.warning(f"[discover] Watch path {watch} exists but contains no audio files")
        return None

    title_match = None
    path_match = None  # title found in full path but not filename
    for f in audio_files:
        n = _norm(f.name.lower())
        full_l = _norm(str(f).lower().replace("\\", "/"))
        if title_norm and title_norm in n:
            artist_norm = _norm(artist)
            if artist_norm and artist_norm in n:
                logger.debug(f"[discover] Exact match: {f.name}")
                return f
            if title_match is None:
                title_match = f
        elif title_norm and title_norm in full_l and path_match is None:
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
    _scan_running = False

    def _maybe_scan():
        nonlocal _scan_running
        if _scan_running:
            return
        try:
            interval_h = int(get_setting("library_scan_interval") or "24")
        except ValueError:
            interval_h = 24
        if interval_h <= 0:
            return
        last = get_setting("last_library_scan") or ""
        if last:
            try:
                elapsed = (datetime.utcnow() - datetime.fromisoformat(last)).total_seconds()
                if elapsed < interval_h * 3600:
                    return
            except ValueError:
                pass
        _scan_running = True

        def _run():
            nonlocal _scan_running
            try:
                scan_library()
            finally:
                _scan_running = False

        threading.Thread(target=_run, daemon=True).start()

    while not stop_event.is_set():
        try:
            _worker_tick()
        except Exception as ex:
            logger.error(f"Worker tick error: {ex}")
        _maybe_scan()
        time.sleep(20)


def _build_playlist_entries(conn, job_id: int, playlist_name: str, source_url: str,
                            allow_refetch: bool = False) -> tuple[list[dict], int, int, int]:
    """Build the full ordered list of playlist entries: completed downloads plus
    every song from the original imported playlist that can be matched to a file
    in the library (via library_index, then a filesystem-walk fallback).

    Returns (merged, lib_index_count, fs_count, miss_count) where merged is a list
    of {path, artist, title} dicts.

    allow_refetch: when True (manual regenerate only), re-fetch the track list from
    the source URL if playlist_tracks is empty. The auto-sync path passes False so
    the worker never makes provider API calls on every tick.
    """
    # --- Downloaded by app (have local_path on disk) ---
    dl_rows = conn.execute(
        "SELECT DISTINCT t.local_path AS path, t.artist, t.title FROM tracks t"
        " JOIN import_jobs j ON j.id = t.job_id"
        " WHERE t.slskd_state='completed' AND t.local_path IS NOT NULL"
        " AND (j.playlist_name=? OR (? != '' AND j.source_url=?))"
        " ORDER BY t.track_number, t.id",
        (playlist_name, source_url, source_url),
    ).fetchall()

    # --- Full track list for this playlist ---
    # Priority 1: playlist_tracks table (stored at import time, includes dedup-skipped songs)
    pt_rows = conn.execute(
        "SELECT artist, title FROM playlist_tracks WHERE job_id=?", (job_id,)
    ).fetchall()

    if pt_rows:
        pl_tracks = [{"artist": r["artist"], "title": r["title"]} for r in pt_rows]
        logger.info(f"[m3u] Using {len(pl_tracks)} tracks from stored playlist_tracks")
    elif allow_refetch:
        # Priority 2: re-fetch from the source URL (handles playlists imported before playlist_tracks existed)
        pl_tracks = []
        provider = next((p for p in _providers if p.supports(source_url)), None)
        if provider and source_url:
            try:
                _, fetched = provider.parse(source_url)
                pl_tracks = [{"artist": t.artist, "title": t.title} for t in fetched]
                if fetched:
                    conn.executemany(
                        "INSERT INTO playlist_tracks(job_id, artist, title, album, track_number) VALUES(?,?,?,?,?)",
                        [(job_id, t.artist, t.title, t.album, t.track_number) for t in fetched],
                    )
                    conn.commit()
                    logger.info(f"[m3u] Re-fetched {len(fetched)} tracks from source and cached in playlist_tracks")
            except Exception as ex:
                logger.warning(f"[m3u] Could not re-fetch playlist from {source_url}: {ex}")
        if not pl_tracks:
            pl_tracks = _playlist_tracks_fallback(conn, playlist_name, source_url)
    else:
        # Priority 3: fall back to tracks table (only what was queued for download)
        pl_tracks = _playlist_tracks_fallback(conn, playlist_name, source_url)

    # --- All library_index entries (may be missing paths for older scans) ---
    lib_all = conn.execute("SELECT artist, title, path FROM library_index").fetchall()

    # Normalize helper: lowercase, strip parenthetical (feat./remastered/live/etc.)
    _paren_re = re.compile(r'\s*[\(\[][^\)\]]*[\)\]]')
    _punct_re = re.compile(r"[^\w\s]")
    def _norm(s: str) -> str:
        s = _paren_re.sub("", s or "")
        s = _punct_re.sub(" ", s)
        return " ".join(s.lower().split())

    # Build library lookups: keyed by normalised values
    lib_with_path = [e for e in lib_all if e["path"]]
    lib_by_key: dict[tuple[str, str], dict] = {}
    lib_by_title: dict[str, list[dict]] = {}
    for e in lib_with_path:
        na = _norm(e["artist"])
        nt = _norm(e["title"])
        if not nt:
            continue
        entry = {"path": e["path"], "artist": e["artist"], "title": e["title"], "_na": na, "_nt": nt}
        lib_by_key.setdefault((na, nt), entry)
        lib_by_title.setdefault(nt, []).append(entry)

    # --- Filesystem fallback index (only built if needed) ---
    music_root = Path(get_setting("library_path") or "/music")
    _fs_files: list[tuple[str, str, str]] = []  # (norm_filename, norm_parent_dirs, full_path)
    _fs_built = False
    def _build_fs():
        nonlocal _fs_built
        if _fs_built:
            return
        _fs_built = True
        if not music_root.exists():
            return
        for f in music_root.rglob("*"):
            if f.is_file() and f.suffix.lower() in AUDIO_EXTS:
                stem = re.sub(r"^\d+\s*[-\.]\s*", "", f.stem)  # strip leading track number
                _fs_files.append((_norm(stem), _norm(str(f.relative_to(music_root).parent)), str(f)))

    def _fs_lookup(artist_n: str, title_n: str) -> str | None:
        _build_fs()
        if not title_n:
            return None
        # Strategy A: title in filename + artist in path/filename
        for fn, parent, path in _fs_files:
            if title_n in fn and (not artist_n or artist_n in fn or artist_n in parent):
                return path
        # Strategy B: title in filename alone (last resort)
        for fn, parent, path in _fs_files:
            if title_n == fn:
                return path
        for fn, parent, path in _fs_files:
            if title_n and len(title_n) >= 4 and title_n in fn:
                return path
        return None

    dl_keys = {(_norm(r["artist"]), _norm(r["title"])) for r in dl_rows}
    seen_paths = {r["path"] for r in dl_rows}

    lib_rows: list[dict] = []
    fs_count = 0
    miss_count = 0
    for t in pl_tracks:
        na = _norm(t["artist"])
        # Spotify often credits many co-artists — try first artist alone too
        na_first = _norm((t["artist"] or "").split(",")[0].split("&")[0])
        nt = _norm(t["title"])
        if not nt or (na, nt) in dl_keys:
            continue

        entry = lib_by_key.get((na, nt)) or lib_by_key.get((na_first, nt))

        if not entry:
            # Title exact, artist substring (handles co-credits)
            for cand in lib_by_title.get(nt, []):
                ca = cand["_na"]
                if ca == na or (na and (na in ca or ca in na)) or (na_first and (na_first in ca or ca in na_first)):
                    entry = cand
                    break

        if not entry and len(nt) >= 5:
            # Title substring fuzzy match — only when artist also overlaps
            for (ea, et), cand in lib_by_key.items():
                if nt in et or et in nt:
                    if ea == na or (na and (na in ea or ea in na)) or (na_first and (na_first in ea or ea in na_first)):
                        entry = cand
                        break

        path: str | None = None
        if entry:
            path = entry["path"]
        else:
            # Final fallback: walk the filesystem
            path = _fs_lookup(na_first or na, nt)
            if path:
                fs_count += 1

        if path and path not in seen_paths:
            lib_rows.append({"path": path, "artist": t["artist"], "title": t["title"]})
            seen_paths.add(path)
        elif not path:
            miss_count += 1
            logger.info(f"[m3u] No library match for '{t['artist']}' / '{t['title']}'")

    merged = list(dl_rows) + lib_rows
    return merged, len(lib_rows) - fs_count, fs_count, miss_count


def _playlist_tracks_fallback(conn, playlist_name: str, source_url: str) -> list[dict]:
    """Last-resort track list from the tracks table (only what was queued)."""
    fallback = conn.execute(
        "SELECT DISTINCT artist, title FROM tracks t"
        " JOIN import_jobs j ON j.id = t.job_id"
        " WHERE (j.playlist_name=? OR (? != '' AND j.source_url=?))"
        " AND artist IS NOT NULL AND title IS NOT NULL AND artist!='' AND title!=''",
        (playlist_name, source_url, source_url),
    ).fetchall()
    tracks = [{"artist": r["artist"], "title": r["title"]} for r in fallback]
    logger.info(f"[m3u] Using {len(tracks)} tracks from tracks table (fallback)")
    return tracks


def write_playlist_m3u(job_id: int, playlist_name: str) -> None:
    """Write/update an M3U with the full playlist (completed downloads + already-owned
    library songs) and sync it to Navidrome. Uses the same list-building logic as the
    manual regenerate button so the two paths can never diverge."""
    library = get_setting("library_path") or "/music"
    safe = re.sub(r'[<>:"/\\|?*]', "", playlist_name).strip()[:120] or "playlist"
    m3u_path = Path(library) / f"{safe}.m3u"
    conn = get_conn()
    job_row = conn.execute("SELECT source_url FROM import_jobs WHERE id=?", (job_id,)).fetchone()
    source_url = (job_row["source_url"] if job_row else "") or ""
    merged, idx_count, fs_count, miss_count = _build_playlist_entries(
        conn, job_id, playlist_name, source_url, allow_refetch=False
    )
    conn.close()
    if not merged:
        return
    n_dl = len(merged) - idx_count - fs_count
    lines = ["#EXTM3U"]
    for r in merged:
        lines.append(f"#EXTINF:0,{r['artist'] or ''} - {r['title'] or ''}")
        lines.append(r["path"])
    m3u_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    logger.info(
        f"[m3u] {safe}.m3u updated — {len(merged)} tracks "
        f"({n_dl} downloads + {idx_count} index + {fs_count} fs-walk, {miss_count} missing)"
    )
    _update_navidrome_playlist(playlist_name, merged)


def _update_navidrome_playlist(playlist_name: str, tracks: list[dict]) -> None:
    """Create or update a Navidrome playlist directly via the Subsonic API.
    Uses POST (not GET) to avoid URL-length limits with large song-ID lists.
    Delete-then-create avoids the ambiguous mixed add+remove semantics of
    updatePlaylist when replacing all songs at once."""
    nav_url = (get_setting("navidrome_url") or "").rstrip("/")
    nav_user = get_setting("navidrome_user") or ""
    nav_pass = get_setting("navidrome_pass") or ""
    if not (nav_url and nav_user and tracks):
        return

    base = {"u": nav_user, "p": nav_pass, "v": "1.16.1", "c": "slskdsync", "f": "json"}

    def _subsonic_ok(resp, what: str) -> bool:
        try:
            body = resp.json().get("subsonic-response", {})
        except Exception:
            body = {}
        if resp.status_code != 200 or body.get("status") != "ok":
            err = body.get("error", {}).get("message") or resp.text[:200]
            logger.warning(f"[nav] {what} failed (HTTP {resp.status_code}): {err}")
            return False
        return True

    # Normaliser shared with the regenerate matching logic
    _paren = re.compile(r'\s*[\(\[][^\)\]]*[\)\]]')
    _punct = re.compile(r"[^\w\s]")
    def _n(s: str) -> str:
        s = _paren.sub("", s or "")
        s = _punct.sub(" ", s)
        return " ".join(s.lower().split())

    # Fetch all songs from Navidrome via POST to build an id lookup
    song_lookup: dict[tuple[str, str], str] = {}  # (norm_artist, norm_title) -> id
    offset = 0
    while True:
        try:
            r = requests.post(f"{nav_url}/rest/search3",
                              data={**base, "query": "", "songCount": 500, "songOffset": offset,
                                    "artistCount": 0, "albumCount": 0}, timeout=20)
            if not _subsonic_ok(r, f"search3 offset={offset}"):
                break
            songs = r.json().get("subsonic-response", {}).get("searchResult3", {}).get("song", [])
            for s in songs:
                key = (_n(s.get("artist", "")), _n(s.get("title", "")))
                song_lookup.setdefault(key, s["id"])
            if len(songs) < 500:
                break
            offset += 500
        except Exception as ex:
            logger.warning(f"[nav] search3 failed at offset {offset}: {ex}")
            break

    # Resolve each playlist track to a Navidrome song ID
    song_ids: list[str] = []
    for t in tracks:
        na = _n(t["artist"] or "")
        na1 = _n((t["artist"] or "").split(",")[0].split("&")[0])
        nt = _n(t["title"] or "")
        if not nt:
            continue
        sid = song_lookup.get((na, nt)) or song_lookup.get((na1, nt))
        if not sid:
            # title-only fallback
            for (ea, et), s_id in song_lookup.items():
                if et == nt:
                    sid = s_id
                    break
        if sid:
            song_ids.append(sid)

    if not song_ids:
        logger.warning(f"[nav] Could not resolve any Navidrome IDs for '{playlist_name}' — playlist not updated")
        return

    # Find existing Navidrome playlist by name
    try:
        r = requests.post(f"{nav_url}/rest/getPlaylists", data=base, timeout=10)
        if not _subsonic_ok(r, "getPlaylists"):
            return
        playlists = r.json().get("subsonic-response", {}).get("playlists", {}).get("playlist", [])
        if isinstance(playlists, dict):
            playlists = [playlists]
    except Exception as ex:
        logger.warning(f"[nav] getPlaylists failed: {ex}")
        return

    existing_id: str | None = next((p["id"] for p in playlists if p.get("name") == playlist_name), None)

    try:
        if existing_id:
            # Get current entry count so we know how many to remove from the front.
            r_get = requests.post(f"{nav_url}/rest/getPlaylist",
                                  data={**base, "id": existing_id}, timeout=10)
            if not _subsonic_ok(r_get, "getPlaylist"):
                return
            current = r_get.json().get("subsonic-response", {}).get("playlist", {}).get("entry", [])
            n_existing = len(current) if isinstance(current, list) else (1 if current else 0)

            # Shrink guard: refuse to replace a large playlist with a much smaller one.
            # This is a backstop against any code path feeding an incomplete list here —
            # the real fix is that callers build the full list via _build_playlist_entries().
            if n_existing > 10 and len(song_ids) <= n_existing * 0.5:
                logger.warning(
                    f"[nav] Refusing to shrink playlist '{playlist_name}': "
                    f"{n_existing} existing → {len(song_ids)} resolved. Leaving it unchanged."
                )
                return

            # Step 1: append all new songs to the end of the existing playlist.
            r_add = requests.post(f"{nav_url}/rest/updatePlaylist",
                                  data={**base, "playlistId": existing_id, "songIdToAdd": song_ids},
                                  timeout=20)
            if not _subsonic_ok(r_add, "updatePlaylist (add)"):
                return

            # Step 2: remove the original entries from the front (indices 0..n_existing-1).
            # The new songs were appended after them so their indices are unaffected.
            if n_existing:
                r_rm = requests.post(f"{nav_url}/rest/updatePlaylist",
                                     data={**base, "playlistId": existing_id,
                                           "songIndexToRemove": list(range(n_existing))},
                                     timeout=20)
                if not _subsonic_ok(r_rm, "updatePlaylist (remove)"):
                    return

            logger.info(f"[nav] Updated Navidrome playlist '{playlist_name}' → {len(song_ids)} songs")
        else:
            r_create = requests.post(f"{nav_url}/rest/createPlaylist",
                                     data={**base, "name": playlist_name, "songId": song_ids},
                                     timeout=20)
            if _subsonic_ok(r_create, "createPlaylist"):
                logger.info(f"[nav] Created Navidrome playlist '{playlist_name}' with {len(song_ids)} songs")
    except Exception as ex:
        logger.warning(f"[nav] Failed to update Navidrome playlist '{playlist_name}': {ex}")


def scan_library() -> None:
    """Index the music library into library_index for dedup checks.
    Uses Navidrome's Subsonic API if configured, falls back to filesystem walk."""
    # Write timestamp before we start so even a failed scan resets the 24h cooldown.
    set_setting("last_library_scan", datetime.utcnow().isoformat(timespec="seconds"))
    with _scan_state_lock:
        _scan_state.update({"in_progress": True, "count": 0, "source": "", "last_at": ""})

    rows: list[tuple[str, str, str, str]] = []  # (artist, title, album, path)
    source = "unknown"
    try:
        nav_url = (get_setting("navidrome_url") or "").rstrip("/")
        nav_user = get_setting("navidrome_user") or ""
        nav_pass = get_setting("navidrome_pass") or ""

        if nav_url and nav_user:
            # Path A: Navidrome Subsonic API
            music_path_str = (get_setting("library_path") or "/music").rstrip("/")
            params_base = {"u": nav_user, "p": nav_pass, "v": "1.16.1",
                           "c": "slskdsync", "f": "json",
                           "songCount": 500, "artistCount": 0, "albumCount": 0, "query": ""}
            offset = 0
            while True:
                try:
                    r = requests.get(f"{nav_url}/rest/search3",
                                     params={**params_base, "songOffset": offset}, timeout=20)
                    data = r.json().get("subsonic-response", {})
                    if data.get("status") != "ok":
                        logger.warning(f"[library] Navidrome search3 error: {data.get('error')}")
                        break
                    songs = data.get("searchResult3", {}).get("song", [])
                    for s in songs:
                        nav_path = s.get("path", "")
                        # Navidrome returns path relative to music dir; make it absolute
                        if nav_path and not nav_path.startswith("/"):
                            nav_path = f"{music_path_str}/{nav_path}"
                        rows.append((s.get("artist", ""), s.get("title", ""), s.get("album", ""), nav_path,
                                     s.get("userRating"), s.get("coverArt")))
                    if len(songs) < 500:
                        break
                    offset += 500
                except Exception as ex:
                    logger.warning(f"[library] Navidrome scan failed at offset {offset}: {ex}")
                    break
            source = "navidrome"
        else:
            # Path B: filesystem walk (same logic as /library route)
            music_path = Path(get_setting("library_path") or "/music")
            num_re = re.compile(r"^\d+\s*[-\.]\s*")
            if music_path.exists():
                for f in music_path.rglob("*"):
                    if not (f.is_file() and f.suffix.lower() in AUDIO_EXTS):
                        continue
                    parts = f.relative_to(music_path).parts
                    artist = parts[0] if len(parts) >= 3 else ""
                    album = parts[1] if len(parts) >= 3 else (parts[0] if len(parts) == 2 else "")
                    title = num_re.sub("", f.stem)
                    rows.append((artist, title, album, str(f), None, None))
            source = "filesystem"

        conn = get_conn()
        conn.execute("DELETE FROM library_index")
        conn.executemany(
            "INSERT INTO library_index(artist, title, album, source, path, user_rating, cover_art_id)"
            " VALUES (?,?,?,?,?,?,?)",
            [(a, t, al, source, p, ur, ca) for a, t, al, p, ur, ca in rows]
        )
        conn.commit()
        conn.close()
        with _scan_state_lock:
            _scan_state.update({
                "in_progress": False, "count": len(rows),
                "source": source, "last_at": datetime.utcnow().isoformat(timespec="seconds"),
            })
        logger.info(f"[library] Indexed {len(rows)} tracks from {source}")
    except Exception as ex:
        logger.error(f"[library] Scan failed: {ex}")
        with _scan_state_lock:
            _scan_state.update({"in_progress": False, "source": source})


def _already_in_library(conn, artist: str, title: str) -> bool:
    """Return True if this artist+title exists in completed downloads or the library index."""
    if conn.execute(
        "SELECT 1 FROM tracks WHERE slskd_state='completed'"
        " AND lower(trim(artist))=lower(trim(?)) AND lower(trim(title))=lower(trim(?))",
        (artist, title)
    ).fetchone():
        return True
    return bool(conn.execute(
        "SELECT 1 FROM library_index"
        " WHERE lower(trim(artist))=lower(trim(?)) AND lower(trim(title))=lower(trim(?))",
        (artist, title)
    ).fetchone())


def _worker_tick():
    conn = get_conn()
    slskd = SlskdClient()
    # Tracks (username, filename) pairs already requested this tick to avoid
    # double-requesting the same file from both album and individual search paths.
    _queued_this_tick: set[tuple[str, str]] = set()

    # ── STUCK SEARCH TIMEOUT ────────────────────────────────────────────────
    # Searches have a 15 s timeout; if a track is still 'queued' after 3 min
    # the search ID is probably stale — reset to 'pending' to retry.
    conn.execute("""
        UPDATE tracks SET slskd_state='pending', slskd_search_id=NULL,
            slskd_error='Search timed out, retrying'
        WHERE slskd_state='queued'
          AND slskd_queued_at IS NOT NULL
          AND datetime(slskd_queued_at, '+3 minutes') < datetime('now')
    """)
    conn.commit()

    # Reset tracks stuck in 'downloading' for >30 min (stalled transfer).
    stuck_dl = conn.execute("""
        SELECT id, slskd_search_id FROM tracks
        WHERE slskd_state='downloading'
          AND slskd_queued_at IS NOT NULL
          AND datetime(slskd_queued_at, '+30 minutes') < datetime('now')
    """).fetchall()
    for row in stuck_dl:
        if row["slskd_search_id"]:
            try:
                slskd.cancel_search(row["slskd_search_id"])
            except Exception:
                pass
        conn.execute(
            "UPDATE tracks SET slskd_state='pending', slskd_search_id=NULL,"
            " slskd_download_user=NULL, slskd_download_filename=NULL,"
            " slskd_error='Download stalled after 30 min — retrying'"
            " WHERE id=?",
            (row["id"],),
        )
        logger.warning(f"[worker] Track {row['id']} stuck downloading >30 min, reset to pending")
    if stuck_dl:
        conn.commit()

    # ── ALBUM SEARCH: start ────────────────────────────────────────────────
    # Jobs with 3+ pending slskd tracks get one "Artist Album" search instead
    # of N individual searches; all tracks hold in 'album_queued' state.
    for job in conn.execute("""
        SELECT j.id, t.artist, t.album, COUNT(*) as cnt
        FROM import_jobs j
        JOIN tracks t ON t.job_id = j.id
        WHERE t.download_source IN ('slskd', '')
          AND t.slskd_state = 'pending'
          AND j.album_search_id IS NULL
        GROUP BY j.id
        HAVING cnt >= 3
        LIMIT 3
    """).fetchall():
        artist = re.split(r',|&|\bfeat\.|\bft\.', job["artist"] or "",
                          flags=re.IGNORECASE)[0].strip()
        album = (job["album"] or "").strip()
        query = f"{artist} {album}".strip()[:100]
        if not query:
            continue
        ok, search_id, msg = slskd.start_search_raw(query)
        if ok:
            conn.execute("UPDATE import_jobs SET album_search_id=? WHERE id=?",
                         (search_id, job["id"]))
            conn.execute(
                "UPDATE tracks SET slskd_state='album_queued'"
                " WHERE job_id=? AND slskd_state='pending'",
                (job["id"],)
            )
            logger.info(f"[album] Album search started job={job['id']} query={query!r} id={search_id}")
        else:
            logger.warning(f"[album] Album search failed job={job['id']}: {msg}")
        conn.commit()

    # ── ALBUM SEARCH: process results ─────────────────────────────────────
    # When the album search completes, pick the peer with best track coverage
    # and initiate downloads for all matched tracks. Unmatched tracks fall
    # back to 'pending' for individual search on the next tick.
    for job in conn.execute("""
        SELECT j.id, j.album_search_id
        FROM import_jobs j
        WHERE j.album_search_id IS NOT NULL
          AND j.preferred_username IS NULL
          AND EXISTS (
              SELECT 1 FROM tracks t
              WHERE t.job_id = j.id AND t.slskd_state = 'album_queued'
          )
        LIMIT 3
    """).fetchall():
        results = slskd.get_search_results(job["album_search_id"])
        if results is None:
            continue  # search still running

        album_tracks = conn.execute(
            "SELECT * FROM tracks WHERE job_id=? AND slskd_state='album_queued'",
            (job["id"],)
        ).fetchall()
        if not album_tracks:
            continue

        # Group flat results by username
        by_user: dict = {}
        for r in results:
            by_user.setdefault(r["username"], []).append(r)

        # Pick the user with the best combination of coverage, speed, and free slot
        def _user_total_score(username, files):
            coverage = _score_user_album_coverage(files, album_tracks)
            speed_bonus = min(files[0].get("upload_speed", 0) / (1024 * 1024), 10)
            slot_bonus = 5 if files[0].get("has_slot") else 0
            return coverage * 10 + speed_bonus + slot_bonus

        best_username = max(by_user, key=lambda u: _user_total_score(u, by_user[u]),
                            default=None)
        conn.execute("UPDATE import_jobs SET preferred_username=? WHERE id=?",
                     (best_username or "", job["id"]))

        if best_username:
            preferred_files = by_user[best_username]
            coverage = _score_user_album_coverage(preferred_files, album_tracks)
            logger.info(f"[album] job={job['id']} preferred peer={best_username!r} "
                        f"({coverage}/{len(album_tracks)} tracks matched)")
            for track in album_tracks:
                best_file = _find_file_for_track(preferred_files, track, slskd)
                if best_file:
                    key = (best_username, best_file["filename"])
                    if key in _queued_this_tick:
                        # Same file already requested for a different track — fall back to
                        # individual search so this track gets its own file, not the wrong one.
                        logger.info(f"[album] {track['title']!r} shares a file with another track; using individual search")
                        conn.execute("UPDATE tracks SET slskd_state='pending' WHERE id=?", (track["id"],))
                        continue
                    ok, msg = slskd.download_file(
                        best_username, best_file["filename"], best_file.get("size", 0))
                    if ok:
                        _queued_this_tick.add(key)
                        conn.execute(
                            "UPDATE tracks SET slskd_state='downloading',"
                            " slskd_search_id=?, slskd_tried_users=?,"
                            " slskd_download_user=?, slskd_download_filename=? WHERE id=?",
                            (job["album_search_id"], best_username,
                             best_username, best_file["filename"], track["id"])
                        )
                        logger.info(f"[album] Downloading {track['title']!r} from {best_username}")
                    else:
                        logger.warning(f"[album] Download failed for {track['title']!r}: {msg}")
                        conn.execute(
                            "UPDATE tracks SET slskd_state='pending' WHERE id=?", (track["id"],))
                else:
                    logger.info(f"[album] No match for {track['title']!r} from preferred peer, falling back")
                    conn.execute(
                        "UPDATE tracks SET slskd_state='pending' WHERE id=?", (track["id"],))
        else:
            logger.warning(f"[album] No peers found for job={job['id']}, falling back to individual search")
            conn.execute(
                "UPDATE tracks SET slskd_state='pending'"
                " WHERE job_id=? AND slskd_state='album_queued'",
                (job["id"],)
            )
        # Clear album search state so we don't re-poll this completed search next tick.
        # Do NOT cancel the slskd search — slskd may link queued downloads to it.
        conn.execute(
            "UPDATE import_jobs SET album_search_id=NULL, preferred_username=NULL WHERE id=?",
            (job["id"],)
        )
        conn.commit()

    # pending slskd → start search (limit 3 per tick to avoid overwhelming slskd)
    for t in conn.execute(
        "SELECT * FROM tracks WHERE slskd_state='pending'"
        " AND (download_source='slskd' OR download_source IS NULL) LIMIT 3"
    ).fetchall():
        meta = TrackMeta(t["artist"] or "", t["album"] or "", t["title"] or "",
                         t["track_number"] or 0, t["source_id"] or "")
        custom = (t["custom_search"] or "").strip()
        attempt = t["slskd_search_attempt"] or 0
        if custom:
            logger.info(f"[slskd] Custom search: {custom!r}")
            ok, search_id, msg = slskd.start_search_raw(custom)
        elif attempt >= 1:
            query_str = (t["title"] or "").strip()
            logger.info(f"[slskd] Title-only retry for '{meta.title}': {query_str!r}")
            ok, search_id, msg = slskd.start_search_raw(query_str) if query_str else (False, "", "empty title")
        else:
            query = SlskdClient._build_query(meta.artist, meta.title)
            logger.info(f"[slskd] Starting search: {query!r}")
            ok, search_id, msg = slskd.start_search(meta)
        if ok:
            logger.info(f"[slskd] Search queued (id={search_id}): {meta.title}")
            conn.execute(
                "UPDATE tracks SET slskd_state='queued', slskd_search_id=?,"
                " slskd_error=NULL, slskd_queued_at=datetime('now'), custom_search=NULL WHERE id=?",
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
        "SELECT * FROM tracks WHERE slskd_state='queued' AND slskd_search_id IS NOT NULL LIMIT 5"
    ).fetchall():
        meta = TrackMeta(t["artist"] or "", t["album"] or "", t["title"] or "",
                         t["track_number"] or 0, t["source_id"] or "")
        results = slskd.get_search_results(t["slskd_search_id"])
        if results is None:
            continue  # search still running
        tried = set(u for u in (t["slskd_tried_users"] or "").split(",") if u)
        if results:
            logger.info(f"[slskd] {len(results)} results for '{meta.title}', selecting best (tried: {len(tried)})")
        scored = sorted(
            ((slskd.score_result(r, meta), i, r) for i, r in enumerate(results)
             if r.get("username") not in tried),
            key=lambda x: x[0],
            reverse=True,
        )
        if scored and scored[0][0] > 0:
            best = scored[0][2]
            key = (best["username"], best["filename"])
            if key in _queued_this_tick:
                # Already requested this file this tick (from album path) — just mark downloading
                tried.add(best["username"])
                conn.execute(
                    "UPDATE tracks SET slskd_state='downloading', slskd_tried_users=?,"
                    " slskd_download_user=?, slskd_download_filename=? WHERE id=?",
                    (",".join(tried), best["username"], best["filename"], t["id"]))
                conn.commit()
                continue
            logger.info(f"[slskd] Downloading from {best['username']}: {best['filename']}")
            ok, msg = slskd.download_file(best["username"], best["filename"], best.get("size", 0))
            if ok:
                _queued_this_tick.add(key)
                tried.add(best["username"])
                conn.execute(
                    "UPDATE tracks SET slskd_state='downloading', slskd_tried_users=?,"
                    " slskd_download_user=?, slskd_download_filename=? WHERE id=?",
                    (",".join(tried), best["username"], best["filename"], t["id"]),
                )
            else:
                logger.warning(f"[slskd] Download request failed for '{meta.title}': {msg}")
                tried.add(best["username"])
                if len(tried) < 4:
                    logger.info(f"[slskd] Retrying '{meta.title}' (attempt {len(tried)+1})")
                    conn.execute(
                        "UPDATE tracks SET slskd_state='queued', slskd_tried_users=?, slskd_error=? WHERE id=?",
                        (",".join(tried), f"Retrying (attempt {len(tried)}): {msg[:80]}", t["id"]),
                    )
                else:
                    conn.execute(
                        "UPDATE tracks SET slskd_state='failed', slskd_error=? WHERE id=?",
                        (f"All peers failed: {msg[:100]}", t["id"]),
                    )
        else:
            # No usable results — retry with progressively simpler queries, then ask user
            attempt = t["slskd_search_attempt"] or 0
            if attempt == 0:
                logger.info(f"[slskd] No results for '{meta.title}', retrying with title-only search")
                conn.execute(
                    "UPDATE tracks SET slskd_state='pending', slskd_search_attempt=1,"
                    " slskd_error='No results — retrying with title-only search…',"
                    " slskd_search_id=NULL WHERE id=?",
                    (t["id"],),
                )
            else:
                logger.warning(f"[slskd] Still no results for '{meta.title}' after {attempt+1} attempts")
                conn.execute(
                    "UPDATE tracks SET slskd_state='needs_search',"
                    " slskd_error='No results found automatically. Enter a custom search.' WHERE id=?",
                    (t["id"],),
                )
        conn.commit()

    # ── TRANSFER STATUS CHECK ──────────────────────────────────────────────
    # Ask slskd whether our in-flight downloads errored or were cancelled so we
    # can immediately retry with a different peer instead of waiting forever.
    dl_tracks = conn.execute(
        "SELECT * FROM tracks WHERE slskd_state='downloading'"
        " AND slskd_download_user IS NOT NULL AND slskd_download_filename IS NOT NULL"
        " LIMIT 30"
    ).fetchall()
    if dl_tracks:
        # Batch API calls: one per unique user
        by_user: dict[str, list] = {}
        for t in dl_tracks:
            by_user.setdefault(t["slskd_download_user"], []).append(t)
        for username, user_tracks in by_user.items():
            transfer_map = slskd.get_user_transfers(username)
            if not transfer_map:
                continue  # slskd unreachable or no data yet
            for t in user_tracks:
                state = transfer_map.get(t["slskd_download_filename"], "")
                # Terminal failure states — retry with a different peer
                if state in ("Errored", "Cancelled", "TimedOut", "Aborted"):
                    tried = set(u for u in (t["slskd_tried_users"] or "").split(",") if u)
                    tried.add(username)
                    logger.info(
                        f"[slskd] Transfer {state} for '{t['title']}' from {username}"
                        f" — queuing retry (tried {len(tried)} peer(s))"
                    )
                    conn.execute(
                        "UPDATE tracks SET slskd_state='pending',"
                        " slskd_tried_users=?, slskd_error=?,"
                        " slskd_download_user=NULL, slskd_download_filename=NULL,"
                        " slskd_search_id=NULL WHERE id=?",
                        (",".join(tried),
                         f"Transfer {state} from {username}; trying another peer",
                         t["id"])
                    )
        conn.commit()

    # Playlists touched by completions this tick — synced once at the end (debounce)
    playlists_to_sync: dict[int, str] = {}

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
            try:
                ok, result = Organizer.move_file(candidate, target,
                                                   force_overwrite=bool(t["force_overwrite"]))
                logger.info(f"[slskd] Organized to: {result}")
                tag_file(Path(result), t)
                aid_score = _acoustid.verify(Path(result), t["artist"] or "", t["title"] or "")
                conn.execute(
                    "UPDATE tracks SET slskd_state='completed', local_path=?, acoustid_score=?,"
                    " slskd_download_user=NULL, slskd_download_filename=NULL WHERE id=?",
                    (result, aid_score, t["id"]))
                conn.commit()
                job = conn.execute("SELECT playlist_name FROM import_jobs WHERE id=?",
                                   (t["job_id"],)).fetchone()
                if job and job["playlist_name"]:
                    playlists_to_sync[t["job_id"]] = job["playlist_name"]
            except Exception as ex:
                logger.error(f"[slskd] Failed to move '{t['title']}': {ex}")
                conn.execute("UPDATE tracks SET slskd_state='failed', slskd_error=? WHERE id=?",
                             (f"Move failed: {ex}", t["id"]))
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

        # Build ordered list of instances to try: configured first, then user-added fallbacks, then built-in list
        configured = mc.base
        user_fallbacks = [u.strip().rstrip("/") for u in (get_setting("monochrome_fallbacks") or "").splitlines() if u.strip()]
        all_fallbacks = user_fallbacks + [u.rstrip("/") for u in MONOCHROME_FALLBACK_URLS]
        instances = [configured] + [u for u in all_fallbacks if u != configured]

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
            if Path(final).exists():
                tag_file(Path(final), t)
                aid_score = _acoustid.verify(Path(final), t["artist"] or "", t["title"] or "")
            else:
                aid_score = None
            conn.execute("UPDATE tracks SET slskd_state='completed', local_path=?, acoustid_score=? WHERE id=?",
                         (final, aid_score, t["id"]))
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
        if ok:
            job = conn.execute("SELECT playlist_name FROM import_jobs WHERE id=?",
                               (t["job_id"],)).fetchone()
            if job and job["playlist_name"]:
                playlists_to_sync[t["job_id"]] = job["playlist_name"]

    conn.close()

    # Flush playlist syncs once per affected playlist (debounced from per-track)
    for sync_job_id, sync_name in playlists_to_sync.items():
        try:
            write_playlist_m3u(sync_job_id, sync_name)
        except Exception as ex:
            logger.error(f"[m3u] Auto-sync failed for '{sync_name}': {ex}")


# ---------------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------------

_scan_state: dict = {"in_progress": False, "count": 0, "source": "", "last_at": ""}
_scan_state_lock = threading.Lock()

app = Flask(__name__)
app.secret_key = os.getenv("APP_SECRET", "change-me")
app.permanent_session_lifetime = timedelta(days=30)

init_db()

_stop_event = threading.Event()
_worker = threading.Thread(target=run_worker, args=(_stop_event,), daemon=True)
_worker.start()

_providers = [SpotifyProvider(), AppleProvider(), TidalProvider()]
_deezer = DeezerProvider()
_apple_music_client = AppleMusicClient()
_listenbrainz_client = ListenBrainzClient()
_acoustid = AcoustIDClient()


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
            session.permanent = True
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
    stats = {"total": 0, "pending": 0, "downloading": 0, "completed": 0, "failed": 0, "needs_search": 0}
    for t in tracks:
        s = t["slskd_state"] or "pending"
        stats["total"] += 1
        if s in ("pending", "queued", "album_queued"):
            stats["pending"] += 1
        elif s == "needs_search":
            stats["needs_search"] += 1
        elif s in stats:
            stats[s] += 1
    return render_template("index.html", tracks=tracks, stats=stats, title="Queue")


@app.route("/search")
def search():
    return render_template("search.html", title="Search")


@app.route("/api/playlists/<int:job_id>/regenerate", methods=["POST"])
def api_regenerate_m3u(job_id):
    conn = get_conn()
    job = conn.execute("SELECT * FROM import_jobs WHERE id=?", (job_id,)).fetchone()
    if not job:
        conn.close()
        return jsonify({"ok": False, "error": "Job not found"}), 404
    playlist_name = (request.get_json() or {}).get("name") or job["playlist_name"] or ""
    if not playlist_name:
        conn.close()
        return jsonify({"ok": False, "error": "No playlist name configured for this import"}), 400
    source_url = job["source_url"] or ""

    merged, idx_count, fs_count, miss_count = _build_playlist_entries(
        conn, job_id, playlist_name, source_url, allow_refetch=True
    )
    conn.close()

    if not merged:
        return jsonify({"ok": False, "error": "No completed tracks found for this playlist"}), 404
    n_dl = len(merged) - idx_count - fs_count
    library = get_setting("library_path") or "/music"
    safe = re.sub(r'[<>:"/\\|?*]', "", playlist_name).strip()[:120] or "playlist"
    m3u_path = Path(library) / f"{safe}.m3u"
    lines = ["#EXTM3U"]
    for r in merged:
        lines.append(f"#EXTINF:0,{r['artist'] or ''} - {r['title'] or ''}")
        lines.append(r["path"])
    m3u_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    logger.info(
        f"[m3u] Regenerated {safe}.m3u — {len(merged)} tracks "
        f"({n_dl} downloads + {idx_count} index + {fs_count} fs-walk, {miss_count} missing)"
    )
    _update_navidrome_playlist(playlist_name, merged)
    return jsonify({"ok": True, "count": len(merged), "path": str(m3u_path), "missing": miss_count})


@app.route("/api/playlists/<int:job_id>/retry-missing", methods=["POST"])
def api_retry_missing(job_id):
    """Reset all needs_search tracks for a playlist to pending so the worker re-tries them."""
    conn = get_conn()
    job = conn.execute("SELECT id FROM import_jobs WHERE id=?", (job_id,)).fetchone()
    if not job:
        conn.close()
        return jsonify({"ok": False, "error": "Job not found"}), 404
    conn.execute(
        "UPDATE tracks SET slskd_state='pending', slskd_search_attempt=0,"
        " custom_search=NULL, slskd_error=NULL, slskd_search_id=NULL, slskd_tried_users=''"
        " WHERE job_id=? AND slskd_state='needs_search'",
        (job_id,),
    )
    conn.commit()
    affected = conn.execute("SELECT changes()").fetchone()[0]
    conn.close()
    return jsonify({"ok": True, "queued": affected})


@app.route("/api/playlists/<int:job_id>/diff")
def api_playlist_diff(job_id):
    """Re-fetch the source playlist and return tracks not yet in this job."""
    conn = get_conn()
    job = conn.execute("SELECT * FROM import_jobs WHERE id=?", (job_id,)).fetchone()
    if not job:
        conn.close()
        return jsonify({"ok": False, "error": "Job not found"}), 404

    source_url = job["source_url"] or ""
    source = (job["source"] or "").lower()
    if not source_url:
        conn.close()
        return jsonify({"ok": False, "error": "No source URL for this playlist"}), 400

    provider = next((p for p in _providers if p.supports(source_url)), None)
    if not provider:
        conn.close()
        return jsonify({"ok": False, "error": "No provider available for this URL"}), 400

    try:
        _source_type, source_tracks = provider.parse(source_url)
    except Exception as ex:
        conn.close()
        return jsonify({"ok": False, "error": str(ex)}), 500

    # All known tracks across every job — so tracks downloaded via any playlist
    # are not reported as "new" here.
    existing = conn.execute("SELECT artist, title FROM tracks").fetchall()
    conn.close()
    existing_set = {(
        (r["artist"] or "").lower().strip(),
        (r["title"] or "").lower().strip(),
    ) for r in existing}

    new_tracks = []
    for t in source_tracks:
        key = ((t.artist or "").lower().strip(), (t.title or "").lower().strip())
        if key not in existing_set:
            new_tracks.append({"artist": t.artist, "title": t.title,
                                "album": t.album, "cover_url": t.cover_url})

    return jsonify({"ok": True, "new_tracks": new_tracks, "total_source": len(source_tracks)})


@app.route("/api/logs")
def api_logs():
    """Return the last N lines from the in-memory log buffer."""
    with _log_buffer_lock:
        lines = list(_log_buffer)
    return jsonify({"lines": lines})


@app.route("/api/status/slskd")
def api_status_slskd():
    """Proxy a lightweight slskd health check: version + active download count."""
    slskd = SlskdClient()
    result = {"connected": False, "version": "", "active": 0, "speed_kbps": 0}
    try:
        r = requests.get(f"{slskd.base}/api/v0/application",
                         headers=slskd._headers(), auth=slskd._auth(), timeout=5)
        if r.status_code == 200:
            data = r.json()
            result["connected"] = True
            v = data.get("version", "")
            if isinstance(v, dict):
                result["version"] = f"{v.get('major','')}.{v.get('minor','')}.{v.get('patch','')}".strip(".")
            else:
                result["version"] = str(v)
        else:
            r2 = requests.get(f"{slskd.base}/api/v1/application",
                              headers=slskd._headers(), auth=slskd._auth(), timeout=5)
            if r2.status_code == 200:
                result["connected"] = True
                v2 = r2.json().get("version", "")
                if isinstance(v2, dict):
                    result["version"] = f"{v2.get('major','')}.{v2.get('minor','')}.{v2.get('patch','')}".strip(".")
                else:
                    result["version"] = str(v2)
    except Exception:
        pass
    # Count active downloads from our DB (cheaper than polling slskd transfers)
    try:
        conn = get_conn()
        result["active"] = conn.execute(
            "SELECT COUNT(*) FROM tracks WHERE slskd_state='downloading'"
        ).fetchone()[0]
        conn.close()
    except Exception:
        pass
    return jsonify(result)


@app.route("/playlists")
def playlists():
    conn = get_conn()
    jobs = conn.execute("""
        SELECT j.id, j.source, j.source_type, j.source_url,
               j.playlist_name, j.created_at,
               COUNT(t.id) AS total,
               SUM(CASE WHEN t.slskd_state='completed' THEN 1 ELSE 0 END) AS done,
               SUM(CASE WHEN t.slskd_state='needs_search' THEN 1 ELSE 0 END) AS needs_fix,
               SUM(CASE WHEN t.slskd_state IN ('pending','queued','album_queued','downloading') THEN 1 ELSE 0 END) AS in_progress,
               SUM(CASE WHEN t.slskd_state='failed' THEN 1 ELSE 0 END) AS failed
        FROM import_jobs j
        LEFT JOIN tracks t ON t.job_id = j.id
        WHERE j.source_url != ''
        GROUP BY j.id
        ORDER BY j.created_at DESC
    """).fetchall()
    missing = {}
    for job in jobs:
        if job["needs_fix"] and job["needs_fix"] > 0:
            missing[job["id"]] = conn.execute(
                "SELECT id, artist, title, album, slskd_error FROM tracks"
                " WHERE job_id=? AND slskd_state='needs_search'",
                (job["id"],),
            ).fetchall()
    conn.close()
    return render_template("playlists.html", jobs=jobs, missing=missing, title="Playlists")


@app.route("/library")
def library():
    music_path_str = str(get_setting("library_path") or "/music")
    conn = get_conn()
    lib_rows = conn.execute(
        "SELECT artist, title, album, path, user_rating, cover_art_id FROM library_index"
    ).fetchall()
    conn.close()

    needs_scan = False
    if lib_rows:
        tracks = []
        for r in lib_rows:
            p = r["path"] or ""
            ext = Path(p).suffix[1:].upper() if p else ""
            tracks.append({
                "artist":      r["artist"] or "",
                "album":       r["album"] or "",
                "title":       r["title"] or "",
                "size_mb":     0,
                "ext":         ext,
                "path":        p,
                "user_rating": r["user_rating"],
                "cover_url":   f"/api/library/cover/{r['cover_art_id']}" if r["cover_art_id"] else "",
            })
    else:
        needs_scan = True
        tracks = []
        music_path = Path(music_path_str)
        if music_path.exists():
            num_re = re.compile(r"^\d+\s*[-\.]\s*")
            for f in sorted(music_path.rglob("*")):
                if not (f.is_file() and f.suffix.lower() in AUDIO_EXTS):
                    continue
                parts = f.relative_to(music_path).parts
                artist = parts[0] if len(parts) >= 3 else ""
                album  = parts[1] if len(parts) >= 3 else (parts[0] if len(parts) == 2 else "")
                title  = num_re.sub("", f.stem)
                size_mb = round(f.stat().st_size / 1_048_576, 1)
                tracks.append({
                    "artist":      artist,
                    "album":       album,
                    "title":       title,
                    "size_mb":     size_mb,
                    "ext":         f.suffix[1:].upper(),
                    "path":        str(f),
                    "user_rating": None,
                    "cover_url":   "",
                })
    return render_template("library.html", tracks=tracks, music_path=music_path_str,
                           needs_scan=needs_scan, title="Library")


@app.route("/api/library/cover/<path:cover_art_id>")
def api_library_cover(cover_art_id):
    nav_url  = (get_setting("navidrome_url") or "").rstrip("/")
    nav_user = get_setting("navidrome_user") or ""
    nav_pass = get_setting("navidrome_pass") or ""
    if not nav_url or not nav_user:
        return "", 404
    size = request.args.get("size", "120")
    try:
        r = requests.get(
            f"{nav_url}/rest/getCoverArt",
            params={"u": nav_user, "p": nav_pass, "v": "1.16.1",
                    "c": "slskdsync", "id": cover_art_id, "size": size},
            timeout=5,
        )
        if r.status_code != 200:
            return "", 404
        resp = Response(r.content, content_type=r.headers.get("content-type", "image/jpeg"))
        resp.headers["Cache-Control"] = "max-age=86400, public"
        return resp
    except Exception:
        return "", 404


@app.route("/settings", methods=["GET", "POST"])
def settings():
    keys = [
        "library_path", "download_watch_path", "folder_template",
        "slskd_url", "slskd_user", "slskd_pass", "slskd_api_key",
        "monochrome_url", "monochrome_fallbacks",
        "navidrome_url", "navidrome_user", "navidrome_pass",
        "apple_team_id", "apple_key_id", "apple_private_key",
        "listenbrainz_username",
        "acoustid_api_key",
        "quality", "replace_existing",
        "library_scan_interval",
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
    return render_template("settings.html", settings={k: get_setting(k) for k in keys}, title="Settings")


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
# Discovery page
# ---------------------------------------------------------------------------

@app.route("/discover")
def discover():
    apple_configured = bool(
        get_setting("apple_team_id").strip()
        and get_setting("apple_key_id").strip()
        and get_setting("apple_private_key").strip()
    )
    lb_username = get_setting("listenbrainz_username").strip()
    return render_template("discover.html",
                           apple_configured=apple_configured,
                           lb_username=lb_username,
                           title="Discover")


@app.route("/api/discover/charts")
def api_discover_charts():
    return jsonify(_deezer.get_charts(limit=25))


@app.route("/api/discover/genres")
def api_discover_genres():
    return jsonify(_deezer.get_genres())


@app.route("/api/discover/genre/<genre_id>")
def api_discover_genre(genre_id):
    return jsonify(_deezer.get_genre_charts(genre_id, limit=25))


@app.route("/api/discover/artist-radio/<artist_id>")
def api_discover_artist_radio(artist_id):
    return jsonify(_deezer.get_artist_radio(artist_id, limit=20))


@app.route("/api/discover/apple")
def api_discover_apple():
    sub = request.args.get("sub", "charts")
    if sub == "new":
        return jsonify(_apple_music_client.get_new_releases())
    return jsonify(_apple_music_client.get_charts())


@app.route("/api/discover/listenbrainz")
def api_discover_listenbrainz():
    username = get_setting("listenbrainz_username").strip()
    if not username:
        return jsonify({"error": "ListenBrainz username not configured"}), 400
    return jsonify(_listenbrainz_client.get_recommendations(username, limit=25))


@app.route("/api/discover/listenbrainz/history")
def api_discover_lb_history():
    username = get_setting("listenbrainz_username").strip()
    if not username:
        return jsonify({"error": "ListenBrainz username not configured"}), 400
    return jsonify(_listenbrainz_client.get_recent_listens(username, limit=30))


@app.route("/api/tracks/<int:track_id>", methods=["DELETE"])
def delete_track(track_id):
    conn = get_conn()
    track = conn.execute("SELECT slskd_search_id, job_id FROM tracks WHERE id=?",
                         (track_id,)).fetchone()
    conn.execute("DELETE FROM tracks WHERE id=?", (track_id,))
    conn.commit()
    if track:
        slskd = SlskdClient()
        # Only cancel the individual search if no other track shares this search_id
        if track["slskd_search_id"]:
            shared = conn.execute(
                "SELECT COUNT(*) FROM tracks WHERE slskd_search_id=? AND id!=?",
                (track["slskd_search_id"], track_id)
            ).fetchone()[0]
            if shared == 0:
                slskd.cancel_search(track["slskd_search_id"])
        # Cancel job-level album search only when the entire job is gone
        remaining = conn.execute(
            "SELECT COUNT(*) FROM tracks WHERE job_id=?", (track["job_id"],)
        ).fetchone()[0]
        if remaining == 0:
            job = conn.execute(
                "SELECT album_search_id FROM import_jobs WHERE id=?", (track["job_id"],)
            ).fetchone()
            if job and job["album_search_id"]:
                slskd.cancel_search(job["album_search_id"])
    conn.close()
    return jsonify({"ok": True})


@app.route("/api/tracks/<int:track_id>/retry", methods=["POST"])
def retry_track(track_id):
    """Reset any track to pending, optionally with a custom search query."""
    data = request.get_json(silent=True) or {}
    query = (data.get("query") or "").strip()
    conn = get_conn()
    track = conn.execute("SELECT slskd_search_id FROM tracks WHERE id=?", (track_id,)).fetchone()
    # Only cancel the slskd search if no other track is still using it
    if track and track["slskd_search_id"]:
        shared = conn.execute(
            "SELECT COUNT(*) FROM tracks WHERE slskd_search_id=? AND id!=?",
            (track["slskd_search_id"], track_id)
        ).fetchone()[0]
        if shared == 0:
            SlskdClient().cancel_search(track["slskd_search_id"])
    conn.execute(
        "UPDATE tracks SET slskd_state='pending', slskd_search_attempt=0,"
        " custom_search=?, slskd_error=NULL, slskd_search_id=NULL, slskd_tried_users='' WHERE id=?",
        (query or None, track_id),
    )
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/api/library/scan", methods=["POST"])
def api_library_scan():
    threading.Thread(target=scan_library, daemon=True).start()
    return jsonify({"ok": True, "message": "Library scan started in background"})


@app.route("/api/library/scan/status")
def api_library_scan_status():
    with _scan_state_lock:
        return jsonify(dict(_scan_state))


@app.route("/api/library/index")
def api_library_index():
    """Lightweight list of downloaded track keys for the search page to check."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT artist, title FROM tracks WHERE slskd_state='completed'"
        " UNION SELECT artist, title FROM library_index"
    ).fetchall()
    conn.close()
    return jsonify([
        {"a": (r["artist"] or "").lower().strip(), "t": (r["title"] or "").lower().strip()}
        for r in rows
    ])


@app.route("/api/library/redownload", methods=["POST"])
def api_library_redownload():
    data = request.get_json() or {}
    artist = (data.get("artist") or "").strip()
    title  = (data.get("title")  or "").strip()
    album  = (data.get("album")  or "").strip()
    if not artist or not title:
        return jsonify({"ok": False, "error": "artist and title required"}), 400
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO import_jobs(source,source_type,source_url,nav_playlist,status) VALUES(?,?,?,?,?)",
        ("library", "redownload", "", 0, "queued"),
    )
    cur.execute(
        "INSERT INTO tracks(job_id,artist,album,title,download_source,force_overwrite)"
        " VALUES(?,?,?,?,?,?)",
        (cur.lastrowid, artist, album, title, "slskd", 1),
    )
    conn.commit()
    conn.close()
    logger.info(f"[library] Re-download queued: {artist} — {title}")
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# Connection tests
# ---------------------------------------------------------------------------

@app.route("/api/test/slskd")
def test_slskd():
    ok, msg = SlskdClient().ping()
    return jsonify({"ok": ok, "message": msg})


@app.route("/api/test/apple")
def test_apple():
    team_id     = get_setting("apple_team_id").strip()
    key_id      = get_setting("apple_key_id").strip()
    private_key = get_setting("apple_private_key").strip()
    # Report what's saved so the user can see which fields landed
    status = (
        f"Team ID: {'✓ ' + team_id if team_id else '✗ missing'} | "
        f"Key ID: {'✓ ' + key_id if key_id else '✗ missing'} | "
        f"Private key: {'✓ ' + str(len(private_key)) + ' chars' if private_key else '✗ missing'}"
    )
    try:
        token = _apple_jwt()
        r = requests.get(
            "https://api.music.apple.com/v1/catalog/us/search",
            headers={"Authorization": f"Bearer {token}"},
            params={"term": "test", "types": "songs", "limit": 1},
            timeout=10,
        )
        if r.status_code == 200:
            return jsonify({"ok": True, "message": f"Connected ✓\n{status}"})
        return jsonify({"ok": False, "message": f"HTTP {r.status_code}: {r.text[:120]}\n{status}"})
    except Exception as ex:
        return jsonify({"ok": False, "message": f"{ex}\n{status}"})


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


@app.route("/api/queue/status")
def api_queue_status():
    """Lightweight polling endpoint — returns only what changes between refreshes."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT id, slskd_state, slskd_error, acoustid_score FROM tracks ORDER BY id DESC LIMIT 100"
    ).fetchall()
    conn.close()
    stats = {"total": 0, "pending": 0, "downloading": 0, "completed": 0,
             "failed": 0, "needs_search": 0}
    tracks = []
    for r in rows:
        s = r["slskd_state"] or "pending"
        stats["total"] += 1
        if s in ("pending", "queued", "album_queued"):
            stats["pending"] += 1
        elif s == "needs_search":
            stats["needs_search"] += 1
        elif s in stats:
            stats[s] += 1
        tracks.append({"id": r["id"], "state": s,
                        "error": (r["slskd_error"] or "")[:80],
                        "acoustid_score": r["acoustid_score"]})
    return jsonify({"stats": stats, "tracks": tracks})


@app.route("/api/queue/action", methods=["POST"])
def api_queue_action():
    action = (request.get_json() or {}).get("action", "")
    conn = get_conn()
    if action == "clear_failed":
        conn.execute("DELETE FROM tracks WHERE slskd_state='failed'")
    elif action == "clear_completed":
        conn.execute("DELETE FROM tracks WHERE slskd_state='completed'")
    elif action == "clear_all":
        conn.execute("DELETE FROM tracks WHERE slskd_state IN ('failed','completed')")
    elif action == "retry_failed":
        # Keep slskd_search_attempt=1 so title-only search is tried next
        # instead of re-running the exact same artist+title query that already failed.
        conn.execute(
            "UPDATE tracks SET slskd_state='pending', slskd_error=NULL, slskd_search_id=NULL,"
            " slskd_tried_users='',"
            " slskd_search_attempt=CASE WHEN slskd_search_attempt>0 THEN 1 ELSE 0 END"
            " WHERE slskd_state='failed'"
        )
    elif action == "retry_downloading":
        conn.execute(
            "UPDATE tracks SET slskd_state='pending', slskd_error=NULL, slskd_search_id=NULL,"
            " slskd_tried_users='' WHERE slskd_state IN ('downloading','queued','album_queued')"
        )
    elif action == "clear_needs_search":
        conn.execute("DELETE FROM tracks WHERE slskd_state='needs_search'")
    else:
        conn.close()
        return jsonify({"ok": False, "error": "unknown action"}), 400
    conn.commit()
    affected = conn.execute("SELECT changes()").fetchone()[0]
    conn.close()
    return jsonify({"ok": True, "affected": affected})


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
    skipped = 0
    for t in tracks:
        title = (t.get("title") or "").strip()
        artist = (t.get("artist") or "").strip()
        if not title or not artist:
            continue
        if _already_in_library(conn, artist, title):
            skipped += 1
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
    logger.info(f"[queue] Batch queued {count} tracks via {source} ({skipped} skipped, in library)")
    msg = f"Queued {count} tracks via {source}"
    if skipped:
        msg += f" ({skipped} already in library, skipped)"
    return jsonify({"ok": True, "count": count, "skipped": skipped, "message": msg})


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
    if _already_in_library(conn, artist, title):
        conn.close()
        return jsonify({"ok": False, "skipped": True,
                        "message": f"\"{title}\" is already in your library"})
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

    create_m3u = request.form.get("create_m3u") == "1"
    playlist_name = (request.form.get("playlist_name") or "").strip() if create_m3u else None

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO import_jobs(source,source_type,source_url,nav_playlist,status,playlist_name) VALUES(?,?,?,?,?,?)",
        (provider.name, source_type, url, 0, "queued", playlist_name or None),
    )
    job_id = cur.lastrowid
    # Store the complete track list before dedup so M3U regeneration can use it later
    cur.executemany(
        "INSERT INTO playlist_tracks(job_id, artist, title, album, track_number) VALUES(?,?,?,?,?)",
        [(job_id, t.artist, t.title, t.album, t.track_number) for t in tracks],
    )
    skipped = 0
    for t in tracks:
        if _already_in_library(conn, t.artist, t.title):
            skipped += 1
            continue
        cur.execute(
            "INSERT INTO tracks(job_id,artist,album,title,track_number,source_id,cover_url,download_source)"
            " VALUES(?,?,?,?,?,?,?,?)",
            (job_id, t.artist, t.album, t.title, t.track_number, t.source_id, t.cover_url, dl_source),
        )
    queued = len(tracks) - skipped
    conn.commit()
    conn.close()
    msg = f"Queued {queued} tracks from {provider.name} ({source_type})"
    if skipped:
        msg += f" · {skipped} already in library, skipped"
    flash(msg, "ok")
    return redirect(url_for("index"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5035)

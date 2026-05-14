import os
import re
import json
import sqlite3
import threading
import time
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

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
    migrations = [
        ("cover_url", "TEXT"),
        ("download_source", "TEXT DEFAULT 'slskd'"),
        ("slskd_search_id", "TEXT"),
    ]
    for col, ddl in migrations:
        if col not in existing_cols:
            cur.execute(f"ALTER TABLE tracks ADD COLUMN {col} {ddl}")

    defaults = {
        "library_path": "/music",
        "slskd_url": "http://slskd:5030",
        "slskd_user": "",
        "slskd_pass": "",
        "slskd_api_key": "",
        "monochrome_url": "https://api.monochrome.tf",
        "navidrome_url": "http://navidrome:4533",
        "navidrome_user": "",
        "navidrome_pass": "",
        "quality": "lossless",
        "replace_existing": "0",
        "folder_template": "{artist}/{album}/{track_number:02d} - {title}{ext}",
        "download_watch_path": "/downloads",
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
# Music source providers (for URL import)
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
            raise RuntimeError("Spotify credentials missing. Set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET.")

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
        base = (get_setting("monochrome_url") or "https://api.monochrome.tf").rstrip("/")

        if "/track/" in url:
            m = re.search(r"/track/(\d+)", url)
            if not m:
                raise RuntimeError("Could not parse TIDAL track ID from URL")
            tid = m.group(1)
            r = requests.get(f"{base}/info/{tid}", timeout=15)
            if r.status_code != 200:
                raise RuntimeError(f"Monochrome API error {r.status_code} — is the monochrome URL configured?")
            d = r.json()
            cover = _tidal_cover_url(d.get("album", {}).get("cover", ""))
            return "track", [TrackMeta(
                artist=(d.get("artist") or {}).get("name", "Unknown Artist"),
                album=(d.get("album") or {}).get("title", "Unknown Album"),
                title=d.get("title", "Unknown Title"),
                track_number=d.get("trackNumber", 0),
                source_id=tid,
                cover_url=cover,
            )]

        if "/album/" in url:
            m = re.search(r"/album/(\d+)", url)
            if not m:
                raise RuntimeError("Could not parse TIDAL album ID from URL")
            aid = m.group(1)
            tracks_r = requests.get(f"{base}/album/{aid}/tracks", timeout=15)
            album_r = requests.get(f"{base}/album/{aid}", timeout=15)
            if tracks_r.status_code != 200:
                raise RuntimeError(f"Monochrome API error {tracks_r.status_code}")
            td = tracks_r.json()
            ad = album_r.json() if album_r.status_code == 200 else {}
            album_name = ad.get("title", "Unknown Album")
            cover = _tidal_cover_url(ad.get("cover", ""))
            tracks = []
            for t in td.get("items", []):
                tracks.append(TrackMeta(
                    artist=(t.get("artist") or ad.get("artist") or {}).get("name", "Unknown Artist"),
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
            r = requests.get(f"{base}/playlist/{pid}/tracks", timeout=15)
            if r.status_code != 200:
                raise RuntimeError(f"Monochrome API error {r.status_code}")
            d = r.json()
            tracks = []
            for t in d.get("items", []):
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

        raise RuntimeError("Unsupported TIDAL URL type (track, album, or playlist links supported)")


def _tidal_cover_url(cover_id: str) -> str:
    if not cover_id:
        return ""
    return f"https://resources.tidal.com/images/{cover_id.replace('-', '/')}/320x320.jpg"


# ---------------------------------------------------------------------------
# Deezer — free catalog search (no auth required)
# ---------------------------------------------------------------------------

class DeezerProvider:
    def search(self, query: str, limit: int = 25) -> list[dict]:
        try:
            r = requests.get(
                "https://api.deezer.com/search",
                params={"q": query, "limit": limit},
                timeout=12,
            )
            r.raise_for_status()
            results = []
            for item in r.json().get("data", []):
                results.append({
                    "id": str(item.get("id", "")),
                    "title": item.get("title", ""),
                    "artist": item.get("artist", {}).get("name", ""),
                    "album": item.get("album", {}).get("title", ""),
                    "cover": item.get("album", {}).get("cover_medium", ""),
                    "duration": item.get("duration", 0),
                    "source": "deezer",
                })
            return results
        except Exception:
            return []

    def get_album_tracks(self, album_id: str) -> tuple[str, list[TrackMeta]]:
        album_r = requests.get(f"https://api.deezer.com/album/{album_id}", timeout=12).json()
        album_name = album_r.get("title", "Unknown Album")
        cover = album_r.get("cover_medium", "")
        tracks_r = requests.get(f"https://api.deezer.com/album/{album_id}/tracks", timeout=12).json()
        tracks = []
        for i, t in enumerate(tracks_r.get("data", []), 1):
            artist = (t.get("artist") or album_r.get("artist") or {}).get("name", "Unknown Artist")
            tracks.append(TrackMeta(
                artist=artist,
                album=album_name,
                title=t.get("title", ""),
                track_number=t.get("track_position", i),
                source_id=str(t.get("id", "")),
                cover_url=cover,
            ))
        return album_name, tracks


# ---------------------------------------------------------------------------
# Monochrome — TIDAL proxy client
# ---------------------------------------------------------------------------

class MonochromeClient:
    def __init__(self):
        self.base = (get_setting("monochrome_url") or "https://api.monochrome.tf").rstrip("/")

    def search(self, query: str, limit: int = 20) -> list[dict]:
        try:
            r = requests.get(
                f"{self.base}/search/",
                params={"s": query, "limit": limit},
                timeout=12,
            )
            r.raise_for_status()
            results = []
            for item in (r.json().get("tracks") or {}).get("items", []):
                cover = _tidal_cover_url((item.get("album") or {}).get("cover", ""))
                results.append({
                    "id": str(item.get("id", "")),
                    "title": item.get("title", ""),
                    "artist": (item.get("artist") or {}).get("name", ""),
                    "album": (item.get("album") or {}).get("title", ""),
                    "cover": cover,
                    "duration": item.get("duration", 0),
                    "source": "tidal",
                })
            return results
        except Exception:
            return []

    def find_tidal_id(self, artist: str, title: str) -> Optional[str]:
        results = self.search(f"{artist} {title}", limit=5)
        if not results:
            return None
        title_l = title.lower()
        artist_l = artist.lower().split(",")[0].strip()
        for r in results:
            if title_l in r["title"].lower() and artist_l in r["artist"].lower():
                return r["id"]
        return results[0]["id"] if results else None

    def download_track(self, tidal_id: str, artist: str, title: str) -> tuple[bool, str]:
        quality_map = {"lossless": "LOSSLESS", "high": "HIGH", "normal": "HIGH", "low": "LOW"}
        quality = quality_map.get(get_setting("quality"), "LOSSLESS")
        try:
            r = requests.get(
                f"{self.base}/track/{tidal_id}",
                params={"quality": quality},
                timeout=30,
            )
            if r.status_code != 200:
                return False, f"Monochrome API returned {r.status_code}"
            data = r.json()
            url = data.get("url") or (data.get("urls") or [None])[0]
            if not url:
                return False, "No direct stream URL in response (track may use DRM or DASH streaming)"

            ext = ".flac" if quality in ("LOSSLESS", "HI_RES_LOSSLESS") else ".m4a"
            watch = Path(get_setting("download_watch_path"))
            watch.mkdir(parents=True, exist_ok=True)
            safe = re.sub(r'[<>:"/\\|?*]', "", f"{artist} - {title}").strip()[:180]
            dest = watch / f"{safe}{ext}"

            with requests.get(url, stream=True, timeout=300) as resp:
                resp.raise_for_status()
                with open(dest, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=65536):
                        f.write(chunk)
            return True, str(dest)
        except Exception as ex:
            return False, str(ex)


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

    def start_search(self, track: TrackMeta) -> tuple[bool, str, str]:
        query = f"{track.artist} {track.title}"
        for ep in ["/api/v0/searches", "/api/v1/searches"]:
            try:
                r = requests.post(
                    f"{self.base}{ep}",
                    headers=self._headers(),
                    auth=self._auth(),
                    json={"query": query},
                    timeout=25,
                )
                if r.status_code < 300:
                    return True, str(r.json().get("id", "")), "search started"
            except Exception as ex:
                last_err = str(ex)
        return False, "", locals().get("last_err", "Could not reach slskd")

    def get_search_results(self, search_id: str) -> list[dict]:
        if not search_id:
            return []
        try:
            r = requests.get(
                f"{self.base}/api/v0/searches/{search_id}",
                headers=self._headers(),
                auth=self._auth(),
                timeout=15,
            )
            if r.status_code != 200 or not r.json().get("isComplete"):
                return []
            r2 = requests.get(
                f"{self.base}/api/v0/searches/{search_id}/files",
                headers=self._headers(),
                auth=self._auth(),
                timeout=15,
            )
            if r2.status_code != 200:
                return []
            flat = []
            for user_result in r2.json():
                username = user_result.get("username", "")
                has_slot = user_result.get("hasFreeUploadSlot", False)
                for f in user_result.get("files", []):
                    flat.append({
                        "username": username,
                        "filename": f.get("filename", ""),
                        "size": f.get("size", 0),
                        "bitRate": f.get("bitRate", 0),
                        "has_slot": has_slot,
                    })
            return flat
        except Exception:
            return []

    def score_result(self, result: dict, track: TrackMeta) -> int:
        fn = result.get("filename", "")
        fn_l = fn.lower()
        ext = fn_l.rsplit(".", 1)[-1] if "." in fn_l else ""
        if ext not in {"flac", "mp3", "m4a", "ogg", "aac", "wav", "aif", "aiff", "opus", "wma"}:
            return -100

        score = 0
        if ext == "flac":
            score += 100
        elif ext in ("wav", "aif", "aiff"):
            score += 80
        elif ext == "m4a":
            score += 65
        elif ext == "mp3":
            br = result.get("bitRate", 0)
            score += 60 if br >= 320 else 50 if br >= 256 else 40 if br >= 192 else 30
        elif ext in ("ogg", "opus"):
            score += 55

        title_l = (track.title or "").lower()
        artist_l = (track.artist or "").lower().split(",")[0].strip()
        if title_l and title_l in fn_l:
            score += 30
        if artist_l and artist_l in fn_l:
            score += 20
        if result.get("has_slot"):
            score += 5
        return score

    def download_file(self, username: str, filename: str, size: int) -> tuple[bool, str]:
        try:
            r = requests.post(
                f"{self.base}/api/v0/transfers/downloads/{username}",
                headers=self._headers(),
                auth=self._auth(),
                json=[{"filename": filename, "size": size}],
                timeout=25,
            )
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
        library = Path(get_setting("library_path"))
        tmpl = get_setting("folder_template")
        ext = src_path.suffix
        rel = tmpl.format(
            artist=(track["artist"] or "Unknown Artist").strip().replace("/", "-"),
            album=(track["album"] or "Unknown Album").strip().replace("/", "-"),
            track_number=track["track_number"] or 0,
            title=(track["title"] or src_path.stem).strip().replace("/", "-"),
            ext=ext,
        )
        return library / rel

    @staticmethod
    def move_file(src: Path, dst: Path) -> tuple[bool, str]:
        dst.parent.mkdir(parents=True, exist_ok=True)
        if dst.exists() and get_setting("replace_existing") != "1":
            return False, f"exists: {dst}"
        shutil.move(str(src), str(dst))
        return True, str(dst)


def discover_download_for_track(track: sqlite3.Row) -> Optional[Path]:
    watch = Path(get_setting("download_watch_path"))
    if not watch.exists():
        return None
    title = (track["title"] or "").lower()
    artist = (track["artist"] or "").lower().split(",")[0].strip()
    for f in watch.glob("**/*"):
        if not f.is_file() or f.suffix.lower() not in AUDIO_EXTS:
            continue
        n = f.name.lower()
        if title and title in n and artist and artist in n:
            return f
    return None


# ---------------------------------------------------------------------------
# Background worker
# ---------------------------------------------------------------------------

def run_worker(stop_event: threading.Event):
    while not stop_event.is_set():
        try:
            _worker_tick()
        except Exception:
            pass
        time.sleep(20)


def _worker_tick():
    conn = get_conn()
    slskd = SlskdClient()

    # pending slskd → start search
    for t in conn.execute(
        "SELECT * FROM tracks WHERE slskd_state='pending' AND (download_source='slskd' OR download_source IS NULL) LIMIT 10"
    ).fetchall():
        meta = TrackMeta(t["artist"] or "", t["album"] or "", t["title"] or "",
                         t["track_number"] or 0, t["source_id"] or "")
        ok, search_id, msg = slskd.start_search(meta)
        if ok:
            conn.execute(
                "UPDATE tracks SET slskd_state='queued', slskd_search_id=?, slskd_error=NULL WHERE id=?",
                (search_id, t["id"]),
            )
        else:
            conn.execute(
                "UPDATE tracks SET slskd_state='failed', slskd_error=? WHERE id=?",
                (msg, t["id"]),
            )
        conn.commit()

    # queued slskd → poll results, auto-download best match
    for t in conn.execute(
        "SELECT * FROM tracks WHERE slskd_state='queued' AND slskd_search_id IS NOT NULL LIMIT 10"
    ).fetchall():
        meta = TrackMeta(t["artist"] or "", t["album"] or "", t["title"] or "",
                         t["track_number"] or 0, t["source_id"] or "")
        results = slskd.get_search_results(t["slskd_search_id"])
        if not results:
            continue  # search not done yet
        scored = sorted(((slskd.score_result(r, meta), r) for r in results), reverse=True)
        if scored and scored[0][0] > 0:
            best = scored[0][1]
            ok, msg = slskd.download_file(best["username"], best["filename"], best.get("size", 0))
            if ok:
                conn.execute("UPDATE tracks SET slskd_state='downloading' WHERE id=?", (t["id"],))
            else:
                conn.execute("UPDATE tracks SET slskd_state='failed', slskd_error=? WHERE id=?", (msg, t["id"]))
        else:
            conn.execute(
                "UPDATE tracks SET slskd_state='failed', slskd_error='No usable files in search results' WHERE id=?",
                (t["id"],),
            )
        conn.commit()

    # downloading → check watch folder, organize
    for t in conn.execute(
        "SELECT * FROM tracks WHERE slskd_state='downloading' LIMIT 20"
    ).fetchall():
        candidate = discover_download_for_track(t)
        if candidate:
            target = Organizer.target_path(t, candidate)
            ok, result = Organizer.move_file(candidate, target)
            if ok:
                conn.execute("UPDATE tracks SET slskd_state='completed', local_path=? WHERE id=?", (result, t["id"]))
            else:
                conn.execute("UPDATE tracks SET slskd_state='completed', slskd_error=? WHERE id=?", (result, t["id"]))
            conn.commit()

    # pending monochrome → lookup TIDAL ID if needed, then download
    mc = MonochromeClient()
    for t in conn.execute(
        "SELECT * FROM tracks WHERE slskd_state='pending' AND download_source='monochrome' LIMIT 5"
    ).fetchall():
        tidal_id = t["source_id"] or ""
        if not tidal_id:
            tidal_id = mc.find_tidal_id(t["artist"] or "", t["title"] or "") or ""
            if tidal_id:
                conn.execute("UPDATE tracks SET source_id=? WHERE id=?", (tidal_id, t["id"]))
                conn.commit()
            else:
                conn.execute(
                    "UPDATE tracks SET slskd_state='failed', slskd_error='Track not found on TIDAL via Monochrome' WHERE id=?",
                    (t["id"],),
                )
                conn.commit()
                continue

        conn.execute("UPDATE tracks SET slskd_state='downloading' WHERE id=?", (t["id"],))
        conn.commit()

        ok, result = mc.download_track(tidal_id, t["artist"] or "", t["title"] or "")
        if ok:
            src = Path(result)
            if src.exists():
                target = Organizer.target_path(t, src)
                move_ok, move_result = Organizer.move_file(src, target)
                final = move_result if move_ok else result
            else:
                final = result
            conn.execute("UPDATE tracks SET slskd_state='completed', local_path=? WHERE id=?", (final, t["id"]))
        else:
            conn.execute("UPDATE tracks SET slskd_state='failed', slskd_error=? WHERE id=?", (result, t["id"]))
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


@app.before_request
def require_login():
    if request.path.startswith("/static") or request.path in ["/login", "/sw.js", "/manifest.json"]:
        return
    if not is_authed():
        return redirect(url_for("login"))


@app.route("/sw.js")
def service_worker():
    return send_from_directory(app.static_folder, "sw.js", mimetype="application/javascript")


@app.route("/manifest.json")
def manifest():
    return send_from_directory(app.static_folder, "manifest.json", mimetype="application/manifest+json")


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

@app.route("/login", methods=["GET", "POST"])
def login():
    default_user = os.getenv("APP_USER", "admin")
    pw_hash = os.getenv("APP_PASSWORD_HASH") or generate_password_hash(os.getenv("APP_PASSWORD", "admin"))
    if request.method == "POST":
        if (request.form.get("username") == default_user
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
        if s in stats:
            stats[s] += 1
        elif s in ("queued",):
            stats["pending"] += 1
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
    ]
    if request.method == "POST":
        for k in keys:
            set_setting(k, request.form.get(k, ""))
        flash("Settings saved", "ok")
        return redirect(url_for("settings"))
    return render_template("settings.html", settings={k: get_setting(k) for k in keys})


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

@app.route("/api/search")
def api_search():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify([])
    return jsonify(_deezer.search(q))


@app.route("/api/tracks")
def api_tracks():
    conn = get_conn()
    rows = [dict(r) for r in conn.execute("SELECT * FROM tracks ORDER BY id DESC LIMIT 100").fetchall()]
    conn.close()
    return jsonify(rows)


@app.route("/api/download", methods=["POST"])
def api_download():
    data = request.get_json(force=True) or {}
    artist = (data.get("artist") or "").strip()
    album = (data.get("album") or "").strip()
    title = (data.get("title") or "").strip()
    source_id = (data.get("source_id") or "").strip()
    cover_url = (data.get("cover") or "").strip()
    dl_source = data.get("source", "slskd")

    if not title or not artist:
        return jsonify({"ok": False, "error": "artist and title are required"}), 400

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
        flash("Unsupported URL. Paste a Spotify, TIDAL, or Apple Music link.", "error")
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

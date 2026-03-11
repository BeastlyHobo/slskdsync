import os
import re
import json
import sqlite3
import threading
import time
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import requests
from flask import Flask, render_template, request, redirect, session, url_for, flash, jsonify
from werkzeug.security import check_password_hash, generate_password_hash
from dotenv import load_dotenv

load_dotenv()

APP_DIR = Path(__file__).resolve().parent
DATA_DIR = APP_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "app.db"


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.executescript(
        """
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
            slskd_state TEXT DEFAULT 'pending',
            slskd_error TEXT,
            local_path TEXT,
            FOREIGN KEY(job_id) REFERENCES import_jobs(id)
        );
        """
    )
    defaults = {
        "library_path": "/music",
        "slskd_url": "http://slskd:5030",
        "slskd_user": "",
        "slskd_pass": "",
        "slskd_api_key": "",
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
        "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )
    conn.commit()
    conn.close()


@dataclass
class TrackMeta:
    artist: str
    album: str
    title: str
    track_number: int = 0
    source_id: str = ""


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
            raise RuntimeError("Spotify credentials missing. Set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET")

        if "/playlist/" in url:
            playlist_id = url.split("/playlist/")[1].split("?")[0]
            data = self.client.playlist_items(playlist_id, additional_types=["track"])
            tracks = []
            for item in data.get("items", []):
                t = item.get("track") or {}
                album = (t.get("album") or {}).get("name") or "Unknown Album"
                artist = ", ".join(a["name"] for a in t.get("artists", [])) or "Unknown Artist"
                tracks.append(
                    TrackMeta(
                        artist=artist,
                        album=album,
                        title=t.get("name") or "Unknown Title",
                        track_number=t.get("track_number") or 0,
                        source_id=t.get("id") or "",
                    )
                )
            return "playlist", tracks

        if "/album/" in url:
            album_id = url.split("/album/")[1].split("?")[0]
            album = self.client.album(album_id)
            album_name = album.get("name") or "Unknown Album"
            tracks = []
            for t in album.get("tracks", {}).get("items", []):
                artist = ", ".join(a["name"] for a in t.get("artists", [])) or "Unknown Artist"
                tracks.append(
                    TrackMeta(
                        artist=artist,
                        album=album_name,
                        title=t.get("name") or "Unknown Title",
                        track_number=t.get("track_number") or 0,
                        source_id=t.get("id") or "",
                    )
                )
            return "album", tracks

        if "/track/" in url:
            track_id = url.split("/track/")[1].split("?")[0]
            t = self.client.track(track_id)
            album = (t.get("album") or {}).get("name") or "Unknown Album"
            artist = ", ".join(a["name"] for a in t.get("artists", [])) or "Unknown Artist"
            return "track", [TrackMeta(artist=artist, album=album, title=t.get("name") or "Unknown Title", track_number=t.get("track_number") or 0, source_id=t.get("id") or "")]

        if "/artist/" in url:
            artist_id = url.split("/artist/")[1].split("?")[0]
            top = self.client.artist_top_tracks(artist_id)
            tracks = []
            for t in top.get("tracks", []):
                album = (t.get("album") or {}).get("name") or "Unknown Album"
                artist = ", ".join(a["name"] for a in t.get("artists", [])) or "Unknown Artist"
                tracks.append(TrackMeta(artist=artist, album=album, title=t.get("name") or "Unknown Title", track_number=t.get("track_number") or 0, source_id=t.get("id") or ""))
            return "artist", tracks

        raise RuntimeError("Unsupported Spotify URL type")


class AppleProvider:
    name = "apple"

    def supports(self, url: str) -> bool:
        return "music.apple.com" in url

    def parse(self, url: str) -> tuple[str, list[TrackMeta]]:
        # Lightweight fallback via iTunes lookup for pasted links containing ?i=<songId>
        song_match = re.search(r"[?&]i=(\d+)", url)
        if song_match:
            sid = song_match.group(1)
            data = requests.get("https://itunes.apple.com/lookup", params={"id": sid}, timeout=20).json()
            result = (data.get("results") or [{}])[0]
            return "track", [
                TrackMeta(
                    artist=result.get("artistName", "Unknown Artist"),
                    album=result.get("collectionName", "Unknown Album"),
                    title=result.get("trackName", "Unknown Title"),
                    track_number=result.get("trackNumber", 0),
                    source_id=str(result.get("trackId", "")),
                )
            ]

        # Album/playlist parsing requires Apple Music API token; keep extension point explicit.
        raise RuntimeError("Apple Music album/playlist parsing requires Apple Music API token support (not configured yet).")


class SlskdClient:
    def __init__(self):
        self.base = get_setting("slskd_url").rstrip("/")
        self.user = get_setting("slskd_user")
        self.password = get_setting("slskd_pass")
        self.api_key = get_setting("slskd_api_key")

    def _headers(self):
        h = {"Content-Type": "application/json"}
        if self.api_key:
            h["X-API-Key"] = self.api_key
        return h

    def queue_track(self, track: TrackMeta) -> tuple[bool, str]:
        query = f"{track.artist} {track.title}"
        payload = {"query": query}
        # Fallback endpoints for slskd versions.
        endpoints = ["/api/v0/searches", "/api/v1/searches"]
        for ep in endpoints:
            try:
                r = requests.post(
                    f"{self.base}{ep}",
                    headers=self._headers(),
                    auth=(self.user, self.password) if self.user else None,
                    data=json.dumps(payload),
                    timeout=25,
                )
                if r.status_code < 300:
                    return True, "search queued"
            except Exception as ex:
                err = str(ex)
        return False, f"unable to queue on slskd endpoints ({err if 'err' in locals() else 'unknown error'})"


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
    def move_file(src: Path, dst: Path):
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
        if not f.is_file():
            continue
        n = f.name.lower()
        if title and title in n and artist and artist in n:
            return f
    return None


def run_worker(stop_event: threading.Event):
    while not stop_event.is_set():
        conn = get_conn()
        pending = conn.execute(
            "SELECT * FROM tracks WHERE slskd_state IN ('pending','queued','downloading') ORDER BY id LIMIT 20"
        ).fetchall()
        slskd = SlskdClient()
        for t in pending:
            if t["slskd_state"] == "pending":
                ok, msg = slskd.queue_track(TrackMeta(t["artist"], t["album"], t["title"], t["track_number"], t["source_id"] or ""))
                if ok:
                    conn.execute("UPDATE tracks SET slskd_state='queued', slskd_error=NULL WHERE id=?", (t["id"],))
                else:
                    conn.execute("UPDATE tracks SET slskd_state='failed', slskd_error=? WHERE id=?", (msg, t["id"]))
                conn.commit()
                continue

            candidate = discover_download_for_track(t)
            if candidate:
                target = Organizer.target_path(t, candidate)
                ok, result = Organizer.move_file(candidate, target)
                if ok:
                    conn.execute("UPDATE tracks SET slskd_state='completed', local_path=? WHERE id=?", (result, t["id"]))
                else:
                    conn.execute("UPDATE tracks SET slskd_state='completed', slskd_error=? WHERE id=?", (result, t["id"]))
                conn.commit()

        conn.close()
        time.sleep(20)


app = Flask(__name__)
app.secret_key = os.getenv("APP_SECRET", "change-me")

init_db()

stop_event = threading.Event()
worker = threading.Thread(target=run_worker, args=(stop_event,), daemon=True)
worker.start()


def is_authed() -> bool:
    return session.get("authed") is True


@app.before_request
def require_login():
    if request.path.startswith("/static") or request.path in ["/login"]:
        return
    if not is_authed():
        return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    default_user = os.getenv("APP_USER", "admin")
    pw_hash = os.getenv("APP_PASSWORD_HASH")
    if not pw_hash:
        pw_hash = generate_password_hash(os.getenv("APP_PASSWORD", "admin"))

    if request.method == "POST":
        user = request.form.get("username", "")
        password = request.form.get("password", "")
        if user == default_user and check_password_hash(pw_hash, password):
            session["authed"] = True
            return redirect(url_for("index"))
        flash("Invalid credentials", "error")
    return render_template("login.html")


@app.route("/")
def index():
    conn = get_conn()
    jobs = conn.execute("SELECT * FROM import_jobs ORDER BY created_at DESC LIMIT 30").fetchall()
    tracks = conn.execute("SELECT * FROM tracks ORDER BY id DESC LIMIT 200").fetchall()
    conn.close()
    return render_template("index.html", jobs=jobs, tracks=tracks)


providers = [SpotifyProvider(), AppleProvider()]


@app.route("/import", methods=["POST"])
def import_url():
    url = request.form.get("url", "").strip()
    create_nav_playlist = 1 if request.form.get("create_nav_playlist") else 0
    if not url:
        flash("URL is required", "error")
        return redirect(url_for("index"))

    provider = next((p for p in providers if p.supports(url)), None)
    if not provider:
        flash("Unsupported URL", "error")
        return redirect(url_for("index"))

    try:
        source_type, tracks = provider.parse(url)
    except Exception as ex:
        flash(str(ex), "error")
        return redirect(url_for("index"))

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO import_jobs(source,source_type,source_url,nav_playlist,status) VALUES (?,?,?,?,?)",
        (provider.name, source_type, url, create_nav_playlist, "queued"),
    )
    job_id = cur.lastrowid
    for t in tracks:
        cur.execute(
            "INSERT INTO tracks(job_id,artist,album,title,track_number,source_id) VALUES(?,?,?,?,?,?)",
            (job_id, t.artist, t.album, t.title, t.track_number, t.source_id),
        )
    conn.commit()
    conn.close()

    flash(f"Imported {len(tracks)} tracks from {provider.name}", "ok")
    return redirect(url_for("index"))


@app.route("/settings", methods=["GET", "POST"])
def settings():
    keys = [
        "library_path",
        "slskd_url",
        "slskd_user",
        "slskd_pass",
        "slskd_api_key",
        "navidrome_url",
        "navidrome_user",
        "navidrome_pass",
        "quality",
        "replace_existing",
        "folder_template",
        "download_watch_path",
    ]
    if request.method == "POST":
        for k in keys:
            set_setting(k, request.form.get(k, ""))
        flash("Settings updated", "ok")
        return redirect(url_for("settings"))

    data = {k: get_setting(k) for k in keys}
    return render_template("settings.html", settings=data)


@app.route("/api/tracks")
def api_tracks():
    conn = get_conn()
    rows = [dict(r) for r in conn.execute("SELECT * FROM tracks ORDER BY id DESC LIMIT 200").fetchall()]
    conn.close()
    return jsonify(rows)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)

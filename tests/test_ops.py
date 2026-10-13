"""Regressions for backups, concurrency guards, health and API error shape."""
import sqlite3
import threading

import pytest


# --- backups ---------------------------------------------------------------

def test_backup_is_restorable(A):
    conn = A.get_conn()
    conn.execute("INSERT INTO settings(key,value) VALUES('acoustid_api_key','SECRET')")
    conn.commit()
    conn.close()

    dest = A.backup_db()
    assert dest.exists()

    b = sqlite3.connect(dest)
    assert b.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    assert b.execute("SELECT value FROM settings WHERE key='acoustid_api_key'"
                     ).fetchone()[0] == "SECRET"
    b.close()


def test_backup_excludes_uncommitted_writes(A, job):
    w = A.get_conn()
    w.execute("INSERT INTO tracks(job_id,artist,title,slskd_state)"
              " VALUES(?,'x','uncommitted','pending')", (job,))
    dest = A.backup_db()          # w's transaction is still open
    b = sqlite3.connect(dest)
    assert b.execute("SELECT COUNT(*) FROM tracks WHERE title='uncommitted'"
                     ).fetchone()[0] == 0
    b.close()
    w.rollback()
    w.close()


def test_back_to_back_backups_both_succeed(A):
    """VACUUM INTO refuses to overwrite and the name is second-granular, so
    two runs in the same second used to fail -- and the timestamp is stamped
    before the attempt, meaning no backup for 24h."""
    assert A.backup_db().exists()
    assert A.backup_db().exists()


def test_backup_retention_keeps_newest(A, monkeypatch):
    from datetime import datetime, timedelta

    class FakeDT(datetime):
        _n = 0
        @classmethod
        def utcnow(cls):
            cls._n += 1
            return datetime(2026, 1, 1) + timedelta(days=cls._n)

    monkeypatch.setattr(A, "datetime", FakeDT)
    for _ in range(12):
        A.backup_db()
    names = sorted(p.name for p in A.BACKUP_DIR.glob("app-*.db"))
    assert len(names) == A.BACKUP_KEEP
    assert names[-1] == "app-20260113-000000.db"
    assert not any(n.startswith("app-20260101") for n in names)


# --- concurrency guards ----------------------------------------------------

def test_scan_endpoint_reports_an_in_flight_scan(A, auth):
    with A._scan_state_lock:
        A._scan_state["in_progress"] = True
    try:
        r = auth.post("/api/library/scan")
        assert r.get_json()["message"] == "Library scan already running"
    finally:
        with A._scan_state_lock:
            A._scan_state["in_progress"] = False


def test_scan_library_refuses_to_run_twice(A):
    """The compare-and-set lives in scan_library so it covers every caller."""
    with A._scan_state_lock:
        A._scan_state["in_progress"] = True
    try:
        A.scan_library()      # must return immediately, not start a scan
    finally:
        with A._scan_state_lock:
            A._scan_state["in_progress"] = False


def test_scan_always_clears_its_flag(A, monkeypatch):
    """A flag left set would block every future scan for the process lifetime."""
    monkeypatch.setattr(A, "get_setting",
                        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    A.scan_library()
    assert A._scan_state["in_progress"] is False


@pytest.mark.parametrize("payload,status", [
    ({"scope": "all"}, 404),      # empty library_index
    ({"scope": "track"}, 400),    # missing id
])
def test_acoustid_releases_slot_on_every_early_return(A, auth, payload, status):
    r = auth.post("/api/library/acoustid", json=payload)
    assert r.status_code == status
    assert A._lib_acoustid_state["in_progress"] is False


# --- health + API error shape ---------------------------------------------

def test_health_needs_no_auth(A, client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.get_json()["ok"] is True


def test_api_errors_are_json(A, auth):
    r = auth.get("/api/no-such-route")
    assert r.status_code == 404
    assert r.headers["Content-Type"].startswith("application/json")


def test_non_api_errors_stay_html(A, auth):
    r = auth.get("/no-such-page")
    assert not r.headers.get("Content-Type", "").startswith("application/json")


@pytest.mark.parametrize("n", ["abc", "-5", "999999", "", "0"])
def test_api_logs_tolerates_hostile_n(A, auth, n):
    assert auth.get(f"/api/logs?n={n}").status_code == 200


def test_session_cookie_is_hardened(A, client):
    r = client.post("/login", data={"username": "testuser",
                                    "password": "test-password-123"})
    cookies = r.headers.getlist("Set-Cookie")
    assert any("SameSite=Lax" in c for c in cookies)
    assert any("HttpOnly" in c for c in cookies)


# --- worker loop resilience ------------------------------------------------

def test_worker_survives_a_failing_scheduled_step(A, monkeypatch):
    """Only _worker_tick used to be guarded, so an exception from any
    _maybe_* killed the worker thread and stopped all downloads."""
    monkeypatch.setattr(A, "_worker_tick", lambda: None)
    # Throw from inside the loop's own scheduled step, which is what used to
    # escape and kill the thread (get_setting is the first call _maybe_scan
    # makes, and a transient "database is locked" there is realistic).
    monkeypatch.setattr(A, "get_setting",
                        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("database is locked")))
    monkeypatch.setattr(A.time, "sleep", lambda s: stop.set())

    stop = threading.Event()
    t = threading.Thread(target=A.run_worker, args=(stop,), daemon=True)
    t.start()
    t.join(timeout=10)
    assert not t.is_alive(), "worker thread died instead of logging and continuing"


def test_acoustid_ids_scope_verifies_only_the_given_rows(A, auth, monkeypatch):
    """A filtered Library view verifies what it shows, not the whole library."""
    conn = A.get_conn()
    for i in (1, 2, 3):
        conn.execute("INSERT INTO library_index(id, artist, title, path) VALUES (?,?,?,?)",
                     (i, "A", f"T{i}", f"/music/A/T{i}.mp3"))
    conn.commit()
    conn.close()
    got = {}
    A.set_setting("acoustid_api_key", "k")
    monkeypatch.setattr(A, "_run_lib_acoustid", lambda ids, table: got.update(ids=ids, table=table))
    r = auth.post("/api/library/acoustid", json={"scope": "ids", "ids": [1, 3, 99, "2"]})
    assert r.get_json() == {"ok": True, "total": 2}
    assert sorted(got["ids"]) == [1, 3] and got["table"] == "library_index"


def _lib_rows(A):
    conn = A.get_conn()
    rows = {r["path"]: dict(r) for r in conn.execute("SELECT * FROM library_index")}
    conn.close()
    return rows


def test_rescan_keeps_ids_dates_and_verdicts_by_path(A, tmp_path):
    root = tmp_path / "music"
    for name in ("a", "b"):
        (root / "Art" / "Alb").mkdir(parents=True, exist_ok=True)
        (root / "Art" / "Alb" / f"{name}.mp3").write_bytes(b"x")
    A.set_setting("library_path", str(root))
    A.set_setting("navidrome_user", "")
    A.scan_library()
    before = _lib_rows(A)
    a = str(root / "Art" / "Alb" / "a.mp3")
    conn = A.get_conn()
    conn.execute("UPDATE library_index SET acoustid_score=0.9, indexed_at='2020-01-01 00:00:00'"
                 " WHERE path=?", (a,))
    conn.commit(); conn.close()
    (root / "Art" / "Alb" / "c.mp3").write_bytes(b"x")
    A.scan_library()
    after = _lib_rows(A)
    assert after[a]["id"] == before[a]["id"]
    assert after[a]["acoustid_score"] == 0.9
    # filesystem scans date a file by its mtime, which beats the stored date
    assert after[a]["indexed_at"] != "2020-01-01 00:00:00"
    assert len(after) == 3 and len({r["id"] for r in after.values()}) == 3


def test_a_scan_that_cannot_read_the_library_keeps_the_index(A, tmp_path):
    conn = A.get_conn()
    conn.execute("INSERT INTO library_index(artist,title,path,acoustid_score)"
                 " VALUES('A','T','/m/t.mp3',0.9)")
    conn.commit(); conn.close()
    A.set_setting("library_path", str(tmp_path / "not-mounted"))
    A.set_setting("navidrome_user", "")
    A.scan_library()
    assert list(_lib_rows(A)) == ["/m/t.mp3"]
    assert A._scan_state["in_progress"] is False


def test_finished_download_is_indexed_and_clears_stale_state(A, job):
    conn = A.get_conn()
    conn.execute("INSERT INTO tracks(job_id,artist,album,title) VALUES(?,'Art','Alb','Song')", (job,))
    conn.execute("INSERT INTO bad_flags(path) VALUES('/m/s.flac')")
    conn.execute("INSERT INTO library_index(artist,title,path,acoustid_score) VALUES('X','Y','/m/s.flac',0)")
    t = conn.execute("SELECT * FROM tracks").fetchone()
    A._index_download(conn, t, "/m/s.flac", (0.95, "Song", "Art"))
    conn.commit()
    rows = conn.execute("SELECT * FROM library_index").fetchall()
    assert [(r["artist"], r["title"], r["acoustid_score"]) for r in rows] == [("Art", "Song", 0.95)]
    assert conn.execute("SELECT COUNT(*) FROM bad_flags").fetchone()[0] == 0
    conn.close()


def test_ai_suggest_sends_anthropic_structured_output_shape(A, auth, job, monkeypatch):
    """format must be {"type": "json_schema", "schema": {...}} — the nested
    OpenAI-style {"json_schema": {"name", "schema"}} made every call a 400."""
    pytest.importorskip("anthropic")
    from anthropic.types import JSONOutputFormatParam
    sent = {}

    class FakeMessages:
        def create(self, **kw):
            sent.update(kw)
            block = type("B", (), {"type": "text", "text": '{"queries": [{"q": "a b", "why": "x"}]}'})
            return type("R", (), {"content": [block]})

    class FakeClient:
        def __init__(self, **kw):
            self.messages = FakeMessages()

    monkeypatch.setattr(A._anthropic, "Anthropic", FakeClient)
    A.set_setting("anthropic_api_key", "k")
    conn = A.get_conn()
    conn.execute("INSERT INTO tracks(job_id,artist,title) VALUES(?,'A','T')", (job,))
    conn.commit(); conn.close()
    r = auth.post("/api/tracks/1/ai-suggest").get_json()
    assert r["ok"] and r["suggestions"][0]["q"] == "a b"
    fmt = sent["output_config"]["format"]
    assert set(fmt) == set(JSONOutputFormatParam.__annotations__)
    assert fmt["schema"]["required"] == ["queries"]


def test_acoustid_without_a_key_says_so(A, auth):
    conn = A.get_conn()
    conn.execute("INSERT INTO library_index(id, artist, title, path) VALUES (1,'A','T','/m/t.mp3')")
    conn.commit(); conn.close()
    r = auth.post("/api/library/acoustid", json={"scope": "track", "id": 1})
    assert r.status_code == 400 and "AcoustID API key" in r.get_json()["error"]
    assert A._lib_acoustid_state["in_progress"] is False


def test_library_tracks_are_compact_and_revalidated(A, auth):
    conn = A.get_conn()
    conn.execute("INSERT INTO library_index(id,artist,title,album,path,cover_art_id,acoustid_score)"
                 " VALUES (1,'A','T','Al','/m/a.flac','al-1',0.9)")
    conn.execute("INSERT INTO library_index(id,artist,title,album,path) VALUES (2,'B','U','','/m/b.mp3')")
    conn.execute("INSERT INTO bad_flags(path) VALUES('/m/b.mp3')")
    conn.commit(); conn.close()
    r = auth.get("/api/library/tracks")
    rows = {t["id"]: t for t in r.get_json()}
    assert rows[1]["cover"] == "al-1" and rows[1]["acoustid_score"] == 0.9
    assert "acoustid_score" not in rows[2] and rows[2]["flagged_bad"] is True
    etag = r.headers["ETag"]
    again = auth.get("/api/library/tracks", headers={"If-None-Match": etag})
    assert again.status_code == 304 and not again.data
    conn = A.get_conn()
    conn.execute("UPDATE library_index SET title='T2' WHERE id=1")
    conn.commit(); conn.close()
    assert auth.get("/api/library/tracks", headers={"If-None-Match": etag}).status_code == 200


def test_acoustid_scores_can_be_narrowed_to_ids(A, auth):
    conn = A.get_conn()
    for i in (1, 2, 3):
        conn.execute("INSERT INTO library_index(id,artist,title,path,acoustid_score) VALUES (?,?,?,?,0.5)",
                     (i, "A", f"T{i}", f"/m/{i}.mp3"))
    conn.commit(); conn.close()
    assert set(auth.get("/api/library/acoustid/scores?ids=1,3").get_json()) == {"1", "3"}
    assert len(auth.get("/api/library/acoustid/scores").get_json()) == 3


def test_cover_proxy_caches_hits_and_failures(A, auth, monkeypatch):
    A.set_setting("navidrome_url", "http://nd")
    A.set_setting("navidrome_user", "u")
    calls = []

    class R:
        def __init__(self, status, ctype, body):
            self.status_code, self.headers, self.content = status, {"content-type": ctype}, body

    def fake_get(url, params, timeout):
        calls.append(params["id"])
        return R(200, "image/jpeg", b"JPEG") if params["id"] == "good" else R(404, "text/xml", b"")

    monkeypatch.setattr(A.requests, "get", fake_get)
    A._cover_fail.clear()
    for _ in range(2):
        r = auth.get("/api/library/cover/good")
        assert r.status_code == 200 and r.data == b"JPEG"
        r.close()
        assert auth.get("/api/library/cover/bad").status_code == 404
    # one Navidrome call each: the hit came from disk, the miss from memory
    assert calls == ["good", "bad"]

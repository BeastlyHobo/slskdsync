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

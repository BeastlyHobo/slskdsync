"""Test fixtures for slskdsync.

Importing app.app starts the background worker and creates the DB, so every
test must run against a throwaway DATA_DIR. We point the module globals at a
tmp_path before init_db() and stop the worker so it can't race the tests.
"""
import os
import sys
import pathlib
import sqlite3

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "app"))

# Must be set before the module is imported: get_auth_credentials() reads them.
os.environ.setdefault("APP_USER", "testuser")
os.environ.setdefault("APP_PASSWORD", "test-password-123")


@pytest.fixture(scope="session")
def appmod():
    import app as A
    # The worker thread would run scans/backups against the tests' DB.
    A._stop_event.set()
    return A


@pytest.fixture()
def A(appmod, tmp_path):
    """The app module rebound to an isolated DB for a single test."""
    appmod.DATA_DIR = tmp_path
    appmod.DB_PATH = tmp_path / "app.db"
    appmod.BACKUP_DIR = tmp_path / "backups"
    with appmod._settings_cache_lock:
        appmod._settings_cache.clear()
    appmod.init_db()
    with appmod._scan_state_lock:
        appmod._scan_state.update(in_progress=False)
    with appmod._lib_acoustid_lock:
        appmod._lib_acoustid_state.update(in_progress=False)
    return appmod


@pytest.fixture()
def client(A):
    A.app.config["TESTING"] = False  # exercise the real error handlers
    return A.app.test_client()


@pytest.fixture()
def auth(client):
    client.post("/login", data={"username": "testuser", "password": "test-password-123"})
    return client


@pytest.fixture()
def job(A):
    """An import_jobs row, since tracks.job_id is NOT NULL."""
    conn = A.get_conn()
    conn.execute(
        "INSERT INTO import_jobs(source,source_type,source_url,status)"
        " VALUES('deezer','playlist','http://example/pl','done')"
    )
    conn.commit()
    jid = conn.execute("SELECT id FROM import_jobs").fetchone()[0]
    conn.close()
    return jid

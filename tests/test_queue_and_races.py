"""Regressions for the queue actions and the worker/request races."""
import sqlite3

import pytest

GUARD = (" WHERE id=? AND slskd_state='pending'"
         " AND IFNULL(custom_search,'')=? AND IFNULL(slskd_search_attempt,0)=?")


def seed(A, job, **states):
    conn = A.get_conn()
    for state, n in states.items():
        for i in range(n):
            conn.execute(
                "INSERT INTO tracks(job_id,artist,title,slskd_state) VALUES(?,?,?,?)",
                (job, "artist", f"{state}-{i}", state))
    conn.commit()
    conn.close()


def count(A, where=""):
    conn = A.get_conn()
    n = conn.execute(f"SELECT COUNT(*) FROM tracks {where}").fetchone()[0]
    conn.close()
    return n


def test_clear_all_removes_every_track(A, auth, job):
    """It used to delete only failed+completed, so confirming the
    "Remove ALL tracks" prompt with a live queue silently did nothing."""
    seed(A, job, pending=2, completed=1, failed=1, downloading=1)
    assert count(A) == 5
    r = auth.post("/api/queue/action", json={"action": "clear_all"})
    assert r.status_code == 200
    assert count(A) == 0


def test_clear_done_still_only_removes_completed(A, auth, job):
    seed(A, job, pending=2, completed=3, failed=1)
    auth.post("/api/queue/action", json={"action": "clear_completed"})
    assert count(A, "WHERE slskd_state='completed'") == 0
    assert count(A, "WHERE slskd_state='pending'") == 2
    assert count(A, "WHERE slskd_state='failed'") == 1


def test_unknown_action_is_json_400(A, auth):
    r = auth.post("/api/queue/action", json={"action": "nope"})
    assert r.status_code == 400
    assert r.headers["Content-Type"].startswith("application/json")


@pytest.mark.parametrize("kwargs", [
    {"json": [1, 2, 3]},                                       # array body
    {"data": "{bad", "content_type": "application/json"},      # malformed
    {"data": "x", "content_type": "text/plain"},               # no JSON header
])
def test_bad_json_bodies_do_not_crash(A, auth, kwargs):
    """These previously raised 415 / AttributeError instead of answering."""
    r = auth.post("/api/queue/action", **kwargs)
    assert r.status_code == 400
    assert r.headers["Content-Type"].startswith("application/json")


# --- the worker/retry race -------------------------------------------------
# Modelled directly on the real UPDATE, since driving the worker would need a
# live slskd. The guard is what the fix added; these lock in its semantics.

def _mkdb():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.execute("CREATE TABLE tracks(id INTEGER PRIMARY KEY, slskd_state TEXT,"
              " custom_search TEXT, slskd_search_attempt INT DEFAULT 0,"
              " slskd_search_id TEXT)")
    return c


def _writeback(c, tid, sid, raw, attempt):
    return c.execute("UPDATE tracks SET slskd_state='queued', slskd_search_id=?,"
                     " custom_search=NULL" + GUARD, (sid, tid, raw, attempt)).rowcount


def test_retry_during_search_is_not_discarded():
    c = _mkdb()
    c.execute("INSERT INTO tracks(id,slskd_state,custom_search) VALUES(1,'pending',NULL)")
    t = c.execute("SELECT * FROM tracks").fetchone()
    raw, attempt = t["custom_search"] or "", t["slskd_search_attempt"] or 0
    # user hits retry with a custom query while start_search is in flight
    c.execute("UPDATE tracks SET slskd_state='pending', slskd_search_attempt=0,"
              " custom_search=? WHERE id=1", ("beatles taxman flac",))
    assert _writeback(c, 1, "search-abc", raw, attempt) == 0
    row = c.execute("SELECT * FROM tracks WHERE id=1").fetchone()
    assert row["custom_search"] == "beatles taxman flac"
    assert row["slskd_state"] == "pending"      # next tick uses their query


def test_uncontended_writeback_still_applies():
    c = _mkdb()
    c.execute("INSERT INTO tracks(id,slskd_state,custom_search) VALUES(1,'pending',NULL)")
    t = c.execute("SELECT * FROM tracks").fetchone()
    assert _writeback(c, 1, "search-xyz", t["custom_search"] or "",
                      t["slskd_search_attempt"] or 0) == 1
    row = c.execute("SELECT * FROM tracks WHERE id=1").fetchone()
    assert (row["slskd_state"], row["slskd_search_id"]) == ("queued", "search-xyz")
    assert row["custom_search"] is None


def test_untrimmed_custom_search_still_progresses():
    """The guard must compare the raw column; comparing the stripped value
    would never match a stored '  query  ' and would re-search forever."""
    c = _mkdb()
    c.execute("INSERT INTO tracks(id,slskd_state,custom_search)"
              " VALUES(1,'pending','  spaced query  ')")
    t = c.execute("SELECT * FROM tracks").fetchone()
    assert _writeback(c, 1, "s", t["custom_search"] or "",
                      t["slskd_search_attempt"] or 0) == 1
    assert c.execute("SELECT slskd_state FROM tracks").fetchone()[0] == "queued"


# --- retry semantics -------------------------------------------------------
# Retry used to clear slskd_tried_users, so the scorer re-picked the same peer
# and the same file; and it left force_overwrite unset, so a re-download of a
# track already in the library was organized, found the destination occupied,
# and was deleted. These lock in both fixes.

def _mktrack(A, job, **cols):
    conn = A.get_conn()
    keys = ["job_id", "artist", "title", "slskd_state"] + list(cols)
    vals = [job, "artist", "title", "failed"] + list(cols.values())
    conn.execute(f"INSERT INTO tracks({','.join(keys)})"
                 f" VALUES({','.join('?' * len(keys))})", vals)
    conn.commit()
    tid = conn.execute("SELECT id FROM tracks ORDER BY id DESC LIMIT 1").fetchone()[0]
    conn.close()
    return tid


def _row(A, tid):
    conn = A.get_conn()
    r = conn.execute("SELECT * FROM tracks WHERE id=?", (tid,)).fetchone()
    conn.close()
    return r


def test_retry_keeps_tried_peers(A, auth, job):
    """The whole point: retry must not hand the scorer the same peer again."""
    tid = _mktrack(A, job, slskd_tried_users="alice,bob")
    assert auth.post(f"/api/tracks/{tid}/retry").status_code == 200
    r = _row(A, tid)
    assert r["slskd_tried_users"] == "alice,bob"
    assert r["slskd_state"] == "pending"


def test_start_over_clears_tried_peers(A, auth, job):
    tid = _mktrack(A, job, slskd_tried_users="alice,bob")
    auth.post(f"/api/tracks/{tid}/retry", json={"reset_peers": True})
    assert _row(A, tid)["slskd_tried_users"] == ""


def test_retry_with_query_still_keeps_peers(A, auth, job):
    """A new search query is not a reason to re-offer a peer that failed."""
    tid = _mktrack(A, job, slskd_tried_users="alice")
    auth.post(f"/api/tracks/{tid}/retry", json={"query": "beatles taxman flac"})
    r = _row(A, tid)
    assert r["slskd_tried_users"] == "alice"
    assert r["custom_search"] == "beatles taxman flac"


def test_retry_of_library_track_forces_replace(A, auth, job):
    """Already in the library => the re-download must overwrite it, and record
    the old path so a format change doesn't leave both files behind."""
    tid = _mktrack(A, job, slskd_state="completed",
                   local_path="/music/Artist/Album/01 - Title.mp3")
    auth.post(f"/api/tracks/{tid}/retry")
    r = _row(A, tid)
    assert r["force_overwrite"] == 1
    assert r["replace_path"] == "/music/Artist/Album/01 - Title.mp3"


def test_retry_of_never_downloaded_track_does_not_force_replace(A, auth, job):
    tid = _mktrack(A, job)
    auth.post(f"/api/tracks/{tid}/retry")
    r = _row(A, tid)
    assert r["force_overwrite"] == 0
    assert r["replace_path"] is None

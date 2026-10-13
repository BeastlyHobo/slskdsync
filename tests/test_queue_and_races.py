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


def test_scorer_prefers_the_right_song_over_a_better_format(appmod):
    """A FLAC of the wrong song (or the same title by someone else) used to
    outscore the right song as an MP3."""
    A = appmod
    c = A.SlskdClient()
    m = A.TrackMeta("Radiohead", "Pablo Honey", "Creep", 2)
    right = c.score_result({"filename": r"Radiohead\Pablo Honey\02 - Creep.mp3", "bitRate": 320}, m)
    other_song = c.score_result({"filename": r"Radiohead - Creep EP\03 - Inside My Pocket.flac"}, m)
    other_artist = c.score_result({"filename": r"Stone Temple Pilots\Core\06 - Creep.flac"}, m)
    assert other_song < 0
    assert right > other_artist > 0
    # punctuation and accents don't fake a title/artist miss
    m2 = A.TrackMeta("Björk", "", "Don't Be Sad", 1)
    assert c.score_result({"filename": r"Bjork\01 - Dont Be Sad.flac"}, m2) > 100


def test_scorer_uses_song_length_when_known(appmod):
    A = appmod
    c = A.SlskdClient()
    m = A.TrackMeta("Radiohead", "OK Computer", "Paranoid Android", 2, duration=383)
    studio = {"filename": r"Radiohead\OK Computer\02 - Paranoid Android.mp3", "bitRate": 320, "length": 384}
    live = {"filename": r"Radiohead\Bootlegs\02 - Paranoid Android.flac", "length": 452}
    assert c.score_result(studio, m) > c.score_result(live, m)
    # unknown length on either side changes nothing
    assert c.score_result({**live, "length": 0}, m) == c.score_result(
        live, A.TrackMeta("Radiohead", "OK Computer", "Paranoid Android", 2))


def test_download_stores_the_song_length(A, auth):
    auth.post("/api/download", json={"artist": "A", "title": "T", "duration": "238"})
    auth.post("/api/download", json={"artist": "B", "title": "U", "duration": "nonsense"})
    conn = A.get_conn()
    got = {r["artist"]: r["duration"] for r in conn.execute("SELECT artist, duration FROM tracks")}
    conn.close()
    assert got == {"A": 238, "B": 0}


def test_watch_folder_prefers_the_file_that_was_requested(A, job, tmp_path):
    """Two same-titled files in the watch folder: the one slskd was asked for
    wins, not whichever the title match finds first."""
    watch = tmp_path / "dl"
    (watch / "Other Album").mkdir(parents=True)
    (watch / "Pablo Honey").mkdir()
    other = watch / "Other Album" / "02 - Creep.flac"
    mine = watch / "Pablo Honey" / "02 - Creep.flac"
    for f in (other, mine):
        f.write_bytes(b"x")
    A.set_setting("download_watch_path", str(watch))
    conn = A.get_conn()
    conn.execute("INSERT INTO tracks(job_id,artist,title,slskd_state,slskd_download_filename)"
                 " VALUES(?,?,?,?,?)", (job, "Radiohead", "Creep", "downloading",
                                        r"@@music\Radiohead\Pablo Honey\02 - Creep.flac"))
    t = conn.execute("SELECT * FROM tracks").fetchone()
    conn.close()
    assert A.discover_download_for_track(t, [other, mine]) == mine


def test_transfer_files_reads_slskds_per_user_shape(appmod):
    data = {"username": "peer", "directories": [
        {"directory": "Music\\Album", "files": [
            {"id": "abc", "filename": "Music\\Album\\01 - A.flac", "state": "Queued, Remotely"}]}]}
    files = appmod.SlskdClient._transfer_files(data)
    assert [(f["id"], f["filename"]) for f in files] == [("abc", "Music\\Album\\01 - A.flac")]
    # the flat list shape the old parser handled still works
    assert len(appmod.SlskdClient._transfer_files([{"files": files}])) == 1


def test_deleting_a_downloading_track_cancels_its_transfer(A, auth, job, monkeypatch):
    cancelled = []
    monkeypatch.setattr(A.SlskdClient, "cancel_download",
                        lambda self, u, f: cancelled.append((u, f)) or True)
    monkeypatch.setattr(A.SlskdClient, "cancel_search", lambda self, sid: None)
    conn = A.get_conn()
    conn.execute("INSERT INTO tracks(job_id,artist,title,slskd_state,slskd_download_user,"
                 "slskd_download_filename) VALUES(?,?,?,?,?,?)",
                 (job, "A", "T", "downloading", "peer", "x\\T.flac"))
    conn.commit(); conn.close()
    auth.delete("/api/tracks/1")
    assert cancelled == [("peer", "x\\T.flac")]


def test_new_playlist_tracks_join_the_playlist_job(A, auth, job):
    """'New?' → '+ Queue' used to make separate jobs the playlist's M3U never saw."""
    r = auth.post("/api/download/batch", json={
        "source": "slskd", "playlist_job_id": job,
        "tracks": [{"artist": "A", "title": "New Song", "track_number": 7, "duration": 200}]})
    assert r.get_json()["queued"] == 1
    conn = A.get_conn()
    t = conn.execute("SELECT job_id, track_number, duration FROM tracks").fetchone()
    jobs = conn.execute("SELECT COUNT(*) FROM import_jobs").fetchone()[0]
    conn.close()
    assert (t["job_id"], t["track_number"], t["duration"], jobs) == (job, 7, 200, 1)
    # without it, still one track-type job per song
    auth.post("/api/download/batch", json={"tracks": [{"artist": "B", "title": "Other"}]})
    conn = A.get_conn()
    assert conn.execute("SELECT COUNT(*) FROM import_jobs").fetchone()[0] == 2
    conn.close()
    assert auth.post("/api/download/batch", json={
        "playlist_job_id": 999, "tracks": [{"artist": "C", "title": "X"}]}).status_code == 404

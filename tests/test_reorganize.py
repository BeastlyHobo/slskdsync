"""The keep-and-correct path: re-file a wrong grab instead of deleting it."""
import pathlib

import pytest

pytest.importorskip("mutagen")


@pytest.fixture()
def lib(A, tmp_path):
    """A library root with one mis-named file already indexed."""
    root = tmp_path / "music"
    (root / "Wrong Artist" / "Wrong Album").mkdir(parents=True)
    f = root / "Wrong Artist" / "Wrong Album" / "Wrong Title.mp3"
    f.write_bytes(b"\xff\xfb\x90\x00" + b"\x00" * 512)   # enough to be a file
    A.set_setting("library_path", str(root))
    conn = A.get_conn()
    conn.execute("INSERT INTO library_index(artist,album,title,path,source)"
                 " VALUES('Wrong Artist','Wrong Album','Wrong Title',?,'slskd')", (str(f),))
    conn.execute("INSERT INTO bad_flags(path,artist,title) VALUES(?,'Wrong Artist','Wrong Title')",
                 (str(f),))
    conn.commit(); conn.close()
    return root, f


def test_reorganize_moves_and_reindexes(A, auth, lib, monkeypatch):
    root, f = lib
    # The fixture file isn't decodable audio, and tag_file swallows its own
    # errors, so capture the call rather than assert on written tags.
    tagged = {}
    monkeypatch.setattr(A, "tag_file", lambda path, meta: tagged.update(path=path, meta=meta))
    r = auth.post("/api/library/reorganize", json={
        "path": str(f), "artist": "Right Artist", "album": "Right Album",
        "title": "Right Title", "track_number": 3})
    assert r.status_code == 200 and r.get_json()["ok"], r.get_json()
    # Destination follows the configured folder_template, not a hardcoded shape.
    dst = root / "Right Artist" / "Right Album" / "Right Title.mp3"
    assert dst.is_file()
    assert tagged["path"] == dst
    assert (tagged["meta"]["artist"], tagged["meta"]["title"],
            tagged["meta"]["track_number"]) == ("Right Artist", "Right Title", 3)
    assert not f.exists()
    conn = A.get_conn()
    row = conn.execute("SELECT * FROM library_index").fetchone()
    assert (row["artist"], row["title"], row["path"]) == ("Right Artist", "Right Title", str(dst))
    # The flag said "not the song it claims to be" — it now is.
    assert conn.execute("SELECT COUNT(*) FROM bad_flags").fetchone()[0] == 0
    conn.close()


def test_reorganize_prunes_the_emptied_directories(A, auth, lib):
    root, f = lib
    auth.post("/api/library/reorganize", json={
        "path": str(f), "artist": "Right Artist", "title": "Right Title"})
    assert not (root / "Wrong Artist").exists()
    assert root.is_dir()          # never prunes past the library root


def test_reorganize_refuses_to_clobber(A, auth, lib):
    root, f = lib
    occupied = root / "Right Artist" / "Right Album"
    occupied.mkdir(parents=True)
    (occupied / "Right Title.mp3").write_bytes(b"existing")
    r = auth.post("/api/library/reorganize", json={
        "path": str(f), "artist": "Right Artist", "album": "Right Album",
        "title": "Right Title"})
    assert r.status_code == 409
    assert f.is_file()                                        # source untouched
    assert (occupied / "Right Title.mp3").read_bytes() == b"existing"


def test_reorganize_rejects_paths_outside_the_library(A, auth, lib, tmp_path):
    outside = tmp_path / "secret.mp3"
    outside.write_bytes(b"x")
    r = auth.post("/api/library/reorganize", json={
        "path": str(outside), "artist": "A", "title": "T"})
    assert r.status_code == 400
    assert "outside the library" in r.get_json()["error"]
    assert outside.is_file()


def test_reorganize_requires_artist_and_title(A, auth, lib):
    _root, f = lib
    r = auth.post("/api/library/reorganize", json={"path": str(f), "artist": "A"})
    assert r.status_code == 400
    assert f.is_file()


# --- delete-and-redownload -------------------------------------------------

def test_redownload_avoids_the_peer_that_served_the_old_file(A, auth, lib):
    """The replacement is a brand-new track row, so without this it can pick
    the same peer and re-fetch the identical bad file."""
    _root, f = lib
    conn = A.get_conn()
    conn.execute("INSERT INTO import_jobs(source,source_type,source_url,status)"
                 " VALUES('library','redownload','','done')")
    conn.execute("INSERT INTO download_history(artist,title,peer,path,source)"
                 " VALUES('Wrong Artist','Wrong Title','badpeer',?,'slskd')", (str(f),))
    conn.commit(); conn.close()

    r = auth.post("/api/library/redownload", json={
        "artist": "Right Artist", "title": "Right Title", "old_path": str(f)})
    assert r.get_json()["ok"]
    conn = A.get_conn()
    row = conn.execute("SELECT * FROM tracks ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()
    assert row["slskd_tried_users"] == "badpeer"
    assert row["force_overwrite"] == 1
    assert row["replace_path"] == str(f)


def test_redownload_without_history_still_queues(A, auth, lib):
    _root, f = lib
    r = auth.post("/api/library/redownload", json={
        "artist": "A", "title": "T", "old_path": str(f)})
    assert r.get_json()["ok"]
    conn = A.get_conn()
    row = conn.execute("SELECT * FROM tracks ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()
    assert row["slskd_tried_users"] == ""


# --- streaming-link resolver ----------------------------------------------

def test_resolve_url_rejects_a_non_music_link(A, auth):
    r = auth.post("/api/library/resolve-url", json={"url": "https://example.com/x"})
    assert r.status_code == 400
    assert "Spotify" in r.get_json()["error"]


def test_resolve_url_returns_the_first_track(A, auth, monkeypatch):
    """Providers are egress-blocked here, so stub the parse the same shape the
    real ones return: (kind, [TrackMeta])."""
    meta = A.TrackMeta(artist="Boards of Canada", album="Geogaddi",
                       title="Dandelion", track_number=7, source_id="x")
    stub = type("P", (), {"name": "spotify",
                          "supports": lambda self, u: True,
                          "parse": lambda self, u: ("track", [meta])})()
    monkeypatch.setattr(A, "_providers", [stub])
    r = auth.post("/api/library/resolve-url",
                  json={"url": "https://open.spotify.com/track/abc"})
    body = r.get_json()
    assert body["ok"]
    assert body["track"] == {"artist": "Boards of Canada", "album": "Geogaddi",
                             "title": "Dandelion", "track_number": 7}


def test_resolve_url_surfaces_provider_failure(A, auth, monkeypatch):
    def boom(self, u): raise RuntimeError("Premium required")
    stub = type("P", (), {"name": "spotify",
                          "supports": lambda self, u: True, "parse": boom})()
    monkeypatch.setattr(A, "_providers", [stub])
    r = auth.post("/api/library/resolve-url",
                  json={"url": "https://open.spotify.com/track/abc"})
    assert r.status_code == 502
    assert "Premium required" in r.get_json()["error"]


# --- stored AcoustID verdict ----------------------------------------------
# verify() knew what the fingerprint matched but only logged it, so a red badge
# was a dead end. The match is now stored and has to survive a rescan.

def test_verify_detail_reports_what_the_audio_actually_is(A, monkeypatch, tmp_path):
    """The mismatch branch is the one that matters: score 0.0 plus the name of
    the song the file really is."""
    monkeypatch.setattr(A, "get_setting", lambda k, *a: "key" if k == "acoustid_api_key" else "")
    fake = type("m", (), {"match": staticmethod(
        lambda *a, **k: [(0.93, "rid", "Dandelion", "Boards of Canada")])})
    monkeypatch.setitem(__import__("sys").modules, "acoustid", fake)
    score, title, artist = A._acoustid.verify_detail(
        tmp_path / "x.mp3", "Autechre", "Foil")
    assert score == 0.0                       # not the song we asked for
    assert (title, artist) == ("Dandelion", "Boards of Canada")


def test_verify_detail_agrees_when_it_matches(A, monkeypatch, tmp_path):
    monkeypatch.setattr(A, "get_setting", lambda k, *a: "key" if k == "acoustid_api_key" else "")
    fake = type("m", (), {"match": staticmethod(
        lambda *a, **k: [(0.91, "rid", "Foil", "Autechre")])})
    monkeypatch.setitem(__import__("sys").modules, "acoustid", fake)
    score, title, artist = A._acoustid.verify_detail(tmp_path / "x.mp3", "Autechre", "Foil")
    assert score == pytest.approx(0.91)
    assert (title, artist) == ("Foil", "Autechre")


def test_verify_still_returns_a_bare_score(A, monkeypatch, tmp_path):
    """The worker calls verify(); its contract must not have shifted."""
    monkeypatch.setattr(A, "get_setting", lambda k, *a: "")
    assert A._acoustid.verify(tmp_path / "x.mp3", "a", "t") is None


def test_stored_verdict_survives_a_rescan(A, lib, monkeypatch):
    """library_index is wiped on every rescan; the verdict is app-generated and
    has to be carried across like the score already was."""
    root, f = lib
    conn = A.get_conn()
    conn.execute("UPDATE library_index SET acoustid_score=0.0, acoustid_title='Dandelion',"
                 " acoustid_artist='Boards of Canada' WHERE path=?", (str(f),))
    conn.commit(); conn.close()

    monkeypatch.setattr(A, "_navidrome_rows", lambda *a, **k: [], raising=False)
    A.scan_library()

    conn = A.get_conn()
    row = conn.execute("SELECT * FROM library_index WHERE path=?", (str(f),)).fetchone()
    conn.close()
    assert row is not None, "rescan dropped the row entirely"
    assert row["acoustid_title"] == "Dandelion"
    assert row["acoustid_artist"] == "Boards of Canada"


def test_reorganize_clears_the_stale_verdict(A, auth, lib):
    """After renaming to the matched name, the file IS that song — keeping the
    old verdict would report a mismatch against itself."""
    _root, f = lib
    conn = A.get_conn()
    conn.execute("UPDATE library_index SET acoustid_score=0.0, acoustid_title='Dandelion',"
                 " acoustid_artist='Boards of Canada' WHERE path=?", (str(f),))
    conn.commit(); conn.close()
    auth.post("/api/library/reorganize", json={
        "path": str(f), "artist": "Boards of Canada", "title": "Dandelion"})
    conn = A.get_conn()
    row = conn.execute("SELECT * FROM library_index").fetchone()
    conn.close()
    assert row["acoustid_title"] is None and row["acoustid_score"] is None


# --- fingerprint metadata comparison ---------------------------------------
# The normaliser deleted characters outside a-z0-9 instead of folding them, so
# a diacritic or an ampersand could make a correct match look like a wrong one.
# That is how a file already named what AcoustID suggests still got ID 0%.

@pytest.mark.parametrize("tagged,matched", [
    # The reported case: "&" in the tag, "and" from AcoustID.
    ("Anthony Gonzalez & Gael García Bernal", "Anthony Gonzalez and Gael García Bernal"),
    ("Simon & Garfunkel", "Simon and Garfunkel"),
    # Accents were dropped entirely, so "Björk" normalised to "bjrk".
    ("Björk", "Bjork"),
    ("Beyoncé", "Beyonce"),
    ("Sigur Rós", "Sigur Ros"),
])
def test_equivalent_names_normalise_the_same(A, tagged, matched):
    assert A._acoustid_norm(tagged) == A._acoustid_norm(matched)


def test_genuinely_different_names_still_differ(A):
    assert A._acoustid_norm("Autechre") != A._acoustid_norm("Aphex Twin")
    assert A._acoustid_norm("Un Poco Loco") != A._acoustid_norm("Remember Me")


def test_ampersand_artist_is_not_reported_as_a_different_song(A, monkeypatch, tmp_path):
    """End to end: the exact pairing from the screenshot must score as a match,
    not as the 0.0 'wrong track' sentinel."""
    monkeypatch.setattr(A, "get_setting", lambda k, *a: "key" if k == "acoustid_api_key" else "")
    fake = type("m", (), {"match": staticmethod(lambda *a, **k: [
        (0.97, "rid", "Un Poco Loco", "Anthony Gonzalez and Gael García Bernal")])})
    monkeypatch.setitem(__import__("sys").modules, "acoustid", fake)
    score, title, artist = A._acoustid.verify_detail(
        tmp_path / "x.flac", "Anthony Gonzalez & Gael García Bernal", "Un Poco Loco")
    assert score == pytest.approx(0.97), "equivalent artist spelling scored as a mismatch"
    assert title == "Un Poco Loco"


# --- fingerprint failure diagnostics --------------------------------------
# pyacoustid runs fpcalc with stderr at /dev/null and surfaces only "exited
# with status N", so a real failure arrived with no cause attached.

def test_diagnose_reports_a_missing_binary(A, tmp_path, monkeypatch):
    monkeypatch.setenv("FPCALC", str(tmp_path / "definitely-not-here"))
    assert "not installed" in A._fpcalc_diagnose(tmp_path / "x.flac")


def test_diagnose_surfaces_the_stderr_message(A, tmp_path, monkeypatch):
    """The message fpcalc printed is the whole point of re-running it."""
    class P:
        returncode = 2
        stdout = ""
        stderr = "ERROR: Could not open the input file (No such file or directory)\n"
    monkeypatch.setattr(A.subprocess, "run", lambda *a, **k: P())
    out = A._fpcalc_diagnose(tmp_path / "x.flac")
    assert "exit 2" in out and "No such file or directory" in out


def test_diagnose_notes_a_transient_failure(A, tmp_path, monkeypatch):
    class P:
        returncode = 0
        stdout = "DURATION=10\nFINGERPRINT=abc\n"
        stderr = ""
    monkeypatch.setattr(A.subprocess, "run", lambda *a, **k: P())
    assert "transient" in A._fpcalc_diagnose(tmp_path / "x.flac")


def test_diagnose_flags_a_silent_nonzero_exit(A, tmp_path, monkeypatch):
    """Exit 3 with nothing on stderr is the reported symptom, and no ordinary
    file problem produces it — say so rather than repeating the number."""
    class P:
        returncode = 3
        stdout = ""
        stderr = ""
    monkeypatch.setattr(A.subprocess, "run", lambda *a, **k: P())
    out = A._fpcalc_diagnose(tmp_path / "x.flac")
    assert "exit 3" in out and "fpcalc -version" in out


def test_file_note_reports_size_and_missing_files(A, tmp_path):
    f = tmp_path / "a.flac"; f.write_bytes(b"x" * 17)
    assert A._file_note(f) == "17 bytes"
    assert "cannot stat" in A._file_note(tmp_path / "nope.flac")


def test_failed_fingerprint_logs_the_cause(A, tmp_path, monkeypatch, caplog):
    """The whole chain: a failure must log the path, the size and the reason."""
    monkeypatch.setattr(A, "get_setting", lambda k, *a: "key" if k == "acoustid_api_key" else "")
    boom = type("m", (), {"match": staticmethod(
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("fpcalc exited with status 3")))})
    monkeypatch.setitem(__import__("sys").modules, "acoustid", boom)
    class P:
        returncode = 2
        stdout = ""
        stderr = "ERROR: Could not open the input file (Invalid data found when processing input)\n"
    monkeypatch.setattr(A.subprocess, "run", lambda *a, **k: P())
    f = tmp_path / "Surface Pressure.flac"; f.write_bytes(b"")
    with caplog.at_level("WARNING"):
        score, _t, _a = A._acoustid.verify_detail(f, "Jessica Darrow", "Surface Pressure")
    assert score is None
    msg = caplog.text
    assert "Surface Pressure.flac" in msg
    assert "0 bytes" in msg
    assert "Invalid data found" in msg

"""Deleting songs, albums and artists from the Library page."""
import pytest


@pytest.fixture()
def lib(A, tmp_path, monkeypatch):
    root = tmp_path / "music"
    A.set_setting("library_path", str(root))
    scans = []
    monkeypatch.setattr(A, "_navidrome_start_scan", lambda: scans.append(1))

    def song(artist, album, title):
        f = root / artist / album / f"{title}.flac"
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_bytes(b"\x00" * 50)
        return f

    s1 = song("Band", "First", "One")
    s2 = song("Band", "First", "Two")
    s3 = song("Band", "Second", "Three")
    conn = A.get_conn()
    for f in (s1, s2, s3):
        conn.execute("INSERT INTO library_index(artist,title,album,path) VALUES(?,?,?,?)",
                     ("Band", f.stem, f.parent.name, str(f)))
    # "Three" is in a playlist; a stray completed queue row says "One" is owned.
    jid = conn.execute(
        "INSERT INTO import_jobs(source,source_type,source_url,status,playlist_name)"
        " VALUES('Spotify','playlist','https://x/pl','done','Road Trip')").lastrowid
    A._m3u_path_for("Road Trip").write_text(f"#EXTM3U\n{s3}\n", encoding="utf-8")
    conn.execute("INSERT INTO import_jobs(source,source_type,source_url,status)"
                 " VALUES('library','redownload','','done')")
    conn.execute("INSERT INTO tracks(job_id,artist,title,slskd_state,local_path)"
                 " VALUES(?,'Band','One','completed',?)", (jid + 1, str(s1)))
    conn.commit(); conn.close()
    return {"root": root, "s1": s1, "s2": s2, "s3": s3, "scans": scans}


def test_preview_lists_sizes_and_playlists(A, auth, lib):
    d = auth.post("/api/library/delete-preview",
                  json={"paths": [str(lib["s1"]), str(lib["s3"])]}).get_json()
    by = {s["path"]: s for s in d["songs"]}
    assert by[str(lib["s1"])] == {"path": str(lib["s1"]), "size": 50, "playlists": []}
    assert by[str(lib["s3"])]["playlists"] == ["Road Trip"]


def test_delete_requires_typed_confirmation(A, auth, lib):
    for confirm in (None, "", "yes", "DELET"):
        r = auth.post("/api/library/delete", json={"paths": [str(lib["s1"])], "confirm": confirm})
        assert r.status_code == 400
    assert lib["s1"].exists()


def test_delete_album(A, auth, lib):
    d = auth.post("/api/library/delete", json={
        "paths": [str(lib["s1"]), str(lib["s2"])], "confirm": "delete"}).get_json()
    assert d["ok"] and sorted(d["deleted"]) == sorted([str(lib["s1"]), str(lib["s2"])])
    assert not (lib["root"] / "Band" / "First").exists()     # emptied album folder pruned
    assert lib["s3"].exists() and lib["scans"]
    conn = A.get_conn()
    assert conn.execute("SELECT COUNT(*) FROM library_index").fetchone()[0] == 1
    assert not A._already_in_library(conn, "Band", "One")
    conn.close()


def test_delete_keeps_songs_in_playlists(A, auth, lib):
    d = auth.post("/api/library/delete", json={
        "paths": [str(lib["s1"]), str(lib["s2"]), str(lib["s3"])], "confirm": "DELETE"}).get_json()
    assert d["ok"] and d["kept"] == [str(lib["s3"])] and len(d["deleted"]) == 2
    assert lib["s3"].exists()
    assert (lib["root"] / "Band").exists()                   # still holds Second/Three


def test_delete_refuses_paths_outside_the_library(A, auth, lib, tmp_path):
    outside = tmp_path / "precious.flac"
    outside.write_bytes(b"x")
    d = auth.post("/api/library/delete", json={
        "paths": [str(outside), str(lib["root"] / ".." / "precious.flac")],
        "confirm": "DELETE"}).get_json()
    assert d["deleted"] == [] and len(d["failed"]) == 2
    assert outside.exists()

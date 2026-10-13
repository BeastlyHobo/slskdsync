"""Removing a playlist and deleting the songs it brought into the library."""
import pytest


@pytest.fixture()
def lib(A, tmp_path, monkeypatch):
    """Two playlists sharing one song, plus a song owned before either."""
    root = tmp_path / "music"
    A.set_setting("library_path", str(root))
    calls = []
    monkeypatch.setattr(A, "_navidrome_delete_imported", lambda name: calls.append(("del", name)))
    monkeypatch.setattr(A, "_navidrome_start_scan", lambda: calls.append(("scan",)))

    def song(artist, title):
        f = root / artist / "Album" / f"{title}.mp3"
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_bytes(b"\x00" * 100)
        return f

    only_a = song("Artist A", "Only A")
    shared = song("Artist S", "Shared")
    owned = song("Artist O", "Owned Before")
    only_b = song("Artist B", "Only B")

    conn = A.get_conn()

    def playlist(name, url, downloads, m3u):
        cur = conn.execute(
            "INSERT INTO import_jobs(source,source_type,source_url,status,playlist_name)"
            " VALUES('Spotify','playlist',?,'done',?)", (url, name))
        jid = cur.lastrowid
        for f in downloads:
            conn.execute(
                "INSERT INTO tracks(job_id,artist,title,album,slskd_state,local_path)"
                " VALUES(?,?,?,?,'completed',?)",
                (jid, f.parent.parent.name, f.stem, "Album", str(f)))
        A._m3u_path_for(name).write_text(
            "#EXTM3U\n" + "".join(f"#EXTINF:0,x\n{f}\n" for f in m3u), encoding="utf-8")
        return jid

    a = playlist("Gym", "https://open.spotify.com/playlist/a", [only_a, shared], [only_a, shared, owned])
    b = playlist("Chill", "https://open.spotify.com/playlist/b", [only_b], [only_b, shared])
    for f in (only_a, shared, owned, only_b):
        conn.execute("INSERT INTO library_index(artist,title,album,path) VALUES(?,?,?,?)",
                     (f.parent.parent.name, f.stem, "Album", str(f)))
    conn.commit(); conn.close()
    return {"root": root, "a": a, "b": b, "only_a": only_a, "shared": shared,
            "owned": owned, "only_b": only_b, "calls": calls}


def test_songs_mark_downloads_and_other_playlists(A, auth, lib):
    d = auth.get(f"/api/playlists/{lib['a']}/songs").get_json()
    assert d["ok"]
    by = {s["path"]: s for s in d["songs"]}
    assert set(by) == {str(lib["only_a"]), str(lib["shared"]), str(lib["owned"])}
    assert by[str(lib["only_a"])]["downloaded"] and not by[str(lib["only_a"])]["other_playlists"]
    assert by[str(lib["shared"])]["other_playlists"] == ["Chill"]
    assert not by[str(lib["owned"])]["downloaded"]
    assert by[str(lib["only_a"])]["size"] == 100


def test_remove_deletes_picked_files_and_the_playlist(A, auth, lib):
    r = auth.post(f"/api/playlists/{lib['a']}/remove",
                  json={"delete_paths": [str(lib["only_a"])]})
    d = r.get_json()
    assert d["ok"] and d["deleted"] == 1 and d["failed"] == []
    assert not lib["only_a"].exists()
    assert not (lib["root"] / "Artist A").exists()       # emptied folders pruned
    assert lib["owned"].exists()                         # not picked → kept
    assert not A._m3u_path_for("Gym").exists()
    assert A._m3u_path_for("Chill").exists()
    assert ("del", "Gym") in lib["calls"] and ("scan",) in lib["calls"]
    conn = A.get_conn()
    assert conn.execute("SELECT COUNT(*) FROM import_jobs WHERE id=?", (lib["a"],)).fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM tracks WHERE job_id=?", (lib["a"],)).fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM library_index WHERE path=?",
                        (str(lib["only_a"]),)).fetchone()[0] == 0
    # No longer "owned", so it could be downloaded again some day.
    assert not A._already_in_library(conn, "Artist A", "Only A")
    conn.close()


def test_remove_keeps_songs_another_playlist_uses(A, auth, lib):
    d = auth.post(f"/api/playlists/{lib['a']}/remove",
                  json={"delete_paths": [str(lib["shared"])]}).get_json()
    assert d["ok"] and d["deleted"] == 0
    assert lib["shared"].exists()
    conn = A.get_conn()
    # Chill's own download row is untouched.
    assert conn.execute("SELECT COUNT(*) FROM tracks WHERE job_id=?", (lib["b"],)).fetchone()[0] == 1
    conn.close()


def test_remove_only_deletes_this_playlists_songs(A, auth, lib):
    d = auth.post(f"/api/playlists/{lib['a']}/remove",
                  json={"delete_paths": [str(lib["only_b"])]}).get_json()
    assert d["ok"] and d["deleted"] == 0
    assert lib["only_b"].exists()


def test_remove_with_no_files_keeps_every_song(A, auth, lib):
    d = auth.post(f"/api/playlists/{lib['a']}/remove", json={}).get_json()
    assert d["ok"] and d["deleted"] == 0
    assert lib["only_a"].exists() and lib["shared"].exists() and lib["owned"].exists()
    assert not A._m3u_path_for("Gym").exists()


def test_remove_reports_a_missing_file(A, auth, lib):
    lib["only_a"].unlink()
    d = auth.post(f"/api/playlists/{lib['a']}/remove",
                  json={"delete_paths": [str(lib["only_a"])]}).get_json()
    assert d["ok"] and d["deleted"] == 0
    assert d["failed"] == [{"path": str(lib["only_a"]), "error": "file not found"}]


def test_remove_unknown_playlist(A, auth):
    assert auth.post("/api/playlists/999/remove", json={}).status_code == 404
    assert auth.get("/api/playlists/999/songs").status_code == 404


def test_remove_cancels_in_flight_downloads(A, auth, lib, monkeypatch):
    cancelled = []

    class FakeSlskd:
        def cancel_search(self, sid):
            if sid:
                cancelled.append(("search", sid))

        def cancel_download(self, user, filename):
            cancelled.append(("download", user, filename))

    monkeypatch.setattr(A, "SlskdClient", FakeSlskd)
    conn = A.get_conn()
    conn.execute("INSERT INTO tracks(job_id,artist,title,slskd_state,slskd_search_id)"
                 " VALUES(?,'X','Searching','searching','s1')", (lib["a"],))
    conn.execute("INSERT INTO tracks(job_id,artist,title,slskd_state,slskd_search_id,"
                 "slskd_download_user,slskd_download_filename)"
                 " VALUES(?,'X','Downloading','downloading','s2','peer','f.flac')", (lib["a"],))
    conn.commit(); conn.close()
    assert auth.post(f"/api/playlists/{lib['a']}/remove", json={}).get_json()["ok"]
    assert sorted(cancelled) == [("download", "peer", "f.flac"), ("search", "s1"), ("search", "s2")]

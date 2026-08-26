"""Matching a finished download in the watch folder to the track that wanted it.

Peers name files however they like. Matching on the filename text alone leaves
files sitting in the watch folder forever when the name uses underscores, drops
accents, spells "&" as "and", or is just a track number — even though the file
is correct and usually tagged correctly too.
"""
import struct

import pytest

pytest.importorskip("mutagen")
import mutagen  # noqa: E402


def write_flac(path, title=None, artist=None, album=None, seconds=180):
    """A valid, taggable FLAC header with no audio frames.

    Enough for mutagen to open, report a duration, and round-trip tags, without
    needing an encoder in the test environment.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    si = bytearray()
    si += struct.pack(">H", 4096) + struct.pack(">H", 4096)
    si += (0).to_bytes(3, "big") + (0).to_bytes(3, "big")
    si += ((44100 << 44) | (1 << 41) | (15 << 36) | (44100 * seconds)).to_bytes(8, "big")
    si += b"\x00" * 16
    path.write_bytes(b"fLaC" + bytes([0x80]) + len(si).to_bytes(3, "big") + bytes(si))
    if title or artist or album:
        f = mutagen.File(path, easy=True)
        if title:  f["title"]  = [title]
        if artist: f["artist"] = [artist]
        if album:  f["album"]  = [album]
        f.save()
    return path


@pytest.fixture()
def watch(A, tmp_path):
    d = tmp_path / "downloads"
    d.mkdir()
    A.set_setting("download_watch_path", str(d))
    return d


def find(A, watch, artist, title):
    return A.discover_download_for_track({"artist": artist, "title": title})


# --- the shapes that were getting stuck ------------------------------------

def test_underscores_for_spaces(A, watch):
    f = write_flac(watch / "Anthony_Gonzalez_-_Un_Poco_Loco.flac")
    assert find(A, watch, "Anthony Gonzalez", "Un Poco Loco") == f


def test_dots_for_spaces(A, watch):
    f = write_flac(watch / "01.Un.Poco.Loco.flac")
    assert find(A, watch, "Anthony Gonzalez", "Un Poco Loco") == f


def test_accents_dropped_by_the_peer(A, watch):
    f = write_flac(watch / "Sigur Ros - Staralfur.flac")
    assert find(A, watch, "Sigur Rós", "Starálfur") == f


def test_accents_added_by_the_peer(A, watch):
    f = write_flac(watch / "Beyonce - Halo.flac")
    assert find(A, watch, "Beyoncé", "Halo") == f


def test_ampersand_spelled_out(A, watch):
    f = write_flac(watch / "Simon and Garfunkel - The Boxer.flac")
    assert find(A, watch, "Simon & Garfunkel", "The Boxer") == f


def test_ampersand_in_the_title(A, watch):
    f = write_flac(watch / "Salt-N-Pepa - Whatta Man.flac")
    assert find(A, watch, "Salt-N-Pepa", "Whatta Man") == f


def test_filename_is_only_a_track_number_but_tags_are_right(A, watch):
    """The case filename matching can never solve."""
    write_flac(watch / "junk" / "02.flac", title="Nothing Like This", artist="Someone Else")
    f = write_flac(watch / "junk" / "07.flac", title="Un Poco Loco",
                   artist="Anthony Gonzalez", album="Coco")
    assert find(A, watch, "Anthony Gonzalez", "Un Poco Loco") == f


def test_untitled_filename_with_tags(A, watch):
    f = write_flac(watch / "track 3.mp3.flac", title="Surface Pressure", artist="Jessica Darrow")
    assert find(A, watch, "Jessica Darrow", "Surface Pressure") == f


# --- what must keep working ------------------------------------------------

def test_still_wont_match_a_longer_word(A, watch):
    """The word-boundary guard: 'Home' must not grab 'Homewrecker'."""
    write_flac(watch / "Someone - Homewrecker.flac")
    assert find(A, watch, "Someone", "Home") is None


def test_waits_rather_than_guess_between_two_versions(A, watch):
    write_flac(watch / "Artist A - Halo.flac")
    write_flac(watch / "Artist B - Halo.flac")
    assert find(A, watch, "Unrelated Artist", "Halo") is None


def test_artist_disambiguates_same_title(A, watch):
    write_flac(watch / "Artist A - Halo.flac")
    f = write_flac(watch / "Beyonce - Halo.flac")
    assert find(A, watch, "Beyoncé", "Halo") == f


def test_tag_match_still_needs_the_title_to_agree(A, watch):
    """A tagged file for a different song must not be grabbed."""
    write_flac(watch / "99.flac", title="Completely Different", artist="Anthony Gonzalez")
    assert find(A, watch, "Anthony Gonzalez", "Un Poco Loco") is None


def test_no_audio_files_returns_none(A, watch):
    assert find(A, watch, "Anyone", "Anything") is None


def test_exact_filename_match_is_unaffected(A, watch):
    f = write_flac(watch / "Jessica Darrow - Surface Pressure.flac")
    assert find(A, watch, "Jessica Darrow", "Surface Pressure") == f


# --- through to the library ------------------------------------------------

def test_a_stuck_download_is_found_moved_and_renamed(A, watch, tmp_path):
    """The point of the exercise: a file the old matcher left sitting in the
    watch folder ends up in the library under the right name."""
    music = tmp_path / "music"
    music.mkdir()
    A.set_setting("library_path", str(music))
    A.set_setting("folder_template", "{artist}/{album}/{title}{ext}")

    # Named nothing like the track, but tagged correctly — the stuck case.
    src = write_flac(watch / "Coco OST" / "07.flac",
                     title="Un Poco Loco", artist="Anthony Gonzalez", album="Coco")
    track = {"artist": "Anthony Gonzalez", "album": "Coco", "title": "Un Poco Loco",
             "track_number": 7, "cover_url": ""}

    found = A.discover_download_for_track(track)
    assert found == src, "matcher did not find the tagged file"

    dst = A.Organizer.target_path(track, found)
    ok, result = A.Organizer.move_file(found, dst)
    assert ok
    assert dst == music / "Anthony Gonzalez" / "Coco" / "Un Poco Loco.flac"
    assert dst.is_file()
    assert not src.exists(), "source left behind in the watch folder"

    A.tag_file(dst, track)
    written = mutagen.File(dst, easy=True)
    assert written["title"] == ["Un Poco Loco"]
    assert written["artist"] == ["Anthony Gonzalez"]


def test_tags_are_not_reread_for_every_track(A, watch, monkeypatch):
    """The tag pass runs per waiting track; without caching a full watch folder
    would be re-parsed on every tick."""
    for i in range(5):
        write_flac(watch / f"{i:02d}.flac", title=f"Song {i}", artist="Someone")
    reads = []
    real = mutagen.File
    monkeypatch.setattr(A.mutagen, "File", lambda p, **k: (reads.append(str(p)), real(p, **k))[1])

    assert find(A, watch, "Someone", "Song 3") is not None
    first = len(reads)
    assert first >= 5
    assert find(A, watch, "Someone", "Song 4") is not None
    assert len(reads) == first, "tags were re-parsed instead of served from cache"

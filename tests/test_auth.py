"""Regressions for the auth fallback and the settings-blanking bug.

A settings save that didn't carry the username field blanked it, and
get_auth_credentials() then fell through to the env defaults -- so the real
password stopped working and admin/admin started working, with /setup refusing
to run because a hash was still present.
"""
from werkzeug.security import check_password_hash, generate_password_hash


def configure(A, user="tristan", password="correct horse battery"):
    A.set_setting("app_username", user)
    A.set_setting("app_password_hash", generate_password_hash(password))


def logs_in(A, user, password):
    u, h = A.get_auth_credentials()
    return u == user and check_password_hash(h, password)


def test_configured_credentials_work(A):
    configure(A)
    assert logs_in(A, "tristan", "correct horse battery")
    assert not A.is_first_run()


def test_admin_default_rejected_once_configured(A):
    configure(A)
    assert not logs_in(A, "admin", "admin")


def test_blank_username_does_not_unlock_admin(A):
    """The core bug: a blanked username must not re-enable the env defaults."""
    configure(A)
    A.set_setting("app_username", "")
    assert not logs_in(A, "admin", "admin")
    # The configured password still authenticates, under the fallback username.
    _, h = A.get_auth_credentials()
    assert check_password_hash(h, "correct horse battery")


def test_is_first_run_agrees_with_get_auth_credentials(A):
    """Both must key on the hash alone, or the app can end up with no way in:
    /setup refuses to run because a hash exists, while the login accepts the
    env defaults because the username is blank."""
    configure(A)
    A.set_setting("app_username", "")
    assert not A.is_first_run()
    assert not logs_in(A, "admin", "admin")


def test_partial_settings_save_preserves_unsent_keys(A, auth):
    configure(A)
    A.set_setting("acoustid_api_key", "SECRETKEY123")
    A.set_setting("library_path", "/old")

    # A form that carries only library_path -- no username, no api keys.
    auth.post("/settings", data={"library_path": "/music"})

    assert A.get_setting("library_path") == "/music"      # updated
    assert A.get_setting("app_username") == "tristan"     # preserved
    assert A.get_setting("acoustid_api_key") == "SECRETKEY123"
    assert logs_in(A, "tristan", "correct horse battery")
    assert not logs_in(A, "admin", "admin")


def test_short_password_rejected_without_partial_write(A, auth):
    configure(A)
    A.set_setting("library_path", "/old")
    auth.post("/settings", data={"library_path": "/new", "new_password": "abc"})
    # Validation runs before any key is written.
    assert A.get_setting("library_path") == "/old"


def test_password_change_still_works(A, auth):
    configure(A)
    auth.post("/settings", data={"app_username": "tristan",
                                 "new_password": "a-longer-password"})
    assert logs_in(A, "tristan", "a-longer-password")
    assert not logs_in(A, "tristan", "correct horse battery")

---
name: slskdsync-dev
description: House rules, architecture map, and hard-won gotchas for developing the slskdsync codebase. Read this BEFORE editing app/app.py or any template — it covers the DB schema and what survives rescans, the download worker pipeline, sandbox limitations (no Flask import, no spotify.com), testing patterns that actually work here, frontend JS conventions, and the git author/push workflow the stop-hook enforces.
---

# slskdsync development guide

Self-hosted music app: syncs Spotify/Apple Music/TIDAL/Deezer playlists to Soulseek
(slskd) downloads, organizes files into a Navidrome library, writes M3U playlists.
Flask + SQLite + vanilla-JS templates. PWA (manifest + service worker).

## Layout

- `app/app.py` — the ENTIRE backend (~4000 lines): DB, providers, clients, worker,
  all routes. There are no other Python modules. Search by section, don't read whole file.
- `app/templates/*.html` — each page is self-contained: Jinja shell + inline CSS +
  inline JS view layer. `base.html` holds nav (`nav_items` list), toast, brand macro.
- `app/static/styles.css` — shared CSS variables (`--surface2`, `--accent`, `--err`,
  `--radius-pill`, …). Reuse them; don't hardcode colors.
- Data lives at `app/data/` (`DATA_DIR = APP_DIR / "data"`): `app.db`, `logs/app.log`
  (RotatingFileHandler 5MB×2), covers cache.
- `AGENTS.md` / `CLAUDE.md` — behavioral guidelines (surgical changes, simplicity first).

## Database (SQLite, WAL) — what survives what

Schema is created in `init_db()` (~line 107); columns are added via `PRAGMA table_info`
migration loops right below it. **To add a column: add it to the migration list, never
edit the CREATE TABLE only.**

| Table | Role | Lifecycle |
|---|---|---|
| `settings` | key/value config | permanent |
| `import_jobs` | one per import/download batch (`source_type`: playlist/album/track/redownload…) | permanent |
| `tracks` | download queue rows, keyed to job. `slskd_state`: pending → searching → downloading → completed/failed | permanent |
| `library_index` | scanned library files | **WIPED on every rescan** (`DELETE FROM library_index` in `scan_library()`) — never store user data here that must persist |
| `playlist_tracks` | playlist membership snapshot at import time | replaced on manual M3U regen (DELETE+INSERT in `_build_playlist_entries`) |
| `bad_flags` | user "wrong grab" flags, **keyed by file path** | permanent — deliberately survives rescans; join by path, not id |
| `download_history` | append-only record of every completed download (peer, format, score) | permanent — powers `/stats`; queue rows get cleared, this doesn't |

`tracks` notable columns: `force_overwrite` (re-downloads overwrite existing file),
`custom_search` (user-edited search query, cleared after use), `slskd_tried_users`
(peer fallback), `acoustid_score` (also on `library_index`).

## Download pipeline (`_worker_tick`, ~line 2131)

Background thread loop (`run_worker`), each tick:
1. pending tracks → slskd search (uses `custom_search` if set, else artist+title)
2. search results → pick file (`_find_file_for_track`), enqueue transfer, record user
3. downloading → poll watch folder (`discover_download_for_track`) → on arrival:
   `Organizer.target_path()` → `Organizer.move_file(force_overwrite=…)` → `tag_file()`
   (mutagen + cover embed) → AcoustID verify → mark completed → debounced M3U sync
4. failed transfers retry with a different peer via `slskd_tried_users`

`Organizer.target_path` builds `library/Artist/Album/NN - Title{ext}` — **ext comes
from the downloaded file**, so re-downloading in a different format leaves the old
file behind (known gap; same-format re-downloads overwrite cleanly).

## Providers (Spotify quirk!)

`SpotifyProvider.parse()` tries the official API (`_parse_api`, spotipy — requires
the app owner to have **Premium** since 2026), and on 403/429 or missing credentials
falls back to `_parse_scraper` (`spotifyscraper` lib — anonymous embed token, no
credentials needed). Scraper gotcha: `playlist.tracks` yields `PlaylistTrack`
wrappers — the real track is `pt.track`, NOT `pt` itself.

All providers return `(kind, list[TrackMeta])`. `TrackMeta` dataclass ~line 285.

## Sandbox limitations (agent environment)

- **Cannot import the Flask app**: `cryptography`/`cffi` are broken here. Never try
  `python -c "import app"` to verify. Instead test logic in isolation: copy the SQL /
  pure-Python under test into a scratchpad script with a throwaway `sqlite3` DB using
  the real schema, and assert there.
- **ALL external music APIs are egress-blocked** (proxy CONNECT 403): Spotify,
  Deezer, ListenBrainz, Apple — everything except package registries. WebFetch is
  blocked too. Never plan on live-verifying an external API here; test parsing
  against fixture JSON shaped per the API's documented schema, and tell the user
  which shapes need a visual check on their deployment (their Docker box has
  normal internet).
- **Templates can't be `node --check`ed** — Jinja `{{ … }}` breaks the JS parser.
  Extract just the new JS functions into a temp file to syntax-check them.
- `slskd`, Navidrome, and the user's Docker stack are not reachable; the user tests
  live behavior. Say clearly what was verified vs. what needs their testing.

## Frontend conventions & bugs already paid for

- Escape helper `esc()` must escape **apostrophes** too (`&#39;`) — an inline
  `onclick="fn('${esc(x)}')"` broke on "Don't Be Sad". Prefer `addEventListener` +
  `data-i`/`data-tid`/`data-path` attributes over interpolating strings into onclick.
  When you must inline, use `JSON.stringify(value)` into the attribute.
- `library.html`: server ships ALL tracks as JSON (`{{ tracks | tojson }}`); all
  views (home/artists/albums/songs/playlists/drill-downs) are client-side renders.
  Songs view paginates (`PAGE_SIZE=300`, "Load more"). Nav state persists in
  `sessionStorage` (`lib_state`). Full `render()` is expensive — prefer targeted DOM
  updates (see AcoustID single-track badge update in `pollAid()`).
- Row/modal pattern: rows carry `data-path` (+ `data-tid` when it's a library_index
  row); `showTrackInfo` looks the track up in `ALL_TRACKS || _plTracks` by path.
- Buttons use `.lib-sm-btn`; filter chips `.af-chip`; modal classes `.tm-*`.
- Playlist-tracks rows come from `/api/library/playlist/<job_id>` and are normalized
  to library-track shape (with `id: null`) so shared `trackRowHtml()` works.

## Key API endpoints

- `POST /api/library/acoustid` `{scope: track|album|artist|playlist|all, …}` — starts
  bg job; poll `GET …/acoustid/status` (`{in_progress, done, total}`); fetch
  `GET …/acoustid/scores` (`{id: score}` map). Score: <0 = unidentified, 0–1 = match.
- `POST /api/library/redownload` `{artist, title, album, query?}` — creates a
  1-track job with `force_overwrite=1` and optional `custom_search`.
- `GET /api/library/track-info/<lib_id>` — disk details (size/bitrate/duration) via mutagen.
- `POST /api/library/flag-bad` `{path, artist, title, flagged}` — toggles `bad_flags`.
- `POST /api/download/batch` — one job **per track** (deliberate: avoids the
  album-batching optimization grouping them into one search).
- `GET /api/logs?n=1000` — last N lines from log file, falls back to ring buffer.
- `POST /api/playlists/<job_id>/sync` — full on-demand sync (fetch → queue new →
  M3U → Navidrome), same path as nightly; stamps `import_jobs.last_synced_at/last_sync_new`.
- `GET /api/library/index` — owned keys PLUS in-flight queue items flagged `q:1`
  (Search/Discover render "queued" and hide download buttons).
- `GET /api/status/sources` — cached (5 min) monochrome health; UI adds `no-mono`
  body class to hide TIDAL buttons when down.
- Pages: `/attention` (needs_search + bad flags + AcoustID <50% triage),
  `/stats` (history/format/AcoustID breakdown, linked from Settings).

## Frontend gotcha #1 (bugs already paid for — twice)

NEVER interpolate `JSON.stringify(x)` bare into a double-quoted HTML attribute
(`onclick="fn(${aJ})"`) — the JSON's own quotes terminate the attribute and the
handler becomes a syntax error for EVERY value. Wrap it: `${esc(JSON.stringify(x))}`,
or better, use `data-*` attributes + `addEventListener`. Same trap via Jinja:
`onclick="fn('{{ x|e }}')"` breaks on apostrophes — use `data-name="{{ x|e }}"`.

## Settings keys (settings table, via `get_setting`/`set_setting`)

`slskd_url/api_key/user/pass`, `library_path` (default `/music`),
`download_watch_path`, `folder_template`, `replace_existing`, `quality`,
`navidrome_url/user/pass`, `spotify_client_id/secret`, `apple_team_id/key_id/private_key`,
`acoustid_api_key`, `anthropic_api_key`, `monochrome_url/fallbacks`,
`listenbrainz_username`, `library_scan_interval`, `app_username/password_hash`.

## Git workflow (stop-hook enforced)

- Work happens on local `main`; push to the session feature branch with
  `git push origin main:<feature-branch>` (branch named in the session prompt).
- **Before committing**: `git config user.email noreply@anthropic.com && git config
  user.name Claude`. The stop-hook flags any commit whose author/committer differs;
  fix with `git commit --amend --no-edit --reset-author` then
  `git push --force-with-lease origin main:<branch>`.
- The hook may still report "Unverified" for lack of a GPG signature — that's
  environmental (no signing key); don't loop on re-amending a correct commit.
- Never create a PR unless explicitly asked.

## Known gaps / watch-outs

- Re-download with a different file extension orphans the old file (see pipeline above).
- `library_index` ids change on every rescan — never persist them client-side or in DB.
- Apostrophes, quotes, and slashes in metadata are recurring bug sources: paths
  sanitize `/`→`-` in `target_path`; JS needs full `esc()`.
- The user runs this in Docker (see `docker-compose.yml`); log location and `/music`
  paths differ from the repo checkout.

# slskdsync

Single-container web app to:
- Paste Spotify/Apple Music URLs (playlist/album/track; Spotify artist supported)
- Queue searches in `slskd`
- Watch completed downloads and move files into a Navidrome-friendly library structure
- Optionally mark playlist imports for Navidrome playlist creation (toggle saved per import)

## Run

```bash
cp .env.example .env
docker compose pull
docker compose up -d
```

Open `http://localhost:5035`.

To use a published `latest` image, set `SLSKDSYNC_IMAGE` in `.env` (for example `ghcr.io/<your-user>/slskdsync:latest`) and run:

```bash
docker compose pull
docker compose up -d
```

If you are developing locally from source instead of pulling, run:

```bash
docker build -t slskdsync:latest .
docker compose up -d
```


## Settings (in UI)
- `slskd_url`, `slskd_user`, `slskd_pass`, `slskd_api_key`
- `download_watch_path` (where slskd writes files)
- `library_path` (Navidrome library root)
- `folder_template` default: `{artist}/{album}/{track_number:02d} - {title}{ext}`
- `quality` preference (currently informational and future result-selection hook)
- `replace_existing` (`0` skip, `1` overwrite)
- `navidrome_url`, `navidrome_user`, `navidrome_pass`

## Notes
- Downloads always come from slskd; this app queues searches and organizes files when detected.
- Spotify import uses Spotify API client credentials from `.env`.
- Apple Music support currently includes track links containing `?i=<trackId>` via iTunes lookup.
- Apple album/playlist and Navidrome playlist publishing are scaffolded as extension points.

## Default Login
- Username: `admin`
- Password: `admin`

Change these in `.env` before production use.

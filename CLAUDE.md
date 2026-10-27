# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A single-file Go HTTP server that wraps the [`sldl`](https://github.com/fiso64/slsk-batchdl) CLI (Soulseek batch downloader). It exposes a web UI and JSON/SSE API for searching MusicBrainz, previewing results via sldl, and triggering downloads that are then auto-tagged with beets.

## Build & run

```sh
# Local build
go build -o sldl-wrapper .

# Docker (production path)
docker compose up --build

# Cross-compile a linux binary (gitignored, not used by the Docker build)
CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o sldl-wrapper-linux .
```

No test suite exists.

## Deployment

Runs as a Docker container on the Synology NAS, project dir `/volume1/docker/sldl-wrapper` (UI on port 6060). No CI: copy changed files there (Synology MCP `create_file` paths are relative to `/volume1`, i.e. `/docker/sldl-wrapper/...`), then rebuild with `docker compose up -d --build` in that dir. What needs a rebuild:

- `main.go`, `Dockerfile` → rebuild
- `static/index.html`, `beets-config.yaml`, the `sldl` binary → bind-mounted, picked up without a rebuild (beets config on the next import)

The `sldl` binary is not in the image; it is bind-mounted from the NAS file (`/volume1/docker/sldl-wrapper/sldl`). To upgrade sldl, replace that file. The Dockerfile installs apt packages before copying the app, so a code-only rebuild reuses that cached layer.

## Runtime dependencies

These are only available inside the container (not locally):

| Dependency | Path/command | Purpose |
|---|---|---|
| `sldl` binary | `/sldl/sldl` | Soulseek downloader |
| `beet` | system PATH | Auto-tagging with config at `/etc/beets/config.yaml` |
| `python3` + `mutagen` | system PATH | Promo-tag stripping |
| Music volume | `/music` | Download destination, path-traversal boundary |
| Beets data | `/data` | `beets-library.db` and `beets-import.log` |

Environment variables required: `SLDL_USER`, `SLDL_PASS`. Optional: `SLDL_PREVIEW_USER`, `SLDL_PREVIEW_PASS` (see Job queue), `SLSKD_URL` (see slskd search; docker-compose sets it). On the NAS the four account variables live in `/volume1/docker/sldl-wrapper/.env`.

The compose file also runs `slskd-search` (`slskd/slskd`, pinned), logged in as the preview account, data in `/volume1/docker/slskd-search/app`, no published ports.

## Architecture

Everything lives in `main.go`. Key concepts:

**Job queue** — a Soulseek account can only be logged in once (a second login kicks the first), so sldl runs on one account must be serialized. `jobQueue` (cap 50, one `jobWorker`) runs downloads. If `SLDL_PREVIEW_USER`/`SLDL_PREVIEW_PASS` are set, previews get their own `previewQueue` + worker, log in as that account and pass `--listen-port 49997` (two sldl processes in one container can't both bind the default 49998). Without them, `previewQueue` is just `jobQueue` and previews wait behind downloads.

**slskd search** — when `SLSKD_URL` is set, `/preview` (`previewSlskd`) and the download fallback search through slskd (`searchSlskd`) instead of sldl. slskd stays logged in, so a search takes ~4-13s instead of 20-30s of silent sldl login + wait. It ends a search 3s after the last response (`slskdIdleTimeout`), cancels at 8s (`slskdMaxSearch`), then groups files into one result per user folder in sldl's `--print json-all` shape so the UI is unchanged. Quirks: slskd only serves responses once a search ends, reports completion slightly before it stores them (hence the re-read loop), and keeps searches until deleted. slskd logs in as the preview account, so sldl must never use `SLDL_PREVIEW_USER` while slskd runs; the sldl preview path only runs with `SLSKD_URL` unset. The Soulseek server also silently blocks searches containing some artist names ("Frank Ocean"), so `searchSlskdOrAlbum` retries a search nobody answered with only the album title. That retry keeps only results with the artist's name in a file path (`withArtistInPath`), because the bare title also finds other artists' albums holding a song of that name. A folder only counts as a full copy with at most `maxCopyFiles` (2n+2) audio files, in both the UI's auto-pick and `fallbackCandidates`. Soulseek silently drops searches from an account that searched ~10 times in a few minutes, which looks exactly like an empty result; repeated identical searches also get far fewer responses.

**Download lock** — `downloadMu` additionally serializes whole download pipelines (sldl, fallback retry, beets). This is required because the saved album is found by file mtime: if download B's sldl started while A's beets was still writing tags, B's scan would pick up album A. `downloadsPending` counts downloads in or waiting for the pipeline (exposed in `/queue`). Previews don't take the lock.

**Beets retry** — when beets skips an album, `runBeets` strips the files' own MusicBrainz IDs and edition tags (`staleTagStripScript`: media, label, junk years) and imports once more; uploaders' stale IDs otherwise push perfect matches past `strong_rec_thresh`.

**Download jobs** — `POST /download` doesn't stream: it creates a `Job`, starts `runDownload` in a goroutine and returns `{"id"}`. `runDownload` parses sldl output (`Job.sldlLine`, run with `--no-progress`) into per-track states and beets output (`Job.beetsLine`) into a `JobTag` (matched/skipped + reason/error, release, similarity, URL). The UI's Downloads drawer polls `GET /jobs` (no raw logs), fetches `GET /jobs/log?id=` on demand and removes finished jobs via `POST /jobs/dismiss?id=` (`id=finished` for all). Jobs are persisted to `/data/jobs.json` on every status change; anything still active when the server restarts comes back as `interrupted`. The parsers depend on sldl 2.6's output strings (see `sldlTrackRe`, `sldlListedRe`); recheck them when upgrading sldl.

**SSE streaming** — `/preview` and `/retag` go through `serveSSE`, which runs the work in its own goroutine and forwards whatever it `emit`s as `data:` lines. Each write has a 30s deadline. When the client disconnects (or a write times out), `emit` becomes a no-op and the work carries on, so a closed tab or a phone losing signal never stalls the queue.

**Downloads outlive the request** — downloads run with `exec.Command` in a background goroutine, so they finish with no browser attached. Outcomes are also `log.Printf`'d to the container log. `/preview` is tied to the request context and dies with it.

**Endpoints**:
- MusicBrainz calls go through `mbDo`, which retries 503 ("server is currently busy", several times a day) up to 3 times and turns any other non-200 into an error, so the UI shows it instead of "No results." or an empty tracklist
- `GET /search?q=&type=` — proxies MusicBrainz `/recording/` search, or `/release-group/` when `type=album`
- `GET /release?id=` — proxies MusicBrainz release detail (with recordings)
- `GET /release-group?id=` — picks the earliest Official release in the group and returns its detail (with recordings)
- `POST /preview` — with `SLSKD_URL`, searches slskd and emits `[log] N user(s) responded…` progress then `[results]` (see slskd search). Otherwise runs sldl with `--print json-all` (no actual download); extracts the JSON block from mixed output and emits it as a `[results]` SSE event. sldl prints *nothing* when a search is empty, so blank output + exit 0 is sent as `[results] []`; a non-zero exit becomes `[preview-error]` lines. Every preview's outcome is `log.Printf`'d
- `POST /download` — starts a download job (see Download jobs): sldl, then beets on the newly-created album directory. If a `slsk://` URL yields nothing, it removes sldl's `failed/` residue and falls back (see Fallback)
- `GET /retag?path=` — strips promo tags (inline Python/mutagen script) then runs beets; path must be under `/music`
- `GET /queue` — returns `{"queued": N, "downloads": M}` (jobQueue depth, downloads running or waiting). Check `downloads` is 0 before redeploying: a restart kills running downloads
- `GET /jobs`, `GET /jobs/log?id=`, `POST /jobs/dismiss?id=` — downloads drawer API

**Path safety** — `/download` validates `subfolder` doesn't escape `/music`; `/retag` validates `path` prefix against `/music`. Both use `filepath.Clean`.

**Query normalization** — `sanitizeQuery` maps MusicBrainz typography (’ ‐ – … etc.) to ASCII and drops `&`, because Soulseek matches words literally. `slsk://` URLs are passed through untouched: they are exact paths and user names can contain `&`.

**sldl flags** — `buildSldlArgs` always passes `--max-stale-time 600000` (10 min per stalled track; Soulseek peers with queues often deliver eventually), plus `--min-bitrate` when given one.

**Bitrate floor** — downloads use `--min-bitrate 320` by default. `/preview` searches with no floor. The modal (`renderModalResults`) lists every result: first the complete ones whose lowest audio-file bitrate is ≥320 (unknown bitrate passes, like sldl), then the lower-bitrate or incomplete ones under a divider. Without a tracklist, "complete" means at least half the tracks of the largest result (so one song in a compilation folder doesn't count as the album). Each card shows its bitrate or "lossless". Downloading a card under 320 sends `lowBitrate: true`, which drops the floor for that download (not for its fallback). Albums get two buttons: Download (`autoDownload`, see Tracklist matching) and Choose…, which opens the modal to pick a copy by hand.

**Tracklist matching** — for an album the UI sends the MusicBrainz tracklist to `renderModalResults`, which counts per result how many tracks have a file (`countTrackMatches`: `normTitle` keys equal / prefix / suffix, length within 7s, each file once). With a tracklist, "complete" means at least half its tracks, and full matches sort first (320k MP3 before lossless, then fewest extra files). Full copies also sort uploaders with a free slot first. Clicking an album's Download runs the preview in the background (`autoDownload`, the button shows "Searching…") and downloads the best full 320k+ copy without opening the modal; the modal only opens, with that same search's results, when there is no full copy. The same logic exists in Go (`normTitle`, `countTrackMatches`) and in the MCP server (`mcp/server.py`: `_norm_title`, `_count_matches`, `_full_copies`); keep all three in sync.

**Fallback** — the UI sends `fallbackQuery` ("Artist - Album") and `tracks` with every download. The UI also sends `alternatives`: up to 3 other full 320k+ copies from the preview. When a `slsk://` source fails, an album tries `alternatives` first, because repeating the preview's search minutes later gets far fewer responses. Every album source with a tracklist goes through `downloadAlbumSource`: sldl runs with `--failed-album-path disable` so a partial folder stays put, the folder is checked against the tracklist (`localTrackMatches`), and when tracks are missing the same source is run once more (sldl skips files it has, so only the failed ones are fetched); still incomplete, this run's files are removed (`removeNewFiles`) and the next source is tried. If every source fails the job ends `failed`, not "Nothing new was downloaded". With alternatives, `killIfStalled` drops a source that keeps every track waiting (queued behind other users) for 2 min, so it moves on instead of waiting up to 10 min per track. Without them it searches `fallbackQuery` (slskd, or sldl `--print json-all`), keeps only folders with every track and no audio file under the floor (`fallbackCandidates`, failed user excluded) and tries the best 3 (lossy before lossless), always at the 320 floor even for `lowBitrate` downloads. No tracklist or no full match means nothing else is downloaded. A song retries as a name search for `fallbackQuery` at the floor.

**Post-download tagging flow**: after sldl finishes, `handleDownload` walks `outDir` for directories with audio files newer than `job.startTime` (skipping `failed/` subtrees), then passes the first match to `runBeets`. `runBeets` runs `beet import -q <dir>`; `beetsLine` parses both the old "(Similarity: X%)" and the newer "Match (X%):" output: quiet mode, applies confident MusicBrainz matches and otherwise follows `quiet_fallback: skip`. Don't use `-A`; that is `--noautotag`. Files are tagged in place (`move: no`, `copy: no`). In `beets-config.yaml`, `strong_rec_thresh` is a distance (lower = stricter), so keep it near the 0.04 default.

## MCP server

`mcp/server.py` is a stdio MCP server (Python, `mcp==1.26.0` low-level API, plus `mutagen`) that drives this wrapper's HTTP API so a Claude session can search, download and upgrade albums. It runs in the NAS Claude Code container (`/volume1/docker/claude-code/build/mcp/sldl/server.py`, rebuilt into the image), not in this container.

- `search_album`, `find_copies`, `download_album`, `job_status`: thin wrappers over `/search`, `/release-group`, `/preview`, `/download`, `/jobs`. `download_album` prefers a studio Album release group over a same-titled single.
- `upgrade_album` / `finish_upgrade`: replace an album already in the library with a 320k copy. Downloads into `_staging_upgrade/<ts>`, verifies (complete, ≥320k, beets matched), moves old files to a backup outside the library, swaps, rescans Navidrome, then moves play counts, stars, ratings and playlist entries to the new media_file ids by title and deletes the leftover missing rows. Needs `MUSIC_ROOT` (default `/Volumes/MyDrive/Music/Music`), `NAVIDROME_DB` (writable), `NAVIDROME_URL/USERNAME/PASSWORD`, and access to the MyDrive share (the container needs NAS group 101).

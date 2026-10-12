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

Environment variables required: `SLDL_USER`, `SLDL_PASS`.

## Architecture

Everything lives in `main.go`. Key concepts:

**Job queue** — a buffered channel (`jobQueue`, cap 50) serializes all sldl invocations through a single `jobWorker` goroutine. Both `/preview` and `/download` enqueue jobs and wait until the job runs.

**Download lock** — `downloadMu` additionally serializes whole download pipelines (sldl, fallback retry, beets). This is required because the saved album is found by file mtime: if download B's sldl started while A's beets was still writing tags, B's scan would pick up album A. `downloadsPending` counts downloads in or waiting for the pipeline (exposed in `/queue`). Previews don't take the lock.

**SSE streaming** — all long-running endpoints (`/download`, `/preview`, `/retag`) go through `serveSSE`, which runs the work in its own goroutine and forwards whatever it `emit`s as `data:` lines. Each write has a 30s deadline. When the client disconnects (or a write times out), `emit` becomes a no-op and the work carries on, so a closed tab or a phone losing signal never stalls the queue.

**Downloads outlive the request** — `/download` runs sldl with `exec.Command` (not `CommandContext(r.Context())`), so the download, fallback retry and beets all finish even with no browser attached. Outcomes are also `log.Printf`'d to the container log. `/preview` is the exception: it is tied to the request context and dies with it.

**Endpoints**:
- `GET /search?q=&type=` — proxies MusicBrainz `/recording/` search, or `/release-group/` when `type=album`
- `GET /release?id=` — proxies MusicBrainz release detail (with recordings)
- `GET /release-group?id=` — picks the earliest Official release in the group and returns its detail (with recordings)
- `POST /preview` — runs sldl with `--print json-all` (no actual download); extracts the JSON block from mixed output and emits it as a `[results]` SSE event
- `POST /download` — runs sldl to download, then calls beets on the newly-created album directory. If a `slsk://` URL yields nothing, it removes sldl's `failed/` residue and retries as a name search parsed from the URL path
- `GET /retag?path=` — strips promo tags (inline Python/mutagen script) then runs beets; path must be under `/music`
- `GET /queue` — returns `{"queued": N}` (current depth of jobQueue)

**Path safety** — `/download` validates `subfolder` doesn't escape `/music`; `/retag` validates `path` prefix against `/music`. Both use `filepath.Clean`.

**sldl flags** — `buildSldlArgs` always passes `--max-stale-time 600000` (10 min per stalled track; Soulseek peers with queues often deliver eventually), plus `--min-bitrate` when given one.

**Bitrate floor** — downloads use `--min-bitrate 320` by default. `/preview` searches with no floor; the UI (`renderModalResults`) shows only results whose lowest audio-file bitrate is ≥320 (unknown bitrate counts as passing, like sldl), and only if there are none shows the lower ones sorted best-first. Downloading one of those sends `lowBitrate: true`, which drops the floor for that download and its fallback retry.

**Post-download tagging flow**: after sldl finishes, `handleDownload` walks `outDir` for directories with audio files newer than `job.startTime` (skipping `failed/` subtrees), then passes the first match to `runBeets`. `runBeets` runs `beet import -q <dir>`: quiet mode, applies confident MusicBrainz matches and otherwise follows `quiet_fallback: skip`. Don't use `-A`; that is `--noautotag`. Files are tagged in place (`move: no`, `copy: no`). In `beets-config.yaml`, `strong_rec_thresh` is a distance (lower = stricter), so keep it near the 0.04 default.

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

# Cross-compile the linux binary that gets checked in
CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o sldl-wrapper-linux .
```

No test suite exists.

## Runtime dependencies

These are only available inside the container (not locally):

| Dependency | Path/command | Purpose |
|---|---|---|
| `sldl` binary | `/sldl/sldl` | Soulseek downloader |
| `beet` | system PATH | Auto-tagging with config at `/etc/beets/config.yaml` |
| `python3` + `mutagen` | system PATH | Promo-tag stripping |
| Music volume | `/music` | Download destination, path-traversal boundary |

Environment variables required: `SLDL_USER`, `SLDL_PASS`.

## Architecture

Everything lives in `main.go`. Key concepts:

**Job queue** — a buffered channel (`jobQueue`, cap 50) serializes all sldl invocations through a single `jobWorker` goroutine. Both `/preview` and `/download` enqueue jobs and block until the job runs.

**SSE streaming** — all long-running endpoints (`/download`, `/preview`, `/retag`) set `Content-Type: text/event-stream` and flush line-by-line via `http.Flusher`. The `X-Accel-Buffering: no` header disables nginx proxy buffering.

**Endpoints**:
- `GET /search?q=&type=` — proxies MusicBrainz `/recording/` or `/release/` search
- `GET /release?id=` — proxies MusicBrainz release detail (with recordings)
- `POST /preview` — runs sldl with `--print json-all` (no actual download); extracts the JSON block from mixed output and emits it as a `[results]` SSE event
- `POST /download` — runs sldl to download, then calls beets on any newly-created subdirectory
- `GET /retag?path=` — strips promo tags (inline Python/mutagen script) then runs beets; path must be under `/music`
- `GET /queue` — returns `{"queued": N}` (current depth of jobQueue)

**Path safety** — `/download` validates `subfolder` doesn't escape `/music`; `/retag` validates `path` prefix against `/music`. Both use `filepath.Clean`.

**Post-download tagging flow**: after sldl finishes, `handleDownload` walks `outDir` for subdirectories newer than `job.startTime`, then passes the first match to `runBeets`. `runBeets` runs `beet import -A <dir>` (auto-mode, no prompts).

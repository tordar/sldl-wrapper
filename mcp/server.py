#!/usr/bin/env python3
"""MCP server for downloading albums through the sldl-wrapper on the NAS."""

import asyncio
import json
import os
import re
import shutil
import sqlite3
import time
import urllib.parse
import urllib.request

import mutagen

import mcp.server.stdio
import mcp.types as types
from mcp.server import Server
from mcp.server.models import InitializationOptions
from mcp.server.lowlevel import NotificationOptions

WRAPPER_URL = os.environ.get("SLDL_WRAPPER_URL", "http://localhost:6060").rstrip("/")
MIN_BITRATE = 320
AUDIO_RE = re.compile(r"\.(mp3|flac|m4a|aac|ogg|opus|wav|alac|ape|wv)$", re.I)
LOSSLESS_RE = re.compile(r"\.(flac|wav|alac|ape|wv)$", re.I)

# Upgrades need the music share and Navidrome's database as this process sees
# them. The wrapper saves to /music, which is MUSIC_ROOT on this side.
MUSIC_ROOT = os.environ.get("MUSIC_ROOT", "/Volumes/MyDrive/Music/Music").rstrip("/")
WRAPPER_MUSIC_ROOT = "/music"
BACKUP_ROOT = os.environ.get("UPGRADE_BACKUP_ROOT", "/Volumes/MyDrive/.sldl-upgrade-backup")
NAVIDROME_DB = os.environ.get("NAVIDROME_DB", "/navidrome/navidrome.db")
NAVIDROME_URL = os.environ.get("NAVIDROME_URL", "http://localhost:4533").rstrip("/")
NAVIDROME_USER = os.environ.get("NAVIDROME_USERNAME", "")
NAVIDROME_PASS = os.environ.get("NAVIDROME_PASSWORD", "")
STAGING = "_staging_upgrade"

server = Server("sldl")


# ── wrapper API ──────────────────────────────────────────────────────────────

def _get(path: str, timeout: int = 30):
    with urllib.request.urlopen(WRAPPER_URL + path, timeout=timeout) as r:
        return json.load(r)


def _post(path: str, body: dict, timeout: int = 30):
    req = urllib.request.Request(WRAPPER_URL + path, data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.load(r)


def _preview(query: str) -> list:
    """Run /preview (SSE) and return its [results] payload."""
    req = urllib.request.Request(WRAPPER_URL + "/preview",
                                 data=json.dumps({"query": query, "album": True, "subfolder": ""}).encode(),
                                 headers={"Content-Type": "application/json"})
    errors = []
    with urllib.request.urlopen(req, timeout=120) as r:
        for raw in r:
            line = raw.decode("utf-8", "replace").strip()
            if line.startswith("data:"):
                line = line[5:].strip()
            if line.startswith("[results]"):
                return json.loads(line[len("[results]"):].strip() or "[]")
            if line.startswith("[preview-error]"):
                errors.append(line)
    raise RuntimeError("preview returned no results event" + (": " + " ".join(errors) if errors else ""))


# ── album lookup ─────────────────────────────────────────────────────────────

def _search_release_groups(artist: str, album: str) -> list[dict]:
    parts = []
    if album:
        parts.append(f'releasegroup:"{album}"')
    if artist:
        parts.append(f'artist:"{artist}"')
    q = urllib.parse.quote(" AND ".join(parts))
    data = _get(f"/search?q={q}&type=album")
    out = []
    for rg in data.get("release-groups", []):
        credit = "".join(c.get("name", "") + c.get("joinphrase", "") for c in rg.get("artist-credit", []))
        out.append({
            "id": rg["id"],
            "title": rg.get("title"),
            "artist": credit,
            "type": rg.get("primary-type"),
            "secondary_types": rg.get("secondary-types", []),
            "year": (rg.get("first-release-date") or "")[:4],
            "score": rg.get("score"),
        })
    return out


def _pick_release_group(artist: str, album: str) -> dict:
    """Best release group for artist/album, preferring a studio Album over a
    same-titled single (the single's MBID is a common mistag)."""
    groups = _search_release_groups(artist, album)
    if not groups:
        raise ValueError(f"No MusicBrainz release group for {artist} - {album}")
    want = album.lower()

    def rank(g):
        return ((g["title"] or "").lower() == want,
                g["type"] == "Album",
                not g["secondary_types"],
                g["score"] or 0)
    return max(groups, key=rank)


def _tracklist(release_group_id: str) -> tuple[dict, list[dict]]:
    rel = _get(f"/release-group?id={urllib.parse.quote(release_group_id)}")
    tracks = [{"title": t["title"], "length": t.get("length") or 0}
              for m in rel.get("media", []) for t in m.get("tracks", [])]
    return rel, tracks


# ── copy ranking (keep in sync with renderModalResults in static/index.html) ─

def _norm_title(s: str) -> str:
    s = re.sub(r"\([^)]*\)|\[[^\]]*\]", "", s.lower())
    return "".join(ch for ch in s if ch.isalnum())


def _audio_files(result: dict) -> list[dict]:
    return [f for f in result.get("Files") or [] if AUDIO_RE.search(f.get("Filename", ""))]


def _count_matches(result: dict, tracks: list[dict]) -> int:
    files = []
    for f in _audio_files(result):
        name = f.get("Filename", "").replace("\\", "/").split("/")[-1]
        key = re.sub(r"^\d+", "", _norm_title(re.sub(r"\.[^.]+$", "", name)))
        files.append((key, f.get("Length") or 0))
    used, matched = set(), 0
    for t in tracks:
        title = _norm_title(t["title"])
        if not title:
            continue
        want = round((t["length"] or 0) / 1000)
        for i, (key, length) in enumerate(files):
            if i in used:
                continue
            if not (key == title or key.endswith(title) or key.startswith(title)):
                continue
            if want and length and abs(length - want) > 7:
                continue
            used.add(i)
            matched += 1
            break
    return matched


def _min_bitrate(result: dict):
    rates = [f.get("Bitrate") for f in _audio_files(result) if f.get("Bitrate")]
    return min(rates) if rates else None


def _link(result: dict) -> str:
    user = (result.get("User") or {}).get("Username", "?")
    first = (_audio_files(result) or result.get("Files") or [{}])[0].get("Filename", "").replace("\\", "/")
    folder = first[:first.rfind("/")] if "/" in first else ""
    return f"slsk://{user}/{folder.lstrip('/')}"


def _full_copies(results: list, tracks: list[dict]) -> list[dict]:
    """Copies holding every track at 320k+ (or lossless), best first."""
    n = len(tracks)
    full = []
    for r in results:
        files = _audio_files(r)
        rate = _min_bitrate(r)
        if rate is not None and rate < MIN_BITRATE:
            continue
        if not files or len(files) > 2 * n + 2:
            continue
        if _count_matches(r, tracks) != n:
            continue
        full.append(r)
    full.sort(key=lambda r: (
        any(LOSSLESS_RE.search(f.get("Filename", "")) for f in _audio_files(r)),
        not (r.get("User") or {}).get("HasFreeUploadSlot"),
        abs(len(_audio_files(r)) - n),
    ))
    return [{
        "source": _link(r),
        "user": _safe_user((r.get("User") or {}).get("Username")),
        "files": len(_audio_files(r)),
        "bitrate": "lossless" if _min_bitrate(r) is None else _min_bitrate(r),
        "free_slot": bool((r.get("User") or {}).get("HasFreeUploadSlot")),
    } for r in full]


# ── untrusted text ───────────────────────────────────────────────────────────
#
# Soulseek folder names, file names and user names are written by strangers
# and would reach the model verbatim, which runs with bypassed permissions.
# Tool results therefore never contain them raw: copies are handed out as
# opaque ids (the slsk:// paths stay in this process), user names are cut to
# a short charset, and job details are rebuilt from wrapper-generated fields.

_copies: dict[str, dict] = {}
_copy_seq = 0


def _safe_user(name) -> str:
    return re.sub(r"[^A-Za-z0-9_-]", "", name or "")[:30] or "?"


def _safe_path(path: str) -> str:
    return re.sub(r"[^\w .,'()&/-]", "", path or "")[:150]


def _register_copies(copies: list[dict], rg_id: str) -> list[dict]:
    """Hand out ids for copies; each remembers the next three as fallbacks."""
    global _copy_seq
    out = []
    ids = []
    for c in copies:
        _copy_seq += 1
        ids.append(f"copy_{_copy_seq}")
    for i, (cid, c) in enumerate(zip(ids, copies)):
        _copies[cid] = {"source": c["source"], "user": c["user"], "release_group_id": rg_id,
                        "alternatives": [x["source"] for x in copies[i + 1:i + 4]]}
        out.append({"copy": cid, **{k: v for k, v in c.items() if k != "source"}})
    return out


_PHASES = re.compile(
    r"^(Logging in to Soulseek|Searching Soulseek|Reading the uploader's folder|"
    r"Uploader has no free slots, waiting in their queue|Reconnecting to Soulseek|Downloading|"
    r"Matching against MusicBrainz|Nothing new was downloaded|That source failed, looking for another full copy|"
    r"That source failed, searching for others|Trying another source|Cancelling|"
    r"Waiting for \d+ download\(s\) ahead|Cancelled\. Removed \d+ partial file\(s\))$")
_MB_URL = re.compile(r"^https://musicbrainz\.org/release/[0-9a-f-]{36}$")


def _resolve(artist: str, album: str, release_group_id: str | None) -> tuple[str, str, list[dict]]:
    if not release_group_id:
        rg = _pick_release_group(artist, album)
        release_group_id = rg["id"]
    rel, tracks = _tracklist(release_group_id)
    if not tracks:
        raise ValueError(f"Release group {release_group_id} has no tracklist")
    return release_group_id, rel.get("title") or album, tracks


# ── upgrade (replace an album in the library with a 320k copy) ───────────────
#
# The swap keeps Navidrome's history: a file whose path changes gets a new
# media_file id, so play counts, stars, ratings and playlist entries are moved
# from the old rows by title before the leftover missing rows are deleted.

def _nd_call(endpoint: str, **params):
    q = urllib.parse.urlencode(dict(u=NAVIDROME_USER, p=NAVIDROME_PASS, v="1.16.1", c="sldl-mcp", f="json", **params))
    with urllib.request.urlopen(f"{NAVIDROME_URL}/rest/{endpoint}?{q}", timeout=30) as r:
        return json.load(r)["subsonic-response"]


def _nd_scan():
    """Quick scan, waiting for it (and any scan already running) to finish."""
    while _nd_call("getScanStatus")["scanStatus"]["scanning"]:
        time.sleep(2)
    _nd_call("startScan")
    time.sleep(3)
    while _nd_call("getScanStatus")["scanStatus"]["scanning"]:
        time.sleep(2)


def _nd_delete_missing(ids: list[str]):
    if not ids:
        return
    req = urllib.request.Request(f"{NAVIDROME_URL}/auth/login",
                                 data=json.dumps({"username": NAVIDROME_USER, "password": NAVIDROME_PASS}).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        token = json.load(r)["token"]
    for i in range(0, len(ids), 50):
        q = "&".join("id=" + urllib.parse.quote(x) for x in ids[i:i + 50])
        req = urllib.request.Request(f"{NAVIDROME_URL}/api/missing?{q}", method="DELETE",
                                     headers={"X-ND-Authorization": "Bearer " + token})
        urllib.request.urlopen(req, timeout=30).close()


def _db():
    return sqlite3.connect(NAVIDROME_DB, timeout=30)


ANNOTATION_COLS = ("user_id", "play_count", "play_date", "rating", "starred", "starred_at", "rated_at")


def _snapshot(rel_dir: str) -> dict:
    """Annotations of the album's current tracks (by title) and of the album."""
    c = _db()
    prefix = rel_dir.rstrip("/") + "/"
    tracks = {}
    album_ids = set()
    for mf_id, title, album_id in c.execute(
            "select id, title, album_id from media_file where missing = 0 and substr(path, 1, length(?)) = ? "
            "and instr(substr(path, length(?) + 1), '/') = 0", (prefix, prefix, prefix)):
        album_ids.add(album_id)
        rows = c.execute(f"select {', '.join(ANNOTATION_COLS)} from annotation "
                         "where item_type = 'media_file' and item_id = ?", (mf_id,)).fetchall()
        tracks[_norm_title(title)] = {"id": mf_id, "title": title, "rows": rows}
    albums = {}
    for a in album_ids:
        albums[a] = c.execute(f"select {', '.join(ANNOTATION_COLS)} from annotation "
                              "where item_type = 'album' and item_id = ?", (a,)).fetchall()
    c.close()
    return {"tracks": tracks, "albums": albums}


def _restore(rel_dir: str, snap: dict) -> dict:
    c = _db()
    prefix = rel_dir.rstrip("/") + "/"
    new = c.execute("select id, title, album_id from media_file where missing = 0 and substr(path, 1, length(?)) = ? "
                    "and instr(substr(path, length(?) + 1), '/') = 0", (prefix, prefix, prefix)).fetchall()
    carried, unmatched = 0, []
    album_ids = {a for _, _, a in new}
    for mf_id, title, _ in new:
        old = snap["tracks"].get(_norm_title(title))
        if not old:
            continue
        if old["id"] == mf_id:
            continue
        c.execute("update playlist_tracks set media_file_id = ? where media_file_id = ?", (mf_id, old["id"]))
        if not old["rows"]:
            continue
        c.execute("delete from annotation where item_type = 'media_file' and item_id = ?", (mf_id,))
        for row in old["rows"]:
            c.execute(f"insert into annotation (item_id, item_type, {', '.join(ANNOTATION_COLS)}) "
                      f"values (?, 'media_file', {', '.join('?' * len(ANNOTATION_COLS))})", (mf_id, *row))
        carried += 1
    new_titles = {_norm_title(t) for _, t, _ in new}
    unmatched = [v["title"] for k, v in snap["tracks"].items() if k not in new_titles]

    # Album: keep star/rating from the old album row, play count = track sum.
    old_album_rows = {r[0]: r for rows in snap["albums"].values() for r in rows}
    for t in snap["tracks"].values():
        for r in t["rows"]:
            old_album_rows.setdefault(r[0], (r[0], 0, r[2], 0, 0, None, None))
    for album_id in album_ids:
        for user, row in old_album_rows.items():
            total = c.execute("select coalesce(sum(a.play_count), 0) from media_file mf join annotation a "
                              "on a.item_id = mf.id and a.item_type = 'media_file' and a.user_id = ? "
                              "where mf.album_id = ? and mf.missing = 0", (user, album_id)).fetchone()[0]
            vals = dict(zip(ANNOTATION_COLS, row))
            vals["play_count"] = total
            c.execute("delete from annotation where item_type = 'album' and item_id = ? and user_id = ?",
                      (album_id, user))
            c.execute(f"insert into annotation (item_id, item_type, {', '.join(ANNOTATION_COLS)}) "
                      f"values (?, 'album', {', '.join('?' * len(ANNOTATION_COLS))})",
                      (album_id, *[vals[k] for k in ANNOTATION_COLS]))
    c.commit()
    c.close()
    return {"tracks_carried_over": carried, "old_tracks_not_in_new_copy": unmatched}


def _missing_ids(prefixes: list[str]) -> list[str]:
    c = _db()
    ids = []
    for p in prefixes:
        p = p.rstrip("/") + "/"
        ids += [r[0] for r in c.execute("select id from media_file where missing = 1 "
                                        "and substr(path, 1, length(?)) = ?", (p, p))]
    c.close()
    return ids


def _audio_in(path: str) -> list[str]:
    return sorted(os.path.join(path, f) for f in os.listdir(path) if AUDIO_RE.search(f))


def _tags(path: str) -> dict:
    m = mutagen.File(path, easy=True) or {}
    info = mutagen.File(path).info
    first = lambda k: (m.get(k) or [""])[0]
    return {
        "title": first("title"),
        "artist": first("albumartist") or first("artist"),
        "album": first("album"),
        "track": int((first("tracknumber") or "0").split("/")[0] or 0),
        "disc": int((first("discnumber") or "1").split("/")[0] or 1),
        "discs": int(((first("discnumber") or "1/1").split("/") + ["1"])[1] or 1),
        "bitrate": None if LOSSLESS_RE.search(path) else getattr(info, "bitrate", 0) // 1000,
    }


def _safe_name(s: str) -> str:
    return re.sub(r'[\\/:*?"<>|]', "_", s).strip()


def _start_upgrade(args: dict) -> dict:
    rel_dir = args["path"].strip("/")
    if rel_dir.startswith(MUSIC_ROOT.lstrip("/")):
        rel_dir = rel_dir[len(MUSIC_ROOT.lstrip("/")):].strip("/")
    album_dir = os.path.join(MUSIC_ROOT, rel_dir)
    if not os.path.isdir(album_dir) or os.path.realpath(album_dir) == os.path.realpath(MUSIC_ROOT):
        raise ValueError(f"Not an album folder under the music root: {rel_dir}")
    files = _audio_in(album_dir)
    if not files:
        raise ValueError(f"No audio files in {rel_dir}")
    tags = [_tags(f) for f in files]
    low = [os.path.basename(f) for f, t in zip(files, tags) if t["bitrate"] is not None and t["bitrate"] < MIN_BITRATE - 5]
    if not low and not args.get("force"):
        return {"status": "nothing_to_do", "message": f"Every file in {rel_dir} is already {MIN_BITRATE} kbps or lossless."}

    artist = args.get("artist") or tags[0]["artist"]
    album = args.get("album") or tags[0]["album"]
    rg_id, title, tracks = _resolve(artist, album, args.get("release_group_id"))

    # Old tracks the new copy wouldn't have (bonus tracks, deluxe editions).
    want = {_norm_title(t["title"]) for t in tracks}
    dropped = [t["title"] for t in tags if _norm_title(t["title"]) not in want]
    if dropped and not args.get("allow_drop"):
        return {"status": "aborted", "release_group_id": rg_id,
                "message": "The MusicBrainz release has no match for these tracks in the folder, so the swap "
                           "would lose them. Pass a release_group_id for the right edition, or allow_drop=true.",
                "tracks_not_in_release": dropped}

    query = f"{artist} - {title}"
    copies = _full_copies(_preview(query), tracks)
    if not copies:
        return {"status": "aborted", "message": f"No complete {MIN_BITRATE} kbps copy of {query} on Soulseek right now."}
    stage = f"{STAGING}/{int(time.time())}"
    os.makedirs(os.path.join(MUSIC_ROOT, stage), exist_ok=True)
    job = _post("/download", {
        "query": copies[0]["source"],
        "album": True,
        "subfolder": stage,
        "label": f"{artist} – {title} (upgrade)",
        "fallbackQuery": query,
        "tracks": tracks,
        "alternatives": [c["source"] for c in copies[1:4]],
        "expect": len(tracks),
    })
    plan = {"job_id": job["id"], "rel_dir": rel_dir, "stage": stage, "release_group_id": rg_id,
            "title": title, "tracks": tracks, "low_bitrate_files": low}
    with open(os.path.join(MUSIC_ROOT, stage, ".plan.json"), "w") as f:
        json.dump(plan, f)
    return {"status": "downloading", **{k: plan[k] for k in ("job_id", "rel_dir", "release_group_id", "title",
                                                           "low_bitrate_files")},
            "user": copies[0]["user"]}


def _find_plan(job_id: str) -> tuple[str, dict]:
    root = os.path.join(MUSIC_ROOT, STAGING)
    for d in os.listdir(root) if os.path.isdir(root) else []:
        p = os.path.join(root, d, ".plan.json")
        if os.path.exists(p):
            plan = json.load(open(p))
            if plan["job_id"] == job_id:
                return os.path.join(root, d), plan
    raise ValueError(f"No pending upgrade for job {job_id}")


def _job(job_id: str) -> dict:
    data = _get("/jobs")
    for j in data if isinstance(data, list) else data.get("jobs", []):
        if j.get("id") == job_id:
            return j
    raise ValueError(f"No job {job_id}")


def _finish_upgrade(job_id: str, wait_seconds: int) -> dict:
    stage_dir, plan = _find_plan(job_id)
    deadline = time.time() + wait_seconds
    job = _job(job_id)
    while job.get("status") in ("queued", "downloading", "tagging") and time.time() < deadline:
        time.sleep(10)
        job = _job(job_id)
    if job.get("status") in ("queued", "downloading", "tagging"):
        return {"status": "downloading", "job_id": job_id, "phase": job.get("phase"),
                "message": "Still downloading. Call finish_upgrade again later."}

    def abort(msg, **extra):
        shutil.rmtree(stage_dir, ignore_errors=True)
        try:
            os.rmdir(os.path.join(MUSIC_ROOT, STAGING))
        except OSError:
            pass
        _nd_delete_missing(_missing_ids([os.path.relpath(stage_dir, MUSIC_ROOT)]))
        return {"status": "aborted", "message": msg + " The album folder was not touched.", "job": _job_summary(job, False), **extra}

    saved = job.get("saved") or ""
    new_dir = MUSIC_ROOT + saved[len(WRAPPER_MUSIC_ROOT):] if saved.startswith(WRAPPER_MUSIC_ROOT) else ""
    if job.get("status") != "done" or not new_dir or not os.path.isdir(new_dir) \
            or not os.path.realpath(new_dir).startswith(os.path.realpath(stage_dir)):
        return abort(f"Download did not finish cleanly ({job.get('status')}: {job.get('error') or 'no folder saved'}).")
    if (job.get("tag") or {}).get("status") != "matched":
        return abort("Beets could not match the download to MusicBrainz.")

    new_files = _audio_in(new_dir)
    new_tags = [_tags(f) for f in new_files]
    if len(new_files) != len(plan["tracks"]):
        return abort(f"Download has {len(new_files)} audio files, the release has {len(plan['tracks'])}.")
    low = [os.path.basename(f) for f, t in zip(new_files, new_tags) if t["bitrate"] is not None and t["bitrate"] < MIN_BITRATE - 5]
    if low:
        return abort(f"Download has {len(low)} file(s) under 320 kbps.")

    rel_dir = plan["rel_dir"]
    album_dir = os.path.join(MUSIC_ROOT, rel_dir)
    snap = _snapshot(rel_dir)

    backup = os.path.join(BACKUP_ROOT, os.path.basename(stage_dir))
    os.makedirs(backup, exist_ok=True)
    for f in _audio_in(album_dir):
        shutil.move(f, backup)
    multi_disc = max(t["discs"] for t in new_tags) > 1 or len({t["disc"] for t in new_tags}) > 1
    placed = []
    for f, t in zip(new_files, new_tags):
        num = f"{t['disc']}-{t['track']:02d}" if multi_disc else f"{t['track']:02d}"
        name = f"{num} - {_safe_name(t['title'])}{os.path.splitext(f)[1].lower()}"
        shutil.move(f, os.path.join(album_dir, name))
        placed.append(name)
    cover = os.path.join(new_dir, "cover.jpg")
    if os.path.exists(cover) and not os.path.exists(os.path.join(album_dir, "cover.jpg")):
        shutil.move(cover, album_dir)
    shutil.rmtree(stage_dir, ignore_errors=True)

    _nd_scan()
    restored = _restore(rel_dir, snap)
    _nd_delete_missing(_missing_ids([rel_dir, os.path.relpath(stage_dir, MUSIC_ROOT)]))
    shutil.rmtree(backup, ignore_errors=True)
    for d in (BACKUP_ROOT, os.path.join(MUSIC_ROOT, STAGING)):
        try:
            os.rmdir(d)
        except OSError:
            pass
    url = (job.get("tag") or {}).get("url") or ""
    return {"status": "done", "album": rel_dir, "files": placed, "source": _safe_user(job.get("source")),
            "beets_release": url if _MB_URL.match(url) else None, **restored}


# ── tools ────────────────────────────────────────────────────────────────────

@server.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="search_album",
            description="Search MusicBrainz release groups for an album. Returns id, title, artist, type "
                        "(Album/Single/EP) and year. Use the id with find_copies or download_album when "
                        "the name is ambiguous (e.g. a single with the same title as the album).",
            inputSchema={
                "type": "object",
                "properties": {
                    "artist": {"type": "string"},
                    "album": {"type": "string"},
                },
                "required": ["album"],
            },
        ),
        types.Tool(
            name="find_copies",
            description="Search Soulseek for complete copies of an album (every MusicBrainz track, "
                        "320 kbps or lossless), best first. Does not download anything.",
            inputSchema={
                "type": "object",
                "properties": {
                    "artist": {"type": "string"},
                    "album": {"type": "string"},
                    "release_group_id": {"type": "string", "description": "MusicBrainz release group id; "
                                         "picked automatically from artist/album when omitted."},
                    "limit": {"type": "integer", "default": 5},
                },
                "required": ["artist", "album"],
            },
        ),
        types.Tool(
            name="download_album",
            description="Download an album at 320 kbps into the music library (/volume1/MyDrive/Music/Music/"
                        "<artist>/...) and tag it with beets. Picks the best complete copy and passes up "
                        "to 3 others as fallbacks. Returns a job id; follow it with job_status. "
                        "Downloads run in the background and take 1-10 minutes.",
            inputSchema={
                "type": "object",
                "properties": {
                    "artist": {"type": "string"},
                    "album": {"type": "string"},
                    "release_group_id": {"type": "string", "description": "MusicBrainz release group id; "
                                         "picked automatically from artist/album when omitted."},
                    "copy": {"type": "string", "description": "A copy id from find_copies (e.g. 'copy_3') "
                             "to download instead of the best one."},
                    "subfolder": {"type": "string", "description": "Folder under the music root to save into. "
                                  "Default: the artist name."},
                },
                "required": ["artist", "album"],
            },
        ),
        types.Tool(
            name="job_status",
            description="Status of sldl-wrapper download jobs: phase, per-track progress, where it was saved "
                        "and whether beets matched it to MusicBrainz. Without job_id, lists all jobs.",
            inputSchema={
                "type": "object",
                "properties": {
                    "job_id": {"type": "string"},
                },
            },
        ),
        types.Tool(
            name="upgrade_album",
            description="Replace an album already in the library with a fresh 320 kbps copy, keeping its "
                        "Navidrome play counts, stars and ratings. Downloads into a staging folder first; the "
                        "library folder is only touched once the copy is complete, 320 kbps and matched by "
                        "beets. Waits up to wait_seconds for the download, then swaps; if it is still running, "
                        "call finish_upgrade with the job id later. Aborts without changes when the folder "
                        "has tracks the MusicBrainz release lacks (pass allow_drop or a release_group_id).",
            inputSchema={
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Album folder relative to the music root, "
                             "e.g. 'Bombay Bicycle Club/Everything Else Has Gone Wrong'."},
                    "artist": {"type": "string", "description": "Default: the files' album artist tag."},
                    "album": {"type": "string", "description": "Default: the files' album tag."},
                    "release_group_id": {"type": "string"},
                    "force": {"type": "boolean", "default": False,
                              "description": "Upgrade even when every file is already 320 kbps."},
                    "allow_drop": {"type": "boolean", "default": False},
                    "wait_seconds": {"type": "integer", "default": 600},
                },
                "required": ["path"],
            },
        ),
        types.Tool(
            name="finish_upgrade",
            description="Finish an upgrade_album whose download was still running: verify the staged copy, "
                        "swap it into the library and carry over Navidrome play counts.",
            inputSchema={
                "type": "object",
                "properties": {
                    "job_id": {"type": "string"},
                    "wait_seconds": {"type": "integer", "default": 600},
                },
                "required": ["job_id"],
            },
        ),
    ]


def _job_summary(job: dict, tracks: bool) -> dict:
    """Job fields safe to show the model (see "untrusted text")."""
    out = {k: job.get(k) for k in ("id", "status", "created", "finished") if job.get(k)}
    if job.get("label"):
        out["label"] = job["label"]  # set by the client from MusicBrainz data
    phase = job.get("phase") or ""
    if phase:
        out["phase"] = phase if _PHASES.match(phase) else "Working"
    if job.get("source"):
        out["source"] = _safe_user(job["source"])
    if job.get("saved"):
        out["saved"] = _safe_path(job["saved"])
    if job.get("error"):
        out["error"] = re.sub(r"[^A-Za-z0-9 .,+()-]", "", job["error"])[:120]
    if job.get("notes"):
        out["notes"] = len(job["notes"])
    tag = job.get("tag") or {}
    if tag:
        out["tag"] = {k: tag[k] for k in ("status", "similarity", "changes") if k in tag}
        if _MB_URL.match(tag.get("url") or ""):
            out["tag"]["url"] = tag["url"]
    ts = job.get("tracks") or []
    if ts:
        states = {}
        for t in ts:
            states[t.get("state")] = states.get(t.get("state"), 0) + 1
        out["track_states"] = states
        if tracks:
            out["tracks"] = [{"n": i + 1, "state": t.get("state")} for i, t in enumerate(ts)]
    return out


def _call(name: str, args: dict):
    if name == "search_album":
        return _search_release_groups(args.get("artist", ""), args["album"])

    if name == "find_copies":
        rg_id, title, tracks = _resolve(args["artist"], args["album"], args.get("release_group_id"))
        copies = _full_copies(_preview(f"{args['artist']} - {title}"), tracks)
        return {"release_group_id": rg_id, "title": title, "tracks": len(tracks),
                "complete_copies": len(copies),
                "copies": _register_copies(copies[: max(args.get("limit", 5), 4)], rg_id)[: args.get("limit", 5)]}

    if name == "download_album":
        artist = args["artist"]
        rg_id, title, tracks = _resolve(artist, args["album"], args.get("release_group_id"))
        query = f"{artist} - {title}"
        if args.get("copy"):
            picked = _copies.get(args["copy"])
            if not picked:
                return {"error": f"Unknown copy {args['copy']}; ids only live for this session. "
                                 "Run find_copies again."}
        else:
            copies = _full_copies(_preview(query), tracks)
            if not copies:
                return {"error": f"No complete {MIN_BITRATE} kbps copy of {query} on Soulseek right now. "
                                 "Try again later or pick one with find_copies."}
            picked = _copies[_register_copies(copies[:4], rg_id)[0]["copy"]]
        source, alternatives = picked["source"], picked["alternatives"]
        body = {
            "query": source,
            "album": True,
            "subfolder": args.get("subfolder") or artist,
            "label": f"{artist} – {title}",
            "fallbackQuery": query,
            "tracks": tracks,
            "alternatives": alternatives,
            "expect": len(tracks),
        }
        job = _post("/download", body)
        return {"job_id": job["id"], "release_group_id": rg_id, "title": title, "user": picked["user"],
                "fallbacks": len(alternatives)}

    if name == "job_status":
        data = _get("/jobs")
        jobs = data if isinstance(data, list) else data.get("jobs", [])
        if args.get("job_id"):
            for j in jobs:
                if j.get("id") == args["job_id"]:
                    return _job_summary(j, tracks=True)
            return {"error": f"No job {args['job_id']}"}
        return {"queue": _get("/queue"), "jobs": [_job_summary(j, tracks=False) for j in jobs]}

    if name == "upgrade_album":
        started = _start_upgrade(args)
        if started["status"] != "downloading":
            return started
        return _finish_upgrade(started["job_id"], args.get("wait_seconds", 600))

    if name == "finish_upgrade":
        return _finish_upgrade(args["job_id"], args.get("wait_seconds", 600))

    raise ValueError(f"Unknown tool: {name}")


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    try:
        result = await asyncio.to_thread(_call, name, arguments or {})
    except Exception as e:
        result = {"error": f"{type(e).__name__}: {e}"}
    return [types.TextContent(type="text", text=json.dumps(result, indent=2, ensure_ascii=False))]


async def main():
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="sldl",
                server_version="1.0.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )


if __name__ == "__main__":
    asyncio.run(main())

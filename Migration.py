#!/usr/bin/env python3
"""
bulk_weblinks_from_md.py
Create a Weblink object in Capacities for each .md file, using:
  - url = https://www.google.com   (placeholder)
  - titleOverwrite = <file name>
  - mdText = full markdown content of the file (trimmed under 200,000 chars)

Respects the official /save-weblink spec and rate limit (10 req / 60s).
Sleeps between calls based on RateLimit headers when present, or a safe fallback.

.env (place next to this script)
--------------------------------
CAPACITIES_API_KEY=YOUR_TOKEN
SPACE_ID=YOUR_SPACE_UUID
VAULT_PATH=xxx
# Optional:
GLOB=*.md               # glob pattern to pick files (default: *.md)
MAX_FILES=0             # 0 = no limit; use a small number to test
SLEEP_SECONDS=7         # fallback sleep between calls (10/min ~ 6s; 7 is conservative)
ADD_FILENAME_HEADER=true# prepend a small "Imported: <file>" header into mdText
TAGS=                   # optional comma-separated tag names (exact names in Capacities)
DESCRIPTION=            # optional descriptionOverwrite (<=1000 chars)

Usage
-----
pip install requests python-dotenv
python bulk_weblinks_from_md.py
"""

import os
import sys
import time
import json
from pathlib import Path
from typing import Optional, Dict, List

import requests
from dotenv import load_dotenv

API = "https://api.capacities.io"

MDTEXT_CAP = 200_000   # per spec
TITLE_CAP  = 500       # per spec
DESC_CAP   = 1_000     # per spec

def must_env(key: str) -> str:
    v = (os.getenv(key) or "").strip()
    if not v:
        print(f"Missing {key} in .env", file=sys.stderr)
        sys.exit(1)
    return v

def headers(tok: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {tok}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

def rate_sleep(resp: requests.Response, fallback_seconds: int):
    """Sleep using RateLimit-Remaining/Reset if present; otherwise fallback."""
    h = {k.lower(): v for k, v in resp.headers.items()}
    try:
        remaining = int(h.get("ratelimit-remaining", "1") or "1")
    except ValueError:
        remaining = 1
    try:
        reset_sec = int(h.get("ratelimit-reset", str(fallback_seconds)) or str(fallback_seconds))
    except ValueError:
        reset_sec = fallback_seconds

    if remaining <= 0:
        wait = max(reset_sec, fallback_seconds)
        print(f"⏳ Rate limit reached. Sleeping {wait}s…")
        time.sleep(wait)
    else:
        time.sleep(fallback_seconds)

def clamp(s: str, cap: int) -> str:
    if len(s) <= cap:
        return s
    return s[: max(0, cap-1) ] + "…"

def post_weblink(
    session: requests.Session,
    space_id: str,
    url_value: str,
    title: Optional[str],
    md_text: str,
    tags: Optional[List[str]] = None,
    description: Optional[str] = None
) -> requests.Response:
    payload: Dict[str, object] = {
        "spaceId": space_id,
        "url": url_value,
        "mdText": md_text,
    }
    if title:
        payload["titleOverwrite"] = title
    if description:
        payload["descriptionOverwrite"] = description
    if tags:
        payload["tags"] = tags

    r = session.post(f"{API}/save-weblink", json=payload, timeout=60)
    return r

def main():
    load_dotenv()

    token      = must_env("CAPACITIES_API_KEY")
    space_id   = must_env("SPACE_ID")
    vault_path = Path(must_env("VAULT_PATH"))

    glob_pat   = (os.getenv("GLOB") or "*.md").strip()
    max_files  = int(os.getenv("MAX_FILES") or "0")
    sleep_sec  = int(os.getenv("SLEEP_SECONDS") or "7")
    add_header = (os.getenv("ADD_FILENAME_HEADER") or "true").strip().lower() in {"1","true","yes","y"}

    # Optional tags/description
    tags_env = (os.getenv("TAGS") or "").strip()
    tags = [t.strip() for t in tags_env.split(",") if t.strip()] or None
    description = clamp((os.getenv("DESCRIPTION") or "").strip(), DESC_CAP) or None

    if not vault_path.exists():
        print(f"VAULT_PATH does not exist: {vault_path}", file=sys.stderr)
        sys.exit(1)

    s = requests.Session()
    s.headers.update(headers(token))

    files = sorted(vault_path.rglob(glob_pat))
    if max_files and len(files) > max_files:
        files = files[:max_files]

    print(f"Found {len(files)} files in {vault_path} (pattern: {glob_pat}).")
    created = 0
    skipped = 0

    for p in files:
        try:
            raw = p.read_text(encoding="utf-8", errors="ignore")
        except Exception as e:
            print(f"⚠️  Could not read {p}: {e}")
            skipped += 1
            continue

        body = raw.strip()
        if not body:
            print(f"— skip empty: {p.name}")
            skipped += 1
            continue

        # Build mdText (optionally prepend provenance header)
        md_text = body
        if add_header:
            md_text = f"### Imported: {p.name}\n\n" + body

        md_text = clamp(md_text, MDTEXT_CAP)

        # titleOverwrite = filename (without extension), clamped
        title = clamp(p.stem, TITLE_CAP)

        # Use requested placeholder URL
        url_value = "https://www.google.com"

        print(f"→ creating Weblink: title='{title}' file='{p.name}' ({len(md_text)} chars)")
        r = post_weblink(s, space_id, url_value, title, md_text, tags=tags, description=description)
        print(f"  POST /save-weblink -> {r.status_code}")

        if not r.ok:
            # Show a short error snippet, keep going
            print(f"  Error: {r.text[:400]}")
            rate_sleep(r, sleep_sec)
            continue

        try:
            data = r.json()
            wid = data.get("id")
            print(f"  ✓ weblink id: {wid}")
        except Exception:
            print("  ✓ created (no JSON body)")

        created += 1
        rate_sleep(r, sleep_sec)

    print(f"\nDone. Created: {created} | Skipped: {skipped} | From: {vault_path}")

if __name__ == "__main__":
    main()

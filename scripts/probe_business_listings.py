#!/usr/bin/env python3
"""One-task probe of DataForSEO's Business Listings Search.

The question this answers is *coverage*, not price. Business Listings returns
structured organisations — name, address, phone, website, category, coordinates,
opening hours — from DataForSEO's own Google Maps database, with no SERP, no page
fetch and no LLM extraction. If Hungarian community groups are actually in there,
it replaces the expensive half of the pipeline for a large slice of the corpus.
If they are not, it is a directory of businesses and our organic-web route stays
the only way to find a village choir.

One request costs about a cent, so run it before believing either answer.

    DATAFORSEO_LOGIN=... DATAFORSEO_PASSWORD=... \\
      .venv/bin/python scripts/probe_business_listings.py \\
      --lat 47.6667 --lon 19.0833 --radius 10 --categories sports_club,community_center

Coordinates come from CITY_COORDS in scraper/web/app.py, which the city importer
maintains with --write-coords; `--city <name>` looks one up.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import urllib.request

ENDPOINT = "https://api.dataforseo.com/v3/business_data/business_listings/search/live"
CATEGORIES_ENDPOINT = (
    "https://api.dataforseo.com/v3/business_data/business_listings/categories")


def _post(url: str, payload, login: str, password: str) -> dict:
    raw = f"{login}:{password}".encode()
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode() if payload is not None else None,
        headers={
            "Authorization": f"Basic {base64.b64encode(raw).decode()}",
            "Content-Type": "application/json",
        },
        method="POST" if payload is not None else "GET",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode())


def _city_coords(name: str) -> "tuple[float, float] | None":
    """Coordinates from CITY_COORDS in the web app.

    Parsed out of the source rather than imported: `scraper.web.app` builds the
    whole FastAPI application at import time, which a one-shot probe has no
    business doing.
    """
    import ast
    path = os.path.join(os.path.dirname(__file__), "..", "scraper", "web", "app.py")
    with open(path, encoding="utf-8") as fh:
        tree = ast.parse(fh.read())
    for node in tree.body:
        targets = getattr(node, "targets", []) or ([node.target] if hasattr(node, "target") else [])
        if not any(getattr(t, "id", "") == "CITY_COORDS" for t in targets):
            continue
        coords = ast.literal_eval(node.value)
        hit = coords.get(name) or next(
            (v for k, v in coords.items() if k.lower() == name.lower()), None)
        return (float(hit[0]), float(hit[1])) if hit else None
    return None


def _known_categories(login: str, password: str) -> set:
    """Every valid category slug. Free endpoint — no account charge."""
    try:
        data = _post(CATEGORIES_ENDPOINT, None, login, password)
    except Exception as exc:
        print(f"could not load categories ({exc}); skipping the check", file=sys.stderr)
        return set()
    out = set()
    for task in data.get("tasks") or []:
        for result in task.get("result") or []:
            for item in result.get("categories") or []:
                out.add(item if isinstance(item, str) else item.get("category", ""))
    return {c for c in out if c}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--city", help="look coordinates up in CITY_COORDS")
    ap.add_argument("--lat", type=float)
    ap.add_argument("--lon", type=float)
    ap.add_argument("--radius", type=float, default=10.0, help="km (default 10)")
    ap.add_argument("--categories", default="",
                    help="comma-separated, max 10; empty = no category filter")
    ap.add_argument("--title", default="", help="optional name filter")
    ap.add_argument("--limit", type=int, default=20)
    ap.add_argument("--list-categories", action="store_true",
                    help="print the available categories (free) and exit")
    ap.add_argument("--json", action="store_true", help="dump the raw response")
    args = ap.parse_args()

    login = os.environ.get("DATAFORSEO_LOGIN", "")
    password = os.environ.get("DATAFORSEO_PASSWORD", "")
    if not (login and password):
        print("DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD must be set", file=sys.stderr)
        return 2

    if args.list_categories:
        for cat in sorted(_known_categories(login, password)):
            print(cat)
        return 0

    if args.city:
        coords = _city_coords(args.city)
        if not coords:
            print(f"no coordinates for {args.city!r} in CITY_COORDS", file=sys.stderr)
            return 2
        lat, lon = coords
    elif args.lat is not None and args.lon is not None:
        lat, lon = args.lat, args.lon
    else:
        print("give --city, or both --lat and --lon", file=sys.stderr)
        return 2

    # "lat,lon,radius": the radius is whole kilometres, minimum 1. A float
    # renders as "10.0" and the API answers 40501 Invalid Field — which names
    # the field but not what is wrong with it. Latitude and longitude take up
    # to 7 decimals.
    radius = max(1, int(round(args.radius)))
    task: dict = {
        "location_coordinate": f"{lat},{lon},{radius}",
        "limit": args.limit,
    }
    if args.categories:
        wanted = [c.strip() for c in args.categories.split(",") if c.strip()][:10]
        # Checked against the free categories endpoint first. An unknown slug
        # fails the paid request with the same opaque 40501, and guessing slugs
        # from documentation prose is exactly how this probe wasted its first
        # two attempts.
        known = _known_categories(login, password)
        unknown = [c for c in wanted if c not in known] if known else []
        if unknown:
            print(f"unknown categories: {', '.join(unknown)}", file=sys.stderr)
            close = [k for k in known
                     if any(part in k for c in unknown for part in c.split("_"))][:15]
            if close:
                print("did you mean: " + ", ".join(close), file=sys.stderr)
            print("full list: --list-categories", file=sys.stderr)
            return 2
        task["categories"] = wanted
    if args.title:
        task["title"] = args.title

    data = _post(ENDPOINT, [task], login, password)
    if args.json:
        print(json.dumps(data, ensure_ascii=False, indent=2))
        return 0

    if data.get("status_code") not in (20000, 20100):
        print(f"API error {data.get('status_code')}: {data.get('status_message')}",
              file=sys.stderr)
        return 1

    total_cost = 0.0
    shown = 0
    for t in data.get("tasks") or []:
        total_cost += float(t.get("cost") or 0)
        if t.get("status_code") not in (20000, 20100):
            print(f"task error {t.get('status_code')}: {t.get('status_message')}",
                  file=sys.stderr)
            continue
        for result in t.get("result") or []:
            print(f"total matches in database: {result.get('total_count')}")
            for item in result.get("items") or []:
                shown += 1
                print(f"\n{shown}. {item.get('title')}")
                print(f"   category : {item.get('category')}")
                print(f"   address  : {item.get('address')}")
                print(f"   phone    : {item.get('phone') or '—'}")
                print(f"   url      : {item.get('url') or '—'}")
                print(f"   rating   : {(item.get('rating') or {}).get('value') or '—'}")

    # The number that decides whether this is worth building on: what a returned
    # organisation costs compared with the ~$0.001 a community costs today via
    # search + fetch + extraction.
    print(f"\ncost: ${total_cost:.4f} for {shown} listings", end="")
    if shown:
        print(f"  (${total_cost / shown:.6f} each)")
    else:
        print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

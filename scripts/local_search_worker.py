#!/usr/bin/env python3
"""Local browser-driven Google search worker.

Runs on your own machine (residential IP), drives a real Chromium via Playwright to
scrape Google search results, and feeds the URL lists back into the production
`search_cache` through the admin API. The production pipeline then finds the search
pre-populated and skips its own (datacenter-IP, CAPTCHA-prone) search step.

It reuses the project's `GooglePlaywrightSearchClient`, so consent handling, CAPTCHA
detection, result scraping, and the 8 s inter-request spacing all come for free.

Setup (once, on this machine):
    pip install -e ".[dev]"           # or at least: pip install playwright httpx
    playwright install chromium

Run:
    PYTHONPATH=. python scripts/local_search_worker.py \
        --base-url https://kozossegek.com \
        --admin-user admin --admin-password "$ADMIN_PASSWORD" \
        --worker-token "$SEARCH_WORKER_TOKEN" \
        --country Hungary --headful

Env fallbacks: ADMIN_USER, ADMIN_PASSWORD, SEARCH_WORKER_TOKEN, WORKER_BASE_URL.

The server side needs `SEARCH_WORKER_TOKEN` set (same value) or the ingest endpoint
is disabled. See docs/wiki/pages/operations/local-search-worker.md.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import random
import sys

import httpx

# Import the project's search client (run with PYTHONPATH=. from the repo root).
_MAC_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

try:
    from scraper.search import GooglePlaywrightSearchClient, SearchQuotaError
except ImportError:
    sys.stderr.write(
        "Could not import scraper.search — run from the repo root with PYTHONPATH=.\n"
    )
    raise


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--base-url", default=os.environ.get("WORKER_BASE_URL", ""),
                   help="Admin base URL, e.g. https://kozossegek.com")
    p.add_argument("--admin-user", default=os.environ.get("ADMIN_USER", "admin"))
    p.add_argument("--admin-password", default=os.environ.get("ADMIN_PASSWORD", ""))
    p.add_argument("--worker-token", default=os.environ.get("SEARCH_WORKER_TOKEN", ""))
    p.add_argument("--country", default="", help="Only search cities in this country (e.g. Hungary)")
    p.add_argument("--batch", type=int, default=25, help="Jobs to pull per API request")
    p.add_argument("--max-jobs", type=int, default=0, help="Stop after N jobs total (0 = until none left)")
    p.add_argument("--num-results", type=int, default=10, help="Results per query")
    p.add_argument("--min-delay", type=float, default=8.0, help="Min seconds between searches")
    p.add_argument("--max-delay", type=float, default=20.0, help="Max seconds between searches (jitter)")
    p.add_argument("--captcha-cooldown", type=float, default=900.0,
                   help="Seconds to wait after a CAPTCHA in headless mode")
    p.add_argument("--headful", action="store_true",
                   help="Show the browser (lets you solve a CAPTCHA by hand)")
    p.add_argument("--user-data-dir", default=os.path.expanduser("~/.cs_search_profile"),
                   help="Persistent Chrome profile dir (keeps consent/login cookies to dodge CAPTCHA)")
    p.add_argument("--browser-locale", default="en-US",
                   help="Browser context locale, match the search country (e.g. sv-SE for Sweden)")
    p.add_argument("--warmup", action="store_true",
                   help="Open the persistent profile on google.com and wait, so you can accept "
                        "consent / sign into Google once. Do this before unattended runs.")
    p.add_argument("--once", action="store_true", help="Process a single batch then exit")
    args = p.parse_args()
    if args.warmup:
        return args  # warmup only needs the browser flags
    if not args.base_url:
        p.error("--base-url (or WORKER_BASE_URL) is required")
    if not args.admin_password:
        p.error("--admin-password (or ADMIN_PASSWORD) is required")
    if not args.worker_token:
        p.error("--worker-token (or SEARCH_WORKER_TOKEN) is required")
    args.base_url = args.base_url.rstrip("/")
    return args


async def _fetch_jobs(api: httpx.AsyncClient, base_url: str, batch: int, country: str) -> list[dict]:
    r = await api.get(
        f"{base_url}/admin/api/search/jobs",
        params={"limit": batch, "country": country},
    )
    r.raise_for_status()
    return r.json().get("jobs", [])


async def _ingest(api: httpx.AsyncClient, base_url: str, job: dict, urls: list[str]) -> None:
    r = await api.post(
        f"{base_url}/admin/api/search/ingest",
        json={"city": job["city"], "topic": job["topic"],
              "queries": job["queries"], "urls": urls},
    )
    r.raise_for_status()


async def _handle_captcha(client: GooglePlaywrightSearchClient, args: argparse.Namespace) -> None:
    # Manual solve only makes sense with a visible browser AND an interactive
    # terminal to press Enter in. When run non-interactively (e.g. by an agent /
    # background job) stdin is not a TTY, so fall back to a cooldown instead of
    # crashing on EOFError.
    if args.headful and sys.stdin is not None and sys.stdin.isatty():
        print("\n⚠️  CAPTCHA detected. Solve it in the visible browser window, "
              "then press Enter here to continue…")
        try:
            await asyncio.get_event_loop().run_in_executor(None, input)
            client._consent_done = False  # re-accept consent if the page changed
            return
        except EOFError:
            pass
    print(f"⚠️  CAPTCHA detected. Cooling down {args.captcha_cooldown:.0f}s "
          "(run in an interactive terminal with --headful to solve it by hand).")
    await asyncio.sleep(args.captcha_cooldown)


async def _warmup(args: argparse.Namespace) -> int:
    """Open the persistent profile on google.com and wait, so the human can accept
    consent / sign into Google. Seeds ~/.cs_search_profile for unattended runs."""
    client = GooglePlaywrightSearchClient(
        rate_limit_seconds=1.0, headless=False,
        user_data_dir=args.user_data_dir, context_locale=args.browser_locale,
        user_agent=_MAC_UA,
    )
    await client.start()
    if client._context is None:
        sys.stderr.write("Failed to start Chromium.\n")
        return 1
    page = await client._context.new_page()
    await page.goto("https://www.google.com", wait_until="domcontentloaded")
    print(f"\nProfile: {args.user_data_dir}\n"
          "Accept the cookie consent, run a search or two, and (recommended) sign into a\n"
          "Google account in this window. Then press Enter here to save & close.")
    try:
        await asyncio.get_event_loop().run_in_executor(None, input)
    except EOFError:
        print("(no interactive terminal — run --warmup from your own shell)")
    await client.stop()
    print("Warmup done — profile saved.")
    return 0


async def run(args: argparse.Namespace) -> int:
    if args.warmup:
        return await _warmup(args)

    auth = httpx.BasicAuth(args.admin_user, args.admin_password)
    headers = {"X-Worker-Token": args.worker_token}
    processed = 0
    total_urls = 0

    client = GooglePlaywrightSearchClient(
        rate_limit_seconds=args.min_delay, headless=not args.headful,
        user_data_dir=args.user_data_dir, context_locale=args.browser_locale,
        user_agent=_MAC_UA,
    )
    await client.start()
    if client._context is None:  # persistent mode leaves _browser None; _context is the signal
        sys.stderr.write("Failed to start Chromium — is `playwright install chromium` done?\n")
        return 1

    try:
        async with httpx.AsyncClient(auth=auth, headers=headers, timeout=60.0) as api:
            while True:
                jobs = await _fetch_jobs(api, args.base_url, args.batch, args.country)
                if not jobs:
                    print(f"No more jobs. Processed {processed} pairs, {total_urls} URLs.")
                    break

                for job in jobs:
                    label = f"{job['city']} / {job['topic']}"
                    while True:
                        try:
                            results = await client.search_all(
                                job["queries"], locale=job.get("locale", "en"),
                                num_results=args.num_results,
                            )
                            break
                        except SearchQuotaError:
                            await _handle_captcha(client, args)
                            # retry the same job after the cooldown / manual solve

                    urls = list(dict.fromkeys(r.url for r in results))
                    await _ingest(api, args.base_url, job, urls)
                    processed += 1
                    total_urls += len(urls)
                    print(f"[{processed}] {label}: {len(urls)} URLs")

                    if args.max_jobs and processed >= args.max_jobs:
                        print(f"Reached --max-jobs={args.max_jobs}. Stopping.")
                        return 0

                    # jittered delay between pairs on top of the client's own spacing
                    await asyncio.sleep(random.uniform(args.min_delay, args.max_delay))

                if args.once:
                    print(f"--once: done. Processed {processed} pairs, {total_urls} URLs.")
                    break
    finally:
        await client.stop()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(run(_parse_args())))
    except KeyboardInterrupt:
        print("\nInterrupted.")
        raise SystemExit(130)

#!/usr/bin/env python3
"""Ask every configured provider which models it actually serves today.

Why this exists
---------------
On 2026-08-16 the router went live with a catalogue written from vendor docs and
round-up articles. Within hours, every single model name was wrong: Groq had
deprecated two, Gemini had closed 2.5-flash to new projects, all three
OpenRouter ":free" slugs had left the free tier, and GitHub Models answered 410
because the service is being retired. Free-tier line-ups move weekly.

So do not read docs — ask the APIs. Each provider exposes an OpenAI-style
`GET /models`; this compares that live list against `config/providers.yaml` and
reports three things:

  UNLISTED  configured here, absent from the provider's /models list
  NEW       listed upstream, not configured  -> candidate to add
  OK        configured and listed

UNLISTED is a *hint*, not a verdict. On 2026-08-16 `open-mistral-nemo` was
absent from Mistral's list and answered requests perfectly — providers keep
serving aliases and legacy names they no longer advertise. Only a real call
settles it, which is what the router's preflight already does.

Usage
-----
    python scripts/check_free_models.py                 # local keys
    python scripts/check_free_models.py --remote        # via the deployed gateway
    python scripts/check_free_models.py --json          # machine-readable

`--remote` reads `ROUTER_BASE_URL` (default https://kozossegek.com) and
`ROUTER_API_KEY`, and asks the running app — the only place the provider keys
actually live. Without it, the script needs the provider keys in the local env.

Exit code is 1 when anything configured is missing upstream, so this can gate a
scheduled check.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scraper.providers import fetch_upstream_models, load_catalogue  # noqa: E402

# There was a `_LIST_PATH` dict here that looked like a dispatch table for
# providers whose /models is not OpenAI-shaped. Nothing ever read it — the real
# dispatch is the `if spec.name == ...` chain in `providers.fetch_upstream_models`
# — and on 2026-09-05 it was dutifully extended for Cloudflare, which of course
# changed nothing. Deleted rather than wired up: a second place to register a
# provider is how the Cloudflare bug happened in the first place.

TIMEOUT = 25

#: Cloudflare fronts the deployed app and 403s urllib's default agent.
_UA = "meetapedia-model-check/1.0"


def _get_json(url: str, headers: dict) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": _UA, **headers})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
        return json.load(resp)


def live_models(spec) -> tuple[list[str], str | None]:
    """(model ids, error), delegated to `scraper.providers`.

    This used to be a second copy of the same logic, and on 2026-09-05 that
    cost exactly what a duplicate costs: Cloudflare's native model endpoint was
    taught to this copy only, so `--remote` — which asks the deployed app, and
    is the *normal* way to run this check because the keys live there — still
    hit the OpenAI-compat /models and reported HTTP 405 for a provider that was
    working fine. One implementation, used by both paths.
    """
    return fetch_upstream_models(spec, timeout=TIMEOUT)


def remote_report(base: str, token: str) -> list[dict]:
    """Per-provider upstream/configured diff, computed on the server.

    Asks /v1/models/upstream, not /v1/models: the latter lists what the router
    can route to *today*, which omits a provider whose daily budget is spent or
    one parked behind allow_paid — and reading that as "the models are gone"
    produced exactly that false alarm on 2026-08-16.
    """
    data = _get_json(f"{base.rstrip('/')}/v1/models/upstream",
                     {"Authorization": f"Bearer {token}"})
    return data.get("data", [])


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--remote", action="store_true",
                    help="ask the deployed gateway instead of using local keys")
    ap.add_argument("--json", action="store_true", dest="as_json")
    ap.add_argument("--free-only", action="store_true",
                    help="only report upstream models whose id ends in :free")
    args = ap.parse_args()

    catalogue = load_catalogue()
    report: dict[str, dict] = {}

    if args.remote:
        base = os.environ.get("ROUTER_BASE_URL", "https://kozossegek.com")
        token = os.environ.get("ROUTER_API_KEY", "")
        if not token:
            raise SystemExit("--remote needs ROUTER_API_KEY in the environment")
        try:
            rows = remote_report(base, token)
        except Exception as exc:
            raise SystemExit(f"gateway unreachable: {exc}") from exc
        for r in rows:
            upstream = r["upstream"]
            if args.free_only:
                upstream = [m for m in upstream if m.endswith(":free")]
            report[r["provider"]] = {
                "enabled": r["enabled"],
                "error": r["error"],
                "configured": r["configured"],
                "gone": r["gone"],
                "new": [m for m in r["new"] if not args.free_only or m.endswith(":free")],
                "upstream_count": len(upstream),
            }
    else:
        for spec in catalogue.providers:
            configured = [m.model for m in spec.models]
            upstream, err = live_models(spec)
            if args.free_only:
                upstream = [m for m in upstream if m.endswith(":free")]
            report[spec.name] = {
                "enabled": spec.enabled,
                "error": err,
                "configured": configured,
                "gone": [m for m in configured if m not in upstream] if upstream else [],
                "new": [m for m in upstream if m not in configured],
                "upstream_count": len(upstream),
            }

    if args.as_json:
        print(json.dumps(report, indent=2, ensure_ascii=False))
    else:
        for name, r in report.items():
            flag = "" if r["enabled"] else "  (disabled in config)"
            print(f"\n== {name}{flag} ==")
            if r["error"]:
                print(f"   ! {r['error']}")
                continue
            print(f"   upstream models: {r['upstream_count']}")
            for m in r["gone"]:
                print(f"   UNLISTED  {m}   <- not in the provider's list; verify with a call")
            if r["new"]:
                shown = r["new"][:12]
                for m in shown:
                    print(f"   NEW   {m}")
                if len(r["new"]) > len(shown):
                    print(f"   ...   +{len(r['new']) - len(shown)} more")
            if not r["gone"] and not r["new"]:
                print("   all configured models present")

    unlisted = {n: r["gone"] for n, r in report.items()
                if r["enabled"] and r["gone"]}
    if unlisted:
        print("\nConfigured but not listed upstream:")
        for n, ms in unlisted.items():
            print(f"  {n}: {', '.join(ms)}")
        print("\nThis is a hint, not proof. Providers serve unlisted aliases —"
              "\nverify before editing anything:")
        first = next(iter(unlisted.items()))
        print(f'  curl -X POST -H "Authorization: Bearer $ROUTER_API_KEY" \\\n'
              f'    -H "Content-Type: application/json" \\\n'
              f'    -d \'{{"model":"{first[0]}:{first[1][0]}",'
              f'"messages":[{{"role":"user","content":"hi"}}],"max_tokens":5}}\' \\\n'
              f'    https://kozossegek.com/v1/chat/completions')
        print("\nIf the call fails too, fix config/providers.yaml in the "
              "repository and deploy. config/ is not a persisted volume: an "
              "edit made on the server is lost at the next deploy.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

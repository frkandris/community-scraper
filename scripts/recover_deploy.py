#!/usr/bin/env python3
"""Restore a lost Traefik route by forcing a Coolify redeploy.

The site has now gone down twice the same way: the container is up and Coolify
reports `running:healthy`, but Traefik holds no route for it, so every visitor
gets a bare 404. The remediation has never varied — force a redeploy, not a
restart — and it has always been performed by a human noticing.

Detection without action is only half a monitor. Synthetic monitoring shifts the
goal from mean-time-between-failures to mean-time-to-recovery
(martinfowler.com/bliki/SyntheticMonitoring.html), and a recovery a script can
perform should not wait for someone to read an email.

Deliberately conservative:
  * it only redeploys when the site is actually unreachable from outside;
  * it never loops — one attempt, then it reports and exits;
  * it exits non-zero even after a successful recovery, so the incident is
    still visible instead of being silently absorbed.

Standard library only, so this cannot fail for reasons unrelated to the site.
"""
from __future__ import annotations

import argparse
import json
import os
import time
import urllib.error
import urllib.request

DEFAULT_PROBE = "https://kozossegek.com/healthz"

#: Cloudflare answers 403 to a request with no User-Agent, and a watchdog that
#: reads that as "the site is down" would redeploy in a loop forever. The same
#: header the smoke test sends.
_UA = "meetapedia-recovery/1.0 (+https://github.com/frkandris/meetapedia)"


def _reachable(url: str, timeout: float = 20.0) -> tuple[bool, str]:
    req = urllib.request.Request(url, headers={"User-Agent": _UA})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read(400).decode("utf-8", "replace")
            return resp.status == 200 and '"ok":true' in body, f"{resp.status}"
    except urllib.error.HTTPError as exc:
        return False, f"HTTP {exc.code}"
    except Exception as exc:  # DNS, TLS, timeout — all "not reachable"
        return False, type(exc).__name__


def _deploy(base: str, token: str, uuid: str) -> str:
    url = f"{base.rstrip('/')}/api/v1/deploy?uuid={uuid}&force=true"
    req = urllib.request.Request(url, method="POST",
                                 headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = json.loads(resp.read().decode("utf-8", "replace"))
    deployments = data.get("deployments") or []
    return deployments[0].get("deployment_uuid", "?") if deployments else "?"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--probe", default=os.environ.get("SMOKE_PROBE_URL", DEFAULT_PROBE))
    ap.add_argument("--wait", type=int, default=420,
                    help="seconds to wait for the site after redeploying")
    args = ap.parse_args()

    base = os.environ.get("COOLIFY_URL", "")
    token = os.environ.get("COOLIFY_TOKEN", "")
    uuid = os.environ.get("COOLIFY_APP_UUID", "")

    ok, why = _reachable(args.probe)
    if ok:
        print(f"{args.probe} is reachable ({why}) — nothing to recover.")
        return 0

    print(f"{args.probe} is NOT reachable ({why}).")
    if not (base and token and uuid):
        missing = [n for n, v in (("COOLIFY_URL", base), ("COOLIFY_TOKEN", token),
                                  ("COOLIFY_APP_UUID", uuid)) if not v]
        print(f"Cannot recover automatically: {', '.join(missing)} not set.")
        print("Set them as repository secrets to let this run unattended.")
        return 1

    try:
        deployment = _deploy(base, token, uuid)
    except Exception as exc:
        print(f"Redeploy request failed: {type(exc).__name__}: {exc}")
        return 1
    print(f"Forced redeploy queued ({deployment}). Waiting up to {args.wait}s…")

    deadline = time.monotonic() + args.wait
    while time.monotonic() < deadline:
        time.sleep(15)
        ok, why = _reachable(args.probe)
        if ok:
            elapsed = int(args.wait - (deadline - time.monotonic()))
            print(f"Site is back after ~{elapsed}s.")
            # Non-zero on purpose: the site went down, and a green run would
            # hide that. Recovery is not the same as nothing having happened.
            return 2
    print(f"Still unreachable after {args.wait}s ({why}). Needs a human.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

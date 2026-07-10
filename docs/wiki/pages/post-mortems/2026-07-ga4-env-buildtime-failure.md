---
type: Post-mortem
title: 2026-07 GA4 Env Var Broke the Docker Build
description: A multiline JSON secret marked "Available at Buildtime" in Coolify was injected as a Dockerfile ARG and broke the build parse; runtime-only env vars fixed it.
tags: [post-mortem, coolify, docker, env-vars, secrets, ga4]
timestamp: 2026-07-10
resource: scraper/report.py
---

# 2026-07 GA4 Env Var Broke the Docker Build

*Deployment failed in 10 seconds after adding `GA4_CREDENTIALS_JSON` — Coolify wrote
the multiline service-account JSON into the generated Dockerfile as an `ARG`.*

## Symptom

Manual redeploy on 2026-07-09 20:13 UTC failed after ~10s with:

```
ERROR: failed to build: failed to solve: failed to process
"=\\n-----END PRIVATE KEY-----\\n\",": unexpected end of statement
while looking for matching double-quote
```

The build log also showed `SecretsUsedInArgOrEnv` warnings and the JSON (with the
private key) partially echoed into `Dockerfile:11`.

## Root cause

Coolify env vars created with **"Available at Buildtime"** checked are injected into
the generated Dockerfile as `ARG NAME=value` lines. A multiline JSON value full of
quotes and `\n` sequences is not a valid single-line ARG, so the Dockerfile no longer
parses. Two extra harms: the secret would be baked into build logs/image layers, and
the app only reads it at runtime anyway ([[ga4-reporting]]).

A second, unrelated find from the same investigation: Coolify **fails a webhook
deploy that arrives while another deploy is running** (the 18:55 webhook for the GA4
commit died against a concurrent manual deploy), so "green latest deploy" must be
checked per-commit — the GA4 code was not actually live until 20:17.

## Fix

Unchecked "Available at Buildtime" (runtime-only) for `GA4_CREDENTIALS_JSON` and
`GA4_PROPERTY_ID` in Coolify → redeploy succeeded (2026-07-09 20:17 UTC).

## Lessons

- Secrets — especially multiline ones — must be **runtime-only** env vars in Coolify
  unless the Dockerfile genuinely consumes them at build time.
- After overlapping deploys, verify WHICH commit is live (deployments list per
  commit), not just that the app is healthy — see [[deployment-coolify]].

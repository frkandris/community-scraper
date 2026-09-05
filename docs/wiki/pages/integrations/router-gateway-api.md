---
type: Integration
title: Router Gateway API (/v1)
description: OpenAI-compatible HTTP endpoint that routes any chat completion across the free-tier provider fleet — usable from any project with an existing OpenAI client.
tags: [api, integration, llm, openai-compatible, gateway]
timestamp: 2026-08-16
resource: scraper/web/api.py
---

# Router Gateway API (/v1)

*A general-purpose LLM endpoint: send OpenAI-shaped chat completions, get one back, and let the router pick a free model that still has quota.*

**This is not a community/scraper API.** Nothing from this project — no prompt,
schema, persona or filter — is added to your request. Your `messages` go
upstream exactly as sent. Anything an LLM can do, this endpoint can do.

Base URL: `https://meetapedia.com/v1` (or `https://kozossegek.com/v1` — same app).

## Quick start

Any OpenAI client works; change `base_url` and `api_key`, nothing else.

```python
from openai import OpenAI

client = OpenAI(base_url="https://meetapedia.com/v1", api_key="YOUR_ROUTER_KEY")

resp = client.chat.completions.create(
    model="auto",                       # let the router choose
    messages=[{"role": "user", "content": "Summarise this in one sentence: ..."}],
    temperature=0.2,
)
print(resp.choices[0].message.content)
```

```javascript
import OpenAI from "openai";
const client = new OpenAI({ baseURL: "https://meetapedia.com/v1", apiKey: process.env.ROUTER_KEY });
const r = await client.chat.completions.create({ model: "auto", messages: [...] });
```

```bash
curl https://meetapedia.com/v1/chat/completions \
  -H "Authorization: Bearer $ROUTER_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model":"auto","messages":[{"role":"user","content":"hello"}]}'
```

## Authentication

`Authorization: Bearer <key>`. Keys come from the `ROUTER_API_KEY` env var,
comma-separated so each consumer gets its own and can be revoked alone:

```
ROUTER_API_KEY=sk-app-crm,sk-app-billing,sk-laptop-scratch
```

**Unset `ROUTER_API_KEY` disables the gateway** — every request 401s. An
unauthenticated LLM proxy is a free-credit faucet for whoever finds it, so
absent configuration closes the door rather than opening it.

**A weak key is the same faucet with an extra step.** On 2026-09-05 the live
value carried two keys, and the first was the literal string `ROUTER_API_KEY`
repeated three times. This repository is public and that name appears in
`CLAUDE.md`, `AGENTS.md`, this page and `scraper/web/api.py`, so the key was
derivable from our own documentation. It authenticated against
`/v1/chat/completions` — which spends the free-tier budget the crawler depends
on — and, because `CONTROL_API_KEY` is unset and `_control_keys()` falls back to
the gateway keys, against `/v1/control/*`, which starts and stops pipeline runs.
Removed the same day. Two things follow: keys go in as random strings, and
**deleting one in Coolify does not revoke it** — the container keeps the old
environment until a redeploy, which is what actually took it out of service.

## Endpoints

### `POST /v1/chat/completions`

Standard OpenAI request body. Forwarded fields: `temperature`, `top_p`,
`max_tokens`, `max_completion_tokens`, `stop`, `presence_penalty`,
`frequency_penalty`, `seed`, `n`, `response_format`, `tools`, `tool_choice`,
`user`. Anything else is dropped — an unknown field is a 400 at several
providers, and the failover chain would then retry it against all of them.

The `model` field selects a **routing policy**:

| Value | Behaviour |
|---|---|
| `auto` (or omitted) | Best-quality model that still has quota. The normal choice. |
| `groq` | Best model on that provider. |
| `groq:qwen3-32b` | Exactly that model. No substitution — if it is out of quota you get a 429, not a different model quietly answering. |
| `qwen3-32b` | That model wherever it lives. |

Response is the upstream provider's body, unmodified, plus one additive field:

```json
{
  "id": "chatcmpl-…",
  "choices": [{"index": 0, "message": {"role": "assistant", "content": "…"}, "finish_reason": "stop"}],
  "usage": {"prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7},
  "x_router": {"provider": "groq", "model": "openai/gpt-oss-120b", "quality": 62, "requested": "auto"}
}
```

`x_router` tells you what actually served the request. OpenAI clients ignore
unknown top-level fields, so it is safe to leave in place.

**Streaming is not supported.** `stream: true` returns a 400 rather than a
silent non-streaming body, which would hang a client waiting for SSE frames.

**Limits:** 60 messages and 200,000 characters per request.

### `GET /v1/models`

Models that can serve a request *right now* — a spent provider disappears from
the list, so this reflects capacity, not just configuration. `auto` is listed
first. Non-standard `quality` field carries the routing score.

### `GET /v1/models/upstream`

Non-standard. Asks each configured provider for its own model list and diffs it
against `config/providers.yaml`, returning `configured` / `upstream` / `gone` /
`new` per provider.

Distinct from `/v1/models` on purpose: that one answers "what can the router use
today" and therefore hides a provider whose daily budget is spent or one parked
behind `allow_paid`. Only this route can answer "did a model disappear" or "is
there a new free model", and only the server can ask it, because the provider
keys live there. A provider that could not be reached reports an `error` and no
`gone` entries.

### `POST /v1/score`

Non-standard. Measures every routed model against a golden set drawn from our
own cached pages and returns the scores. `?pages=N` (default 8, max 40),
`?provider=groq` to narrow it.

**Read-only** — it never writes `config/providers.yaml`. Applying a result is a
deliberate edit; a bad golden set silently rewriting the routing order is the
failure worth keeping manual.

Costs one LLM call per model per page — ~96 calls for a 12-model fleet at the
default, out of the same daily budget the crawler uses. Check `GET /v1/quota`
first.

Note what the numbers mean: **agreement with the incumbent extraction, not
ground truth.** The expected names come from whichever model processed each page
before, so a model that finds a real club the incumbent missed is scored down
for it. The right question it answers is "can this free model replace the one
that produced our corpus".

### `GET /v1/logs`

Non-standard. Recent application log lines from the in-memory ring the admin log
page streams. `?lines=N` (max 1000), `?level=warning`, `?grep=substring` (a plain
substring, not a regex — an operator-supplied regex is an easy accidental
catastrophic backtrack).

Exists so production can be debugged the way `/healthz` is read, without a
platform login. **It needs a running app**, so it cannot explain a container that
fails to start — for that the Coolify deployment log remains the only source.

### `GET /v1/quota`

Non-standard. Today's per-provider budget: `budget`, `used`, `remaining`,
`blocked`. Poll this to back off *before* being rate limited rather than after.

## Errors

OpenAI's envelope: `{"error": {"message", "type", "param", "code"}}`.

| Status | `code` | Meaning |
|---|---|---|
| 401 | `invalid_api_key` | Bad or missing bearer token, or the gateway is unconfigured. |
| 400 | `messages` / `stream` | Malformed request; the message says what. |
| 404 | `model_not_found` | You named a model that is not in the catalogue. |
| 429 | `quota_exhausted` | Everything that could serve you is out of daily budget. |
| 429 | `rate_limited` | All candidates are inside a 429 back-off window. |
| 502 | `upstream_unavailable` | No provider could answer; the message names the last error. |
| 503 | `router_disabled` | `router.enabled` is off, or no provider has a key. |

Retry 429 and 502 with backoff; the router will pick a different provider once
one frees up. 400/404 are your bug — retrying will not help.

## Sharing a budget with the crawler

Gateway calls hit the **same** per-provider daily ledger the extraction pipeline
uses ([[free-tier-model-router]]). Heavy external traffic will reduce what the
nightly crawl can do. `GET /v1/quota` is the place to check before a large job;
`/admin/providers` shows the same numbers with a UI
([[ai-provider-quota-runbook]]).

There is no per-key quota yet. If one consumer starts starving the others, that
is the feature to add — the ledger already tracks per-provider spend, so it
would be a per-key column, not a redesign.

## Guarantees and non-guarantees

**Guaranteed:** the OpenAI request/response shape, the error envelope, the
`model` selection semantics above, and that your messages reach the provider
unmodified.

**Not guaranteed:** which model answers when you ask for `auto` (that is the
point), latency (free tiers vary widely), or that any specific provider stays in
the catalogue — model names move, and `config/providers.yaml` moves with them.
Pin `provider:model` if you need reproducibility, and handle the 404 that
follows if it is ever retired.

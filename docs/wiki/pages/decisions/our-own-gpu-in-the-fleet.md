---
type: Decision
title: Our Own GPU in the Fleet
description: A laptop running Qwen3-8B scores 67 on our task against the free fleet's 52-80, and its allowance never runs out — which is the hole it fills, not a better answer.
tags: [providers, router, local-inference, llama-cpp, quota, measurement]
timestamp: 2026-09-06
resource: config/providers.yaml
---

# Our Own GPU in the Fleet

*Measured, not assumed: an 8B model on a 16 GB MacBook agrees with the incumbent extraction as often as the free fleet's middle, and has no daily limit to spend.*

## The question, and the number that actually mattered

Not "is a local model as good as the cloud" but "is it good enough to be worth
having". Both halves were measured on 2026-09-05/06 against the Hungarian golden
set — sample `d13dfe914a92`, 16 pages, 17 expected communities, exported with
[[exporting-a-golden-set]] and verified identical to the server's by fingerprint.

| model, on this machine | score | s/page | answered | weights |
|---|---|---|---|---|
| Qwen3-8B Q4_K_M | **67** | 27 | 15/16 | 5.0 GB |
| gpt-oss-20b MXFP4 | 65 | 45 | 16/16 | 12.1 GB |

For scale, the free fleet on its own (older, differently-sampled) numbers runs
52-80, with `mistral-small` at 80 and Groq's gpt-oss-20b at 67. So this is the
fleet's middle, not its top — and the cloud comparison on *this* sample is still
outstanding, because on 2026-09-05 every free provider's daily allowance was
already spent by 22:30 UTC.

**That last sentence is the decision.** A provider scoring 67 with no allowance
is worth more than one scoring 74 that has been unavailable since lunchtime.
Routing is by quality, so the better free models still go first; this one is what
remains when they are gone.

Qwen3-8B over gpt-oss-20b on everything except the score: 1.7x faster, and 5.0 GB
against 12.1 — the difference between a laptop that is still usable and one that
is not. gpt-oss-20b's appeal was that Cloudflare and Groq serve the *same
weights*, which would have isolated "where it runs" from "which model it is".
Worth measuring; not worth running.

## The runtime was the variable, not the hardware

The first attempt measured **145 s per page** and looked like a verdict on the
machine. It was a verdict on the packaging. Ollama ships gpt-oss as MXFP4, and
that path does not reach Metal on Apple Silicon:

| | Ollama (MXFP4) | llama.cpp (GGUF) |
|---|---|---|
| prefill | 124 tok/s | **290 tok/s** |
| generation | 14.5 tok/s | **27-29 tok/s** |
| `ollama ps` | `19%/81% CPU/GPU` | — |

Same machine, same weights. Two further things were needed and both are
non-obvious:

- **`iogpu.wired_limit_mb=13500`.** The default GPU budget on 16 GB is ~11.8 GiB
  and gpt-oss-20b is 12.1 GB, so Metal answered
  `kIOGPUCommandBufferCallbackErrorOutOfMemory` at load. Not persistent across a
  reboot. Qwen3-8B does not need it; it is recorded because the diagnosis took
  the longest.
- **`-c 8192`.** The extraction prompt is 3.1-4.4K tokens depending on tokenizer
  (2,852-char system prompt plus `max_text_chars: 8000` of page text) and the cap
  is 4,000 generated. Ollama's VRAM-derived default is 4,096, and a runtime short
  of context *trims the prompt* rather than failing — the page text is the tail
  of the message, so it trims exactly the part the answer depends on. A trimmed
  prompt still scores. It scores the truncation.

## How it is wired in

`localgpu` in `config/providers.yaml`, reached like any other provider. Nothing
in the pipeline knows it is ours.

- **Both the address and the key come from the environment** (`base_url_env:
  LOCAL_GPU_URL`, `api_key_env: LOCAL_GPU_KEY`), and `configured` is false unless
  both are set. That is the correct state whenever the machine is asleep, closed,
  or off — see [[free-tier-model-router]] for what `configured` gates. `base_url`
  is deliberately empty: a stale default would send every call in a run to a dead
  host. The address is env-borne because a Cloudflare quick tunnel gets a new
  hostname on every reconnect and `config/` is not a persisted volume, so a URL
  in the YAML would make each reconnection a code deploy.
- **`timeout_seconds: 600`**, against the global 60. A full 8,000-char page
  measured 66.5 s end to end through the tunnel, so the global would fail every
  call — and a timeout is scored as a *failure*, so it would not report a slow
  provider, it would retire a working one through the circuit breaker. Raising
  the global instead is the wrong fix: it would let a genuinely hung hosted call
  hold a slot for as long as the slowest local model is allowed.
- **`max_output_tokens: 4000`**, against the fleet's 1,500. This is the one place
  that rule inverts. The 1,500 exists because Groq reserves `prompt + max_tokens`
  against an 8,000-token minute window *before* generating; nothing is reserved
  here and nothing is billed, so the cap costs only seconds actually spent.
- **`max_concurrency: 1`**, against unlimited everywhere else — and this is the
  setting that made it work at all. `pipeline.extract_concurrency: 4` is right
  for hosted APIs, where the wait is network latency and four requests overlap
  for free. On one GPU they do not overlap: they share it. Four concurrent pages
  returned four answers each about four times slower (27 tok/s alone against
  **1.85 tok/s** with four slots busy), which bought nothing and pushed every
  call past **Cloudflare's 100-second origin timeout** — HTTP 524 on extractions
  that were otherwise fine. That 100 s is the real ceiling regardless of
  `timeout_seconds: 600`, on every non-Enterprise plan.

  The queue belongs on our side of the wire. Waiting on the semaphore holds no
  HTTP connection open, so the proxy's clock does not start until the model is
  free. Serialising at the far end instead (`llama-server --parallel 1`) would
  do the opposite: the request sits in the origin's own queue with the
  connection open and times out exactly as before.

On the machine: `llama-server` and `cloudflared` run as launchd agents
(`com.meetapedia.llama`, `com.meetapedia.tunnel`) with `KeepAlive`, under
`caffeinate -i` so the provider does not vanish when the laptop is left alone.
`llama-server --api-key` means the tunnel answers 401 without the bearer token.

**Use a named tunnel, not a quick one.** `cloudflared tunnel --url` needs no
account and is the obvious way to start, but its hostname is random and it gets
a *new* one on every reconnect. The first night's tunnel ran 8 hours, dropped,
and then failed to retake its own name — 22 consecutive `control stream
encountered a failure while serving`. The failure mode is the problem: the
server's `LOCAL_GPU_URL` keeps pointing at a hostname that no longer resolves to
anything, `configured` is still true because the variable is still set, and every
call fails at connect time until a human notices. A named tunnel
(`gpu.meetapedia.com`, created 2026-09-06) keeps its hostname across restarts,
reboots and network changes, so the variable is set once and `base_url_env` stops
being a maintenance burden. Costs one browser login; nothing else changes.

The hostname sits on the public brand domain deliberately — the name should say
which project the provider belongs to, and there is nothing to leak behind it:
Cloudflare proxies it (so the machine's own IP is never exposed) and it answers
401 to everything without the key. Credentials are `~/.cloudflared/cert.pem` and
`~/.cloudflared/<tunnel-id>.json`, outside the repository and secret.

## What this does not settle

- **Throughput does not transfer.** 27 s/page is this M3's number. The Hetzner
  box has no GPU; the same model there would be far slower. This machine
  contributes as a machine, not as a proof about the server.
- **The same-sample cloud comparison is still owed.** Scheduled for the next
  00:02 UTC window, before the `ai_only` run starts consuming the day.
- **The scores measure agreement with the incumbent extraction**, not ground
  truth — see [[measuring-extraction-quality]]. A model that finds a real club
  the incumbent missed is scored down for it.
- **Constrained decoding is available and not yet used.** The project's own
  `EXTRACTION_SCHEMA`, sent as a per-request
  `response_format: {"type": "json_schema", ...}`, is enforced at the sampler and
  returns clean JSON with every field — verified 2026-09-06. Only the
  *server-level* `--json-schema-file` flag fails (`Failed to initialize
  samplers: std::exception`), so the schema is fine and the flag is not.
  Meanwhile `response_format: {"type": "json_object"}` — what `json_mode: true`
  sends — is **silently ignored** by llama.cpp, so the prompt is doing all the
  work today.

  Not adopted yet, deliberately. It would change the payload of every extraction
  call, which is the highest-risk shared path in `extract.py`, and it would need
  a per-call-site schema (extraction, venue, person and enrichment each have
  their own) rather than one flag. The failure it prevents is already *recovered*
  by `_json_items` unwrapping fences; the gain is preventing them instead, which
  does not justify an unreviewed change to that path.

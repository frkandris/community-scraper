"""Public OpenAI-compatible gateway over the free-tier model router.

Lets other software call "one LLM endpoint, routed across free providers" with
whatever OpenAI SDK it already uses — only `base_url` and `api_key` change:

    from openai import OpenAI
    client = OpenAI(base_url="https://meetapedia.com/v1", api_key="<ROUTER_API_KEY>")
    client.chat.completions.create(model="auto", messages=[...])

Why OpenAI's shape and not our own: every language already has a maintained
client for it, so integration cost is a config line instead of a new SDK. The
response body is the upstream provider's, forwarded intact — `id`, `usage`,
`finish_reason` and all — with two additive fields (`x_router`) naming what
actually served the request. Additive fields are ignored by strict clients.

Deliberately general purpose: no prompt, schema or persona of this project is
injected. The caller's `messages` go upstream as-is, so this is a plain LLM
endpoint that happens to be free and routed — not a community-extraction API.

Full contract: docs/wiki/pages/integrations/router-gateway-api.md.
"""
from __future__ import annotations

import asyncio
import hmac
import json
import os
import time
from typing import Any

import structlog
from fastapi import APIRouter, Header, Request
from fastapi.responses import JSONResponse

from ..extract import (ExtractorQuotaError, ExtractorRateLimitError,
                       ExtractorUnavailableError)
from .state import app_state

log = structlog.get_logger()

router = APIRouter(prefix="/v1", tags=["gateway"])

#: Comma-separated list, so callers can be revoked individually. Unset → the
#: gateway is off entirely rather than open: an unauthenticated LLM proxy is a
#: free-credit faucet for anyone who finds it.
_API_KEYS_ENV = "ROUTER_API_KEY"

#: Hard ceiling on messages accepted in one request. Not a quality judgement —
#: an unbounded list is an easy way to burn a provider's token budget.
_MAX_MESSAGES = 60
_MAX_CHARS = 200_000


def _valid_keys() -> list[str]:
    return [k.strip() for k in (os.environ.get(_API_KEYS_ENV) or "").split(",") if k.strip()]


def _authorized(authorization: str | None) -> bool:
    keys = _valid_keys()
    if not keys:
        return False
    token = ""
    if authorization and authorization.lower().startswith("bearer "):
        token = authorization[7:].strip()
    # Compare as bytes: hmac.compare_digest raises TypeError on a non-ASCII str,
    # so `Authorization: Bearer héllo` would 500 with a stack trace instead of
    # returning the 401 it deserves.
    token_b = token.encode("utf-8", "surrogatepass")
    # compare_digest against every key: constant-time per comparison, and the
    # loop length depends on configuration, not on the submitted token.
    return any(hmac.compare_digest(token_b, k.encode("utf-8")) for k in keys)


def _error(status: int, message: str, err_type: str, code: str | None = None) -> JSONResponse:
    """OpenAI's error envelope — clients parse `error.type` / `error.code`."""
    return JSONResponse(
        status_code=status,
        content={"error": {"message": message, "type": err_type,
                           "param": None, "code": code}},
    )


def _build_router():
    """A router bound to the live app config, or None when unavailable."""
    from ..router import build_router as _bld
    cfg = app_state.pipeline_cfg
    if cfg is None:
        return None
    return _bld(
        app_state.db_path,
        temperature=cfg.deepseek_temperature,
        timeout_seconds=cfg.deepseek_timeout,
        max_text_chars=cfg.deepseek_max_text_chars,
        rate_limit_seconds=cfg.deepseek_rate_limit_seconds,
        fingerprint_model=cfg.deepseek_fingerprint_model or cfg.deepseek_model,
    )


def _select(fleet: list, requested: str) -> list:
    """Order the fleet for one request according to the `model` field.

    Accepted values:
      "auto" / "" / "default"  → best available (the point of the gateway)
      "<provider>:<model>"     → that exact model, no substitution
      "<provider>"             → best model on that provider
      "<model>"                → that model wherever it lives

    An explicit choice returns a one-element list on purpose: a caller who named
    a model wants that model, and silently answering from another one would make
    their evaluation meaningless.
    """
    want = (requested or "auto").strip()
    if want.lower() in ("auto", "", "default", "router"):
        return fleet
    # Exact model id first: OpenRouter ids end in ":free", so splitting on the
    # colon before trying a whole-id match turns "qwen/qwen3-235b-a22b:free"
    # into provider "qwen/qwen3-235b-a22b" and a bogus 404.
    exact = [e for e in fleet if e.model == want]
    if exact:
        return exact[:1]
    by_provider = [e for e in fleet if e.provider == want]
    if by_provider:
        return by_provider[:1]
    if ":" in want:
        provider, _, model = want.partition(":")
        return [e for e in fleet if e.provider == provider and e.model == model]
    return []


@router.get("/models")
async def list_models(authorization: str | None = Header(default=None)):
    """OpenAI `/v1/models`: what this gateway can route to today.

    Lists models with **daily budget** left, not models callable this
    millisecond. `order()` also excludes anything inside its rpm cooldown, which
    at rpm 30 is a two-second window — a client polling this endpoint would see
    most of the fleet blink in and out for no reason it could act on. Same
    distinction the completions route makes.

    `owned_by` carries the provider; the non-standard `quality` field exposes
    the routing score. `paced` flags a model that is momentarily on cooldown but
    otherwise available.
    """
    if not _authorized(authorization):
        return _error(401, "Invalid or missing API key.", "invalid_request_error",
                      "invalid_api_key")
    mr = _build_router()
    if mr is None or not mr.enabled:
        return _error(503, "Model router is not configured.", "server_error",
                      "router_disabled")
    now = int(time.time())
    callable_now = {id(e) for e in mr.order()}
    data = [{
        "id": f"{e.provider}:{e.model}",
        "object": "model",
        "created": now,
        "owned_by": e.provider,
        "quality": e.quality,
        "paced": id(e) not in callable_now,
    } for e in mr.with_budget()]
    # "auto" is the model most callers should ask for, so advertise it first.
    data.insert(0, {"id": "auto", "object": "model", "created": now,
                    "owned_by": "router",
                    "quality": mr.best_available_quality()})
    return {"object": "list", "data": data}


@router.get("/models/upstream")
async def models_upstream(authorization: str | None = Header(default=None)):
    """What each provider *actually serves* right now, next to what we configured.

    Distinct from `/v1/models`, which lists what the router can route to today —
    that view hides a provider whose daily budget is spent or one parked behind
    `allow_paid`, and a checker reading it concludes the models were retired.
    This asks the providers themselves, which is the only way to answer "is
    there a new free model" at all.

    Keys live only on the server, so this route is the only place the question
    can be asked from.
    """
    if not _authorized(authorization):
        return _error(401, "Invalid or missing API key.", "invalid_request_error",
                      "invalid_api_key")
    from ..providers import fetch_upstream_models, load_catalogue

    catalogue = load_catalogue()
    out = []
    for spec in catalogue.providers:
        upstream, err = await asyncio.to_thread(fetch_upstream_models, spec)
        configured = [m.model for m in spec.models]
        out.append({
            "provider": spec.name,
            "enabled": spec.enabled,
            "paid": spec.paid,
            "error": err,
            "configured": configured,
            "upstream": upstream,
            # Only meaningful when we actually got a list back.
            "gone": [m for m in configured if m not in upstream] if upstream else [],
            "new": [m for m in upstream if m not in configured],
        })
    return {"object": "list", "data": out}


@router.get("/quota")
async def quota(authorization: str | None = Header(default=None)):
    """Non-standard: today's per-provider budget, for callers that want to back
    off before being rate limited rather than after."""
    if not _authorized(authorization):
        return _error(401, "Invalid or missing API key.", "invalid_request_error",
                      "invalid_api_key")
    mr = _build_router()
    if mr is None:
        return _error(503, "Model router is not configured.", "server_error",
                      "router_disabled")
    return {
        "object": "list",
        "day": mr.ledger.day,
        "data": [{k: p[k] for k in
                  ("name", "configured", "best_quality", "budget", "used",
                   "remaining", "blocked", "paid")}
                 for p in mr.ledger.snapshot(mr.catalogue)],
    }


@router.get("/logs")
async def logs(
    authorization: str | None = Header(default=None),
    lines: int = 200,
    grep: str = "",
    level: str = "",
):
    """Recent application log lines — the same in-memory ring the admin log page
    streams, exposed under the gateway's Bearer auth.

    Exists so a debugging session can read production logs the way it reads
    /healthz, without a Coolify login. It is a *complement* to the platform's
    logs, not a replacement: this endpoint needs a running app, so it cannot
    explain a container that fails to start — for that the Coolify deployment
    log is still the only source.

    `grep` is a plain substring match (not a regex: an operator-supplied regex
    is an easy accidental catastrophic backtrack). `level` filters exactly.
    """
    if not _authorized(authorization):
        return _error(401, "Invalid or missing API key.", "invalid_request_error",
                      "invalid_api_key")
    from .log_stream import broadcaster

    rows = broadcaster.get_all()
    if level:
        want = level.lower()
        rows = [r for r in rows if str(r.get("level", "")).lower() == want]
    if grep:
        needle = grep.lower()
        rows = [r for r in rows if needle in json.dumps(r, ensure_ascii=False).lower()]
    rows = rows[-max(1, min(lines, 1000)):]
    return {"object": "list", "count": len(rows), "data": rows}


@router.post("/score")
async def score(
    authorization: str | None = Header(default=None),
    pages: int = 8,
    provider: str = "",
):
    """Measure the routed fleet on our own extraction task.

    Read-only: it reports scores and never writes `config/providers.yaml`. The
    catalogue is a mounted volume, so applying a result is a deliberate edit,
    not a side effect of measuring — a bad golden set silently rewriting the
    routing order is exactly the failure worth keeping manual.

    Costs one LLM call per model per page: at the default 8 pages and a
    12-model fleet that is ~96 calls, which comes out of the same daily budget
    the crawler uses. Check `GET /v1/quota` first.
    """
    if not _authorized(authorization):
        return _error(401, "Invalid or missing API key.", "invalid_request_error",
                      "invalid_api_key")
    mr = _build_router()
    if mr is None or not mr.enabled:
        return _error(503, "Model router is not configured.", "server_error",
                      "router_disabled")
    if not app_state.db_path:
        return _error(503, "No database configured.", "server_error", "no_database")

    fleet = mr.all_extractors()
    if provider:
        fleet = [e for e in fleet if e.provider == provider]
    if not fleet:
        return _error(404, f"No models for provider '{provider}'.",
                      "invalid_request_error", "model_not_found")

    from ..scoring import score_fleet
    try:
        out = await score_fleet(app_state.db_path, fleet,
                                pages=max(1, min(pages, 40)))
    except FileNotFoundError as exc:
        return _error(503, str(exc), "server_error", "no_database")
    if out.get("error"):
        return _error(422, out["error"], "invalid_request_error", "no_golden_pages")
    return out


@router.post("/chat/completions")
async def chat_completions(
    request: Request,
    authorization: str | None = Header(default=None),
):
    """OpenAI `/v1/chat/completions`, routed across the free-tier fleet.

    Differences from the upstream contract, all deliberate:

    * `model` selects a *routing policy*, not only a model — see `_select`.
    * `stream: true` is rejected rather than silently ignored. Answering a
      streaming request with a non-streaming body hangs clients that are waiting
      for SSE frames; an explicit 400 is the honest failure.
    * The response carries an extra `x_router` object naming the provider and
      model that served it. Unknown top-level fields are ignored by every
      OpenAI client, so this stays compatible.
    """
    if not _authorized(authorization):
        return _error(401, "Invalid or missing API key.", "invalid_request_error",
                      "invalid_api_key")
    try:
        body: Any = await request.json()
    except Exception:
        return _error(400, "Request body must be valid JSON.", "invalid_request_error")
    if not isinstance(body, dict):
        return _error(400, "Request body must be a JSON object.", "invalid_request_error")

    messages = body.get("messages")
    if not isinstance(messages, list) or not messages:
        return _error(400, "'messages' must be a non-empty array.",
                      "invalid_request_error", "messages")
    if len(messages) > _MAX_MESSAGES:
        return _error(400, f"'messages' exceeds the {_MAX_MESSAGES}-message limit.",
                      "invalid_request_error", "messages")
    for m in messages:
        if not isinstance(m, dict) or "role" not in m:
            return _error(400, "Each message needs a 'role' and 'content'.",
                          "invalid_request_error", "messages")
    if sum(len(str(m.get("content") or "")) for m in messages) > _MAX_CHARS:
        return _error(400, f"Total content exceeds {_MAX_CHARS} characters.",
                      "invalid_request_error", "messages")
    if body.get("stream"):
        return _error(400, "Streaming is not supported by this gateway; "
                           "omit 'stream' or set it to false.",
                      "invalid_request_error", "stream")

    mr = _build_router()
    if mr is None or not mr.enabled:
        return _error(503, "Model router is not configured.", "server_error",
                      "router_disabled")

    requested = str(body.get("model") or "auto")
    # with_budget(), not order(): order() also excludes providers inside their
    # rpm cooldown, which at rpm: 30 is a two-second wait — telling a caller
    # "no quota left today" for that would be a lie, and FallbackExtractor's
    # _await_pacing waits it out anyway.
    fleet = _select(mr.with_budget(), requested)
    if not fleet:
        # Distinguish "you named something that does not exist" from "everything
        # that could serve you is out of quota" — different fixes.
        if _select(mr._all, requested):
            return _error(429, f"No quota left today for '{requested}'.",
                          "rate_limit_error", "quota_exhausted")
        return _error(404, f"Unknown model '{requested}'. See GET /v1/models.",
                      "invalid_request_error", "model_not_found")

    from ..extract import FallbackExtractor
    # scope_to=fleet so "out of quota" is judged against the models that could
    # serve THIS request. A caller pinning one exhausted model would otherwise
    # see 502 from unrelated providers' spare capacity instead of a 429.
    chain = FallbackExtractor(primaries=fleet, router=mr, scope_to=fleet)
    # Filter here, not only inside _ApiExtractor.completion: these kwargs travel
    # through FallbackExtractor._call(self, method, label, *args, **kwargs), so a
    # body field named "method" or "label" would raise TypeError and surface as a
    # 500 on a public endpoint.
    from ..extract import _ApiExtractor
    params = {k: v for k, v in body.items() if k in _ApiExtractor._PASSTHROUGH_FIELDS}
    try:
        data = await chain.completion(messages, **params)
    except ExtractorRateLimitError:
        return _error(429, "All routed providers are rate limited; retry shortly.",
                      "rate_limit_error", "rate_limited")
    except ExtractorQuotaError:
        return _error(429, "Provider quota exhausted for today.",
                      "rate_limit_error", "quota_exhausted")
    except ExtractorUnavailableError as exc:
        log.warning("gateway_upstream_unavailable", model=requested, error=str(exc))
        return _error(502, f"No provider could serve the request: {exc}",
                      "server_error", "upstream_unavailable")
    except Exception as exc:
        log.exception("gateway_unexpected_error", model=requested)
        return _error(500, f"Gateway error: {type(exc).__name__}", "server_error")

    if not isinstance(data, dict):
        return _error(502, "Upstream returned an unexpected response shape.",
                      "server_error", "bad_upstream_response")
    data["x_router"] = {
        # From the chain, not matched by model id: the catalogue already lists
        # llama-3.3-70b under three providers, and after failover the head of
        # the fleet is the wrong answer anyway.
        "provider": chain.last_provider or fleet[0].provider,
        "model": chain.last_model,
        "quality": chain.last_quality,
        "requested": requested,
    }
    return data

"""Public OpenAI-compatible gateway at /v1/*.

The contract other software depends on, so it is pinned here: auth, error
envelope shape, model selection semantics, and the guarantee that nothing
project-specific is injected into a caller's messages.
"""
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from scraper.db import init_db
from scraper.providers import (ModelSpec, ProviderCatalogue, ProviderSpec,
                               RouterSettings)
from scraper.web import app as web_app
from scraper.web.state import app_state

KEY = "sk-router-test"
AUTH = {"Authorization": f"Bearer {KEY}"}


def _catalogue():
    return ProviderCatalogue(
        router=RouterSettings(enabled=True),
        providers=(
            ProviderSpec(name="alpha", base_url="https://alpha.test/v1",
                         api_key_env="ALPHA_KEY",
                         models=(ModelSpec(model="alpha-big", quality=70),),
                         rpd=1000),
            ProviderSpec(name="beta", base_url="https://beta.test/v1",
                         api_key_env="BETA_KEY",
                         models=(ModelSpec(model="beta-small", quality=40),),
                         rpd=1000),
        ),
    )


@pytest.fixture()
def client(tmp_path: Path, monkeypatch):
    db = tmp_path / "scraper.db"
    init_db(db)
    monkeypatch.setenv("ROUTER_API_KEY", KEY)
    monkeypatch.setenv("ALPHA_KEY", "k")
    monkeypatch.setenv("BETA_KEY", "k")

    class _Cfg:
        deepseek_temperature = 0.1
        deepseek_timeout = 60
        deepseek_max_text_chars = 8000
        deepseek_rate_limit_seconds = 0.0
        deepseek_fingerprint_model = "deepseek-chat"
        deepseek_model = "deepseek-chat"

    old_db, old_cfg = app_state.db_path, app_state.pipeline_cfg
    app_state.db_path = db
    app_state.pipeline_cfg = _Cfg()
    with patch("scraper.providers.load_catalogue", return_value=_catalogue()), \
         patch("scraper.router.load_catalogue", return_value=_catalogue()):
        yield TestClient(web_app.app)
    app_state.db_path, app_state.pipeline_cfg = old_db, old_cfg


def _upstream(content="hello", model="alpha-big"):
    """Minimal OpenAI-shaped response body."""
    return {
        "id": "chatcmpl-1", "object": "chat.completion", "model": model,
        "choices": [{"index": 0, "finish_reason": "stop",
                     "message": {"role": "assistant", "content": content}}],
        "usage": {"prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7},
    }


# ── auth ─────────────────────────────────────────────────────────────────────

def test_requires_bearer_token(client):
    assert client.get("/v1/models").status_code == 401
    assert client.get("/v1/models", headers={"Authorization": "Bearer wrong"}).status_code == 401
    assert client.post("/v1/chat/completions", json={"messages": []}).status_code == 401


def test_gateway_is_off_when_no_key_configured(client, monkeypatch):
    # An unauthenticated LLM proxy is a free-credit faucet; absent config must
    # close the door, not open it.
    monkeypatch.delenv("ROUTER_API_KEY", raising=False)
    assert client.get("/v1/models", headers=AUTH).status_code == 401


def test_error_body_uses_the_openai_envelope(client):
    body = client.get("/v1/models").json()
    assert set(body["error"]) == {"message", "type", "param", "code"}
    assert body["error"]["code"] == "invalid_api_key"


# ── discovery ────────────────────────────────────────────────────────────────

def test_models_lists_auto_first_then_the_fleet(client):
    body = client.get("/v1/models", headers=AUTH).json()
    assert body["object"] == "list"
    ids = [m["id"] for m in body["data"]]
    assert ids[0] == "auto"
    assert ids[1:] == ["alpha:alpha-big", "beta:beta-small"]
    assert body["data"][1]["owned_by"] == "alpha"


def test_quota_endpoint_reports_per_provider_budget(client):
    body = client.get("/v1/quota", headers=AUTH).json()
    names = {p["name"] for p in body["data"]}
    assert names == {"alpha", "beta"}
    assert all("remaining" in p for p in body["data"])


# ── completions ──────────────────────────────────────────────────────────────

def test_auto_routes_to_the_best_model_and_reports_it(client):
    with patch("scraper.extract._ApiExtractor._post",
               return_value=_upstream()) as post:
        resp = client.post("/v1/chat/completions", headers=AUTH, json={
            "model": "auto", "messages": [{"role": "user", "content": "hi"}],
        })
    assert resp.status_code == 200
    body = resp.json()
    # Upstream envelope is forwarded intact.
    assert body["choices"][0]["message"]["content"] == "hello"
    assert body["usage"]["total_tokens"] == 7
    # Plus additive provenance.
    assert body["x_router"]["provider"] == "alpha"
    assert body["x_router"]["model"] == "alpha-big"
    assert body["x_router"]["requested"] == "auto"
    # The wire payload names the routed model, not the caller's "auto".
    assert post.call_args[0][0]["model"] == "alpha-big"


def test_caller_messages_are_forwarded_unmodified(client):
    # The gateway is general purpose: no project prompt, schema or persona is
    # injected. Callers get a plain LLM endpoint.
    messages = [{"role": "system", "content": "You are a pirate."},
                {"role": "user", "content": "Ahoy?"}]
    with patch("scraper.extract._ApiExtractor._post",
               return_value=_upstream()) as post:
        client.post("/v1/chat/completions", headers=AUTH,
                    json={"messages": messages})
    assert post.call_args[0][0]["messages"] == messages


def test_explicit_model_is_honoured_without_substitution(client):
    with patch("scraper.extract._ApiExtractor._post",
               return_value=_upstream(model="beta-small")) as post:
        body = client.post("/v1/chat/completions", headers=AUTH, json={
            "model": "beta:beta-small",
            "messages": [{"role": "user", "content": "hi"}],
        }).json()
    assert post.call_args[0][0]["model"] == "beta-small"
    assert body["x_router"]["provider"] == "beta"


def test_unknown_model_is_404_not_a_silent_substitution(client):
    resp = client.post("/v1/chat/completions", headers=AUTH, json={
        "model": "nope:nothing", "messages": [{"role": "user", "content": "hi"}],
    })
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "model_not_found"


def test_sampling_params_pass_through_but_unknown_ones_do_not(client):
    with patch("scraper.extract._ApiExtractor._post",
               return_value=_upstream()) as post:
        client.post("/v1/chat/completions", headers=AUTH, json={
            "messages": [{"role": "user", "content": "hi"}],
            "temperature": 0.9, "max_tokens": 128,
            "wild_unknown_field": "boom",
        })
    payload = post.call_args[0][0]
    assert payload["temperature"] == 0.9
    assert payload["max_tokens"] == 128
    # An unknown field 400s at several providers, and the chain would then retry
    # it against every one of them.
    assert "wild_unknown_field" not in payload


def test_streaming_is_rejected_explicitly(client):
    # Answering a streaming request with a non-streaming body hangs clients
    # waiting for SSE frames.
    resp = client.post("/v1/chat/completions", headers=AUTH, json={
        "messages": [{"role": "user", "content": "hi"}], "stream": True,
    })
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "stream"


@pytest.mark.parametrize("body,code", [
    ({}, "messages"),
    ({"messages": []}, "messages"),
    ({"messages": "hi"}, "messages"),
    ({"messages": [{"content": "no role"}]}, "messages"),
])
def test_malformed_requests_are_400(client, body, code):
    resp = client.post("/v1/chat/completions", headers=AUTH, json=body)
    assert resp.status_code == 400
    assert resp.json()["error"]["param"] == code or resp.json()["error"]["code"] == code


def test_oversized_payload_is_rejected(client):
    huge = [{"role": "user", "content": "x" * 200_001}]
    resp = client.post("/v1/chat/completions", headers=AUTH, json={"messages": huge})
    assert resp.status_code == 400


def test_upstream_failure_maps_to_502(client):
    from scraper.extract import ExtractorUnavailableError
    with patch("scraper.extract._ApiExtractor._post",
               side_effect=ExtractorUnavailableError("alpha: HTTP 500")):
        resp = client.post("/v1/chat/completions", headers=AUTH, json={
            "messages": [{"role": "user", "content": "hi"}],
        })
    assert resp.status_code == 502
    assert resp.json()["error"]["code"] == "upstream_unavailable"


def test_exhausted_quota_is_429_not_404(client):
    # "You named something that does not exist" and "everything that could serve
    # you is spent" need different fixes, so they get different codes.
    from scraper.router import QuotaLedger
    ledger = QuotaLedger(app_state.db_path)
    for _ in range(1000):
        ledger.note_call("alpha")
        ledger.note_call("beta")
    resp = client.post("/v1/chat/completions", headers=AUTH, json={
        "model": "alpha:alpha-big", "messages": [{"role": "user", "content": "hi"}],
    })
    assert resp.status_code == 429
    assert resp.json()["error"]["code"] == "quota_exhausted"


def test_gateway_calls_count_against_the_shared_ledger(client):
    # External traffic and the crawler share one budget; a call that did
    # not count would let the gateway starve the pipeline invisibly.
    from scraper.router import QuotaLedger
    before = QuotaLedger(app_state.db_path).used("alpha")
    with patch("scraper.extract._ApiExtractor._post", return_value=_upstream()):
        client.post("/v1/chat/completions", headers=AUTH, json={
            "messages": [{"role": "user", "content": "hi"}],
        })
    assert QuotaLedger(app_state.db_path).used("alpha") == before + 1


# ── regressions from review round 2 ──────────────────────────────────────────

def test_model_ids_containing_colons_resolve():
    """OpenRouter ids end in ":free".

    Splitting on the colon before trying a whole-id match turned
    "qwen/qwen3-235b-a22b:free" into provider "qwen/qwen3-235b-a22b" and a bogus
    404 — for three of the shipped models, via the documented bare-model form.
    """
    from scraper.web.api import _select

    class _E:
        def __init__(self, provider, model):
            self.provider, self.model = provider, model

    fleet = [_E("openrouter", "qwen/qwen3-235b-a22b:free"),
             _E("groq", "qwen3-32b")]

    def ids(sel):
        return [f"{e.provider}:{e.model}" for e in sel]

    assert ids(_select(fleet, "qwen/qwen3-235b-a22b:free")) == \
        ["openrouter:qwen/qwen3-235b-a22b:free"]
    assert ids(_select(fleet, "openrouter:qwen/qwen3-235b-a22b:free")) == \
        ["openrouter:qwen/qwen3-235b-a22b:free"]
    assert ids(_select(fleet, "openrouter")) == ["openrouter:qwen/qwen3-235b-a22b:free"]
    assert _select(fleet, "nope") == []


def test_body_field_named_method_does_not_500(client):
    """`method` and `label` are positional parameters of FallbackExtractor._call.

    Splatting unfiltered body keys into it raised TypeError on a public
    endpoint. The allowlist has to be applied here, not one layer deeper.
    """
    with patch("scraper.extract._ApiExtractor._post", return_value=_upstream()):
        resp = client.post("/v1/chat/completions", headers=AUTH, json={
            "messages": [{"role": "user", "content": "hi"}],
            "method": "boom", "label": "boom", "self": "boom",
        })
    assert resp.status_code == 200


def test_x_router_names_the_serving_provider_not_a_model_id_match(client):
    # The catalogue already lists llama-3.3-70b under three providers, and after
    # failover the head of the fleet is the wrong answer anyway.
    with patch("scraper.extract._ApiExtractor._post",
               return_value=_upstream(model="beta-small")):
        body = client.post("/v1/chat/completions", headers=AUTH, json={
            "model": "beta:beta-small",
            "messages": [{"role": "user", "content": "hi"}],
        }).json()
    assert body["x_router"]["provider"] == "beta"
    assert body["x_router"]["model"] == "beta-small"


# ── log access ───────────────────────────────────────────────────────────────

def test_logs_endpoint_requires_auth_and_returns_recent_lines(client):
    """Production log access without a Coolify login.

    Added after the 2026-08-16 rollout, where diagnosing a bad model name meant
    going through the platform API. Note the limit this cannot lift: it needs a
    running app, so a container that fails to start is still a Coolify-log job.
    """
    from scraper.web.log_stream import broadcaster

    assert client.get("/v1/logs").status_code == 401

    broadcaster.add_line({"event": "extractor_model_retired", "level": "warning",
                          "model": "groq:qwen3-32b"})
    broadcaster.add_line({"event": "pipeline_complete", "level": "info"})

    body = client.get("/v1/logs", headers=AUTH).json()
    assert body["count"] >= 2
    assert any("extractor_model_retired" in str(r) for r in body["data"])

    # Substring filter, deliberately not a regex.
    filtered = client.get("/v1/logs?grep=model_retired", headers=AUTH).json()
    assert filtered["count"] >= 1
    assert all("model_retired" in str(r).lower() for r in filtered["data"])

    only_warn = client.get("/v1/logs?level=warning", headers=AUTH).json()
    assert all(r.get("level") == "warning" for r in only_warn["data"])


def test_score_endpoint_measures_the_fleet(client, tmp_path):
    """POST /v1/score runs the golden-set measurement remotely.

    Exists because the CLI needs the production database and the production API
    keys — it can only run where both are, and there is no shell access there.
    """
    from scraper.db import update_cache_page

    db = app_state.db_path
    update_cache_page(db, "g1", {
        "url": "https://x/g1", "city": "Szentendre", "topic": "running",
        "extracted_at": "2026-08-01T00:00:00+00:00",
        "raw_text": "A Szentendrei Futóklub keddenként edz a Duna-parton.",
        "records": [{"name": "Szentendrei Futóklub"}],
    }, create={"url": "https://x/g1"})

    assert client.post("/v1/score").status_code == 401

    def _reply(*a, **k):
        return {"choices": [{"message": {"content":
                 '{"communities":[{"name":"Szentendrei Futóklub",'
                 '"confidence":0.9,"joinable":true}]}'}}]}

    with patch("scraper.extract._ApiExtractor._post", side_effect=_reply):
        body = client.post("/v1/score?pages=1", headers=AUTH).json()

    assert body["pages"] == 1
    assert body["results"], body
    top = body["results"][0]
    assert top["score"] == 100          # exact match on the only expected name
    assert top["answered"] == 1
    # The scores are agreement with the incumbent, not truth — say so in-band.
    assert "not ground truth" in body["note"]


def test_score_endpoint_reports_an_empty_golden_set_clearly(client):
    resp = client.post("/v1/score?pages=2", headers=AUTH)
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "no_golden_pages"

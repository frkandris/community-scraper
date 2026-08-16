"""Free-tier model router: catalogue, quota ledger, ordering, upgrade policy."""
from pathlib import Path

import pytest

from scraper.db import (get_provider_usage, get_upgradable_pages, init_db,
                        record_provider_call, update_cache_page)
from scraper.providers import (ModelSpec, ProviderCatalogue, ProviderSpec,
                               RouterSettings, build_extractors, load_catalogue)
from scraper.router import ModelRouter, QuotaLedger, build_router


def _db(tmp_path: Path) -> Path:
    p = tmp_path / "scraper.db"
    init_db(p)
    return p


def _spec(name="groq", rpd=1000, paid=False, quality=(60, 40), env="X_KEY"):
    return ProviderSpec(
        name=name, base_url="https://api.test/v1", api_key_env=env,
        models=tuple(ModelSpec(model=f"{name}-m{i}", quality=q)
                     for i, q in enumerate(quality)),
        rpm=30, rpd=rpd, paid=paid,
    )


def _catalogue(*specs, enabled=True, allow_paid=False, min_gain=8, max_per_run=500):
    return ProviderCatalogue(
        router=RouterSettings(enabled=enabled, allow_paid=allow_paid,
                              upgrade_min_gain=min_gain,
                              upgrade_max_per_run=max_per_run),
        providers=tuple(specs),
    )


# ── catalogue ────────────────────────────────────────────────────────────────

def test_shipped_catalogue_parses():
    cat = load_catalogue()
    assert cat.router.enabled is True
    names = [p.name for p in cat.providers]
    for expected in ("groq", "cerebras", "gemini", "mistral", "openrouter",
                     "github", "deepseek"):
        assert expected in names
    # DeepSeek is the only paid one and it is parked by default.
    assert [p.name for p in cat.providers if p.paid] == ["deepseek"]
    assert cat.router.allow_paid is False


def test_models_are_sorted_best_first_on_load():
    # The catalogue reorders on parse, so a provider's own listing order in the
    # YAML never has to be trusted.
    from scraper.providers import _parse_models
    models = _parse_models([
        {"model": "c", "quality": 30},
        {"model": "a", "quality": 70},
        {"model": "b", "quality": 50},
    ])
    assert [m.model for m in models] == ["a", "b", "c"]


def test_malformed_model_entries_are_dropped_not_fatal():
    from scraper.providers import _parse_models
    models = _parse_models([
        {"quality": 90},                      # no model name
        "not-a-dict",
        {"model": "ok", "quality": "high"},   # unparseable score → 0
        {"model": "clamped", "quality": 999},
    ])
    assert [(m.model, m.quality) for m in models] == [("clamped", 100), ("ok", 0)]


def test_provider_without_key_is_not_configured(monkeypatch):
    spec = _spec(env="ROUTER_TEST_ABSENT")
    monkeypatch.delenv("ROUTER_TEST_ABSENT", raising=False)
    assert spec.configured is False
    monkeypatch.setenv("ROUTER_TEST_ABSENT", "sk-x")
    assert spec.configured is True


def test_paid_providers_excluded_unless_allowed(monkeypatch):
    monkeypatch.setenv("FREE_KEY", "k")
    monkeypatch.setenv("PAID_KEY", "k")
    cat = _catalogue(_spec("free", env="FREE_KEY"),
                     _spec("paid", paid=True, env="PAID_KEY"))
    assert [p.name for p in cat.usable()] == ["free"]
    assert [p.name for p in cat.usable(allow_paid=True)] == ["free", "paid"]


def test_missing_config_file_disables_router_instead_of_raising(tmp_path):
    cat = load_catalogue(tmp_path)  # no providers.yaml here
    assert cat.router.enabled is False
    assert cat.providers == ()


def test_every_extractor_shares_one_fingerprint_model(monkeypatch):
    # The invariant that protects ~74K cached extractions: routing must never
    # change the cache key.
    monkeypatch.setenv("FREE_KEY", "k")
    cat = _catalogue(_spec("free", env="FREE_KEY", quality=(60, 40)))
    fleet = build_extractors(cat, fingerprint_model="deepseek-chat")
    assert len(fleet) == 2
    assert {e.fingerprint_model for e in fleet} == {"deepseek-chat"}
    assert {e.model_fingerprint for e in fleet} == {fleet[0].model_fingerprint}


# ── quota ledger ─────────────────────────────────────────────────────────────

def test_ledger_counts_failures_against_budget(tmp_path):
    db = _db(tmp_path)
    ledger = QuotaLedger(db, day="2026-08-16")
    spec = _spec(rpd=100)
    ledger.note_call("groq", ok=True)
    ledger.note_call("groq", ok=False, error="boom")
    # A rejected request still consumed a slot — undercounting is how a router
    # walks into a hard block.
    assert ledger.used("groq") == 2
    assert ledger.remaining(spec) == int(100 * 0.95) - 2


def test_ledger_survives_restart(tmp_path):
    db = _db(tmp_path)
    QuotaLedger(db, day="2026-08-16").note_call("groq")
    fresh = QuotaLedger(db, day="2026-08-16")
    assert fresh.used("groq") == 1


def test_token_per_minute_429_does_not_collapse_the_daily_budget(tmp_path):
    # Groq and OpenRouter return multi-minute Retry-After for TOKEN-per-minute
    # limits. At a 2-minute threshold, one of those at call 200 would pin the
    # ceiling to 200 and end the provider's day.
    db = _db(tmp_path)
    ledger = QuotaLedger(db, day="2026-08-16")
    spec = _spec(rpd=10_000)
    for _ in range(200):
        ledger.note_call("groq")
    ledger.note_call("groq", ok=False, rate_limited=True, retry_after=300, spec=spec)
    assert ledger.budget(spec) == int(10_000 * 0.95)   # untouched


def test_minute_limit_429_does_not_collapse_the_daily_budget(tmp_path):
    # Free tiers publish rpm (10-30) alongside rpd (150-14400) and 429 on the
    # per-minute limit constantly. Recording the day's call count on one of
    # those would take Gemini from 1500/day to 15 after a single minute-limit
    # hit — the provider would be "spent" before it had really started.
    db = _db(tmp_path)
    ledger = QuotaLedger(db, day="2026-08-16")
    spec = _spec(rpd=10_000)
    for _ in range(5):
        ledger.note_call("groq")
    ledger.note_call("groq", ok=False, rate_limited=True, retry_after=20, spec=spec)
    assert ledger.budget(spec) == int(10_000 * 0.95)   # untouched
    assert ledger.blocked("groq") is True              # but backed off


def test_long_retry_after_is_read_as_a_daily_ceiling(tmp_path):
    db = _db(tmp_path)
    ledger = QuotaLedger(db, day="2026-08-16")
    spec = _spec(rpd=10_000)
    for _ in range(5):
        ledger.note_call("groq")
    ledger.note_call("groq", ok=False, rate_limited=True, retry_after=3600, spec=spec)
    # Published 10,000; refused for an hour at call 6. The observation wins.
    assert ledger.budget(spec) == int(6 * 0.95)
    # A later, higher observation must not restore optimism.
    record_provider_call(db, "2026-08-16", "groq", rate_limited=True, observed_limit=900)
    assert get_provider_usage(db, "2026-08-16")["groq"]["observed_limit"] == 6


def test_429_near_the_daily_limit_lowers_the_ceiling_even_when_brief(tmp_path):
    db = _db(tmp_path)
    ledger = QuotaLedger(db, day="2026-08-16")
    spec = _spec(rpd=10)  # budget = 9
    for _ in range(8):
        ledger.note_call("groq")
    ledger.note_call("groq", ok=False, rate_limited=True, retry_after=5, spec=spec)
    assert ledger.budget(spec) == int(9 * 0.95)


def test_rpm_paces_calls_per_provider(tmp_path):
    # Pacing must be per provider, not per extractor: a provider with two
    # models gets two extractors, each otherwise free-running at 60/min.
    db = _db(tmp_path)
    ledger = QuotaLedger(db, day="2026-08-16")
    spec = ProviderSpec(name="slow", base_url="u", api_key_env="K",
                        models=(ModelSpec(model="m", quality=50),), rpm=1, rpd=1000)
    assert ledger.available(spec) is True
    ledger.note_call("slow")
    assert ledger.paced(spec) is False      # 60s between calls at rpm=1
    assert ledger.available(spec) is False


def test_ledger_rolls_onto_a_new_utc_day(tmp_path, monkeypatch):
    # The ai_only window runs 16:35 -> 00:20 UTC. Without the roll, the run
    # keeps spending yesterday's row after midnight.
    import scraper.router as router_mod
    db = _db(tmp_path)
    ledger = QuotaLedger(db)            # follows the clock (no fixed day)
    monkeypatch.setattr(router_mod, "utc_day", lambda *a, **k: ledger.day)
    ledger.note_call("groq")
    assert ledger.used("groq") == 1
    monkeypatch.setattr(router_mod, "utc_day", lambda *a, **k: "2099-01-01")
    ledger.note_call("groq")
    assert ledger.day == "2099-01-01"
    assert ledger.used("groq") == 1      # fresh day, fresh allowance


def test_ledger_picks_up_a_concurrent_job_spend(tmp_path):
    # The enrichment job runs alongside ai_only in the same window with its own
    # ledger; without a periodic re-read they together burn ~2x the budget
    # before either notices. The DB counters are atomic, so re-reading suffices.
    db = _db(tmp_path)
    mine = QuotaLedger(db, day="2026-08-16")
    for _ in range(200):
        record_provider_call(db, "2026-08-16", "groq")   # the other process
    # Own calls up to the reload interval; the last one triggers the refresh.
    for _ in range(QuotaLedger._RELOAD_EVERY + 1):
        mine.note_call("groq")
    assert mine.used("groq") > 200


def test_exhausted_provider_is_unavailable(tmp_path):
    db = _db(tmp_path)
    ledger = QuotaLedger(db, day="2026-08-16")
    spec = _spec(rpd=3)  # budget = int(3*0.95) = 2
    assert ledger.available(spec) is True
    ledger.note_call("groq")
    ledger.note_call("groq")
    assert ledger.remaining(spec) == 0
    assert ledger.available(spec) is False


def test_ledger_without_db_still_works():
    # Admin one-offs may run before app_state.db_path is set.
    ledger = QuotaLedger(None)
    ledger.note_call("groq")
    assert ledger.used("groq") == 1


# ── routing order ────────────────────────────────────────────────────────────

def _router(tmp_path, *specs, **kw):
    db = _db(tmp_path)
    cat = _catalogue(*specs, **kw)
    ledger = QuotaLedger(db, day="2026-08-16")
    fleet = build_extractors(cat, fingerprint_model="fp", allow_paid=True)
    return ModelRouter(cat, ledger, fleet), ledger


def test_order_is_best_quality_first(tmp_path, monkeypatch):
    monkeypatch.setenv("A_KEY", "k")
    monkeypatch.setenv("B_KEY", "k")
    router, _ = _router(tmp_path,
                        _spec("a", env="A_KEY", quality=(50,)),
                        _spec("b", env="B_KEY", quality=(70,)))
    assert [e.quality for e in router.order()] == [70, 50]
    assert router.best_available_quality() == 70


def test_spent_provider_drops_out_of_the_order(tmp_path, monkeypatch):
    monkeypatch.setenv("A_KEY", "k")
    monkeypatch.setenv("B_KEY", "k")
    router, ledger = _router(tmp_path,
                             _spec("a", env="A_KEY", quality=(70,), rpd=3),
                             _spec("b", env="B_KEY", quality=(50,), rpd=1000))
    ledger.note_call("a")
    ledger.note_call("a")
    assert [e.provider for e in router.order()] == ["b"]
    assert router.best_available_quality() == 50


def test_quality_ties_break_on_remaining_quota(tmp_path, monkeypatch):
    # Published scores carry more uncertainty than the gaps between them;
    # remaining quota is measured, not estimated.
    monkeypatch.setenv("A_KEY", "k")
    monkeypatch.setenv("B_KEY", "k")
    router, ledger = _router(tmp_path,
                             _spec("a", env="A_KEY", quality=(60,), rpd=100),
                             _spec("b", env="B_KEY", quality=(60,), rpd=5000))
    assert [e.provider for e in router.order()] == ["b", "a"]


def test_disabled_router_yields_empty_fleet(tmp_path, monkeypatch):
    monkeypatch.setenv("A_KEY", "k")
    db = _db(tmp_path)
    cat = _catalogue(_spec("a", env="A_KEY"), enabled=False)
    router = build_router(db, catalogue=cat)
    assert router.enabled is False
    assert router.order() == []


# ── upgrade policy ───────────────────────────────────────────────────────────

def test_upgrade_threshold_leaves_close_calls_alone(tmp_path, monkeypatch):
    monkeypatch.setenv("A_KEY", "k")
    router, _ = _router(tmp_path, _spec("a", env="A_KEY", quality=(60,)), min_gain=8)
    # Best available is 60, so anything at 52+ is not worth re-spending on.
    assert router.upgrade_threshold() == 52


def test_upgradable_pages_are_worst_first_and_fingerprint_scoped(tmp_path):
    db = _db(tmp_path)
    for h, q, fp in (("a", 30, "fp1"), ("b", 10, "fp1"),
                     ("c", 55, "fp1"), ("d", 5, "OTHER")):
        update_cache_page(db, h, {
            "url": f"https://x/{h}", "extracted_at": "2026-08-01T00:00:00+00:00",
            "extract_fingerprint": fp, "extract_quality": q,
        }, create={"url": f"https://x/{h}"})
    rows = get_upgradable_pages(db, min_quality=52, limit=10, fingerprint="fp1")
    # 'c' scores above the bar; 'd' sits at a different fingerprint and is
    # already scheduled for ordinary re-extraction.
    assert [r["url_hash"] for r in rows] == ["b", "a"]


def test_never_extracted_page_is_not_an_upgrade_candidate(tmp_path):
    db = _db(tmp_path)
    update_cache_page(db, "fresh", {
        "url": "https://x/fresh", "extract_fingerprint": "fp1",
    }, create={"url": "https://x/fresh"})
    assert get_upgradable_pages(db, 52, 10, "fp1") == []


def test_pre_router_pages_are_never_upgrade_candidates(tmp_path):
    # NULL extract_quality means "extracted by the paid incumbent", which scores
    # ABOVE every free model. Ranking those worst-first would overwrite good
    # DeepSeek output with weaker free output — a downgrade wearing an
    # upgrade's name. ~74K existing rows are in exactly this state.
    db = _db(tmp_path)
    update_cache_page(db, "old", {
        "url": "https://x/old", "extracted_at": "2026-01-01T00:00:00+00:00",
        "extract_fingerprint": "fp1",
    }, create={"url": "https://x/old"})
    assert get_upgradable_pages(db, 52, 10, "fp1") == []


@pytest.mark.asyncio
async def test_upgrade_pass_respects_the_topic_tier_freeze(tmp_path, monkeypatch):
    # core-tier cities run only core_topics; re-extracting a frozen pair spends
    # quota on work the pipeline deliberately does not do.
    from scraper.pipeline import CityConfig, TopicConfig, _run_quality_upgrade

    monkeypatch.setenv("A_KEY", "k")
    db = _db(tmp_path)
    router, _ = _router(tmp_path, _spec("a", env="A_KEY", quality=(60,)))
    for h, topic in (("core", "running"), ("frozen", "chess")):
        update_cache_page(db, h, {
            "url": f"https://x/{h}", "city": "Kistelepules", "topic": topic,
            "extracted_at": "2026-08-01T00:00:00+00:00",
            "extract_fingerprint": "fp1", "extract_quality": 10,
            "raw_text": "A helyi futóklub minden kedden edz.",
        }, create={"url": f"https://x/{h}"})

    calls = []

    class _Extractor:
        router = None
        last_model = "a-m0"
        last_quality = 60
        canonical_fingerprint = "fp1"

        async def extract(self, **kw):
            calls.append(kw["topic"])
            return []

    ex = _Extractor()
    ex.router = router

    class _Cache:
        def save_extracted(self, *a, **k): pass

    class _Cfg:
        db_path = db
        core_topics = ["running"]

    cities = [CityConfig(name="Kistelepules", country="Hungary", locale="hu",
                         search_variants=[], topic_tier="core")]
    topics = [TopicConfig(name="running", search_terms={}),
              TopicConfig(name="chess", search_terms={})]
    await _run_quality_upgrade(cities, topics, _Cfg(), ex, _Cache(), "fp1")
    assert calls == ["running"]  # 'chess' is tiered out and must be skipped


@pytest.mark.asyncio
async def test_upgrade_pass_uses_the_city_locale_not_a_hardcoded_one(tmp_path, monkeypatch):
    # cache_pages rows carry no locale; the old fallback stamped "hu" onto every
    # record, relabelling German communities as Hungarian in the database.
    from scraper.pipeline import CityConfig, TopicConfig, _run_quality_upgrade

    monkeypatch.setenv("A_KEY", "k")
    db = _db(tmp_path)
    router, _ = _router(tmp_path, _spec("a", env="A_KEY", quality=(60,)))
    update_cache_page(db, "de", {
        "url": "https://x/de", "city": "Berlin", "topic": "running",
        "extracted_at": "2026-08-01T00:00:00+00:00",
        "extract_fingerprint": "fp1", "extract_quality": 10,
        "raw_text": "Der Laufclub trifft sich jeden Dienstag.",
    }, create={"url": "https://x/de"})

    seen = {}

    class _Extractor:
        router = None
        last_model = "a-m0"
        last_quality = 60
        canonical_fingerprint = "fp1"

        async def extract(self, **kw):
            seen.update(kw)
            return []

    ex = _Extractor()
    ex.router = router

    class _Cache:
        def save_extracted(self, *a, **k): pass

    class _Cfg:
        db_path = db
        core_topics = []

    await _run_quality_upgrade(
        [CityConfig(name="Berlin", country="Germany", locale="de", search_variants=[])],
        [TopicConfig(name="running", search_terms={})],
        _Cfg(), ex, _Cache(), "fp1")
    assert seen["locale"] == "de"


# ── admin page ───────────────────────────────────────────────────────────────

def test_providers_admin_page_lists_the_fleet(tmp_path):
    import base64
    from unittest.mock import patch

    from fastapi.testclient import TestClient

    from scraper.web import app as web_app
    from scraper.web.state import app_state

    db = _db(tmp_path)
    old = app_state.db_path
    app_state.db_path = db
    # _ADMIN_PASSWORD is read at import time, so patch the module attribute
    # rather than the env var.
    with patch("scraper.web.app._ADMIN_PASSWORD", "t"):
        auth = {"Authorization": "Basic " + base64.b64encode(b"admin:t").decode()}
        resp = TestClient(web_app.app).get("/admin/providers", headers=auth)
        assert resp.status_code == 200
        for provider in ("groq", "cerebras", "gemini", "mistral", "openrouter",
                         "github", "deepseek"):
            assert provider in resp.text
        # A provider with no key must be shown, not hidden — the env var name is
        # the actionable part of the page.
        assert "GROQ_API_KEY" in resp.text
        assert "Free only" in resp.text  # paid is parked by default
    app_state.db_path = old


# ── regressions from review round 2 ──────────────────────────────────────────

@pytest.mark.asyncio
async def test_rpm_pacing_waits_instead_of_failing(tmp_path, monkeypatch):
    """Pacing must never look like unavailability.

    `_call` only sleeps on `_blocked_until`, which pacing does not set. When the
    single configured provider was merely paced, the loop tried nothing, raised,
    and `_note_failure` counted it — 24 "failures" a minute at the shipped
    rpm: 30, so the breaker opened and aborted the run within seconds. This is
    the expected state during rollout, when only one or two keys are set.
    """
    from scraper.extract import FallbackExtractor

    monkeypatch.setenv("A_KEY", "k")
    # rpm 600 → a 0.1s cooldown, so the test exercises the real sleep path
    # without being slow.
    spec = ProviderSpec(name="a", base_url="u", api_key_env="A_KEY",
                        models=(ModelSpec(model="a-m0", quality=60),),
                        rpm=600, rpd=1000)
    router, _ = _router(tmp_path, spec)
    calls = []

    class _Model:
        provider, model, quality = "a", "a-m0", 60

        async def extract(self, *a, **kw):
            calls.append(1)
            return []

    chain = FallbackExtractor(primaries=[_Model()], router=router)
    for i in range(3):
        await chain.extract(text="t", city="c", topic="running", locale="hu",
                            source_url=f"https://x/{i}")
    assert len(calls) == 3                      # all served, none dropped
    assert chain._consecutive_failures == 0     # nothing counted as a failure
    assert chain.providers_down is False        # breaker never opened


@pytest.mark.asyncio
async def test_spent_quota_raises_the_type_callers_catch(tmp_path, monkeypatch):
    """A spent daily budget must surface as ExtractorUnavailableError.

    ExtractorQuotaError is not a subclass of it, so raising that sailed past
    every `except ExtractorUnavailableError` in the pipeline and failed the run
    — the exact outcome the clean-stop path exists to prevent.
    """
    from scraper.extract import ExtractorUnavailableError, FallbackExtractor

    monkeypatch.setenv("A_KEY", "k")
    router, ledger = _router(tmp_path, _spec("a", env="A_KEY", quality=(60,), rpd=2))
    for _ in range(2):
        ledger.note_call("a")
    assert router.has_capacity() is False

    class _Model:
        provider, model, quality = "a", "a-m0", 60

        async def extract(self, *a, **kw):
            raise AssertionError("must not reach the provider")

    chain = FallbackExtractor(primaries=[_Model()], router=router)
    with pytest.raises(ExtractorUnavailableError):
        await chain.extract(text="t", city="c", topic="running", locale="hu",
                            source_url="https://x/1")
    # …and it is flagged as quota, not as an outage, so the caller stops cleanly.
    assert chain.quota_exhausted is True
    assert chain.providers_down is False


def test_chain_is_built_from_the_whole_fleet_not_todays_survivors(tmp_path, monkeypatch):
    """A provider spent at 16:35 must be able to return after the UTC rollover.

    The chain is built once for a run that spans the off-peak window and crosses
    midnight; filtering by live quota at build time would exclude a provider for
    the whole run, and the ledger's day-rollover could never readmit it.
    """
    monkeypatch.setenv("A_KEY", "k")
    monkeypatch.setenv("B_KEY", "k")
    router, ledger = _router(tmp_path,
                             _spec("a", env="A_KEY", quality=(70,), rpd=2),
                             _spec("b", env="B_KEY", quality=(50,), rpd=1000))
    for _ in range(2):
        ledger.note_call("a")
    assert [e.provider for e in router.order()] == ["b"]          # spent now
    assert [e.provider for e in router.all_extractors()] == ["a", "b"]


# ── regressions from review round 3 ──────────────────────────────────────────

def test_capacity_checks_ignore_momentary_rpm_pacing(tmp_path, monkeypatch):
    """`order()` answers "callable this instant"; planning needs "callable today".

    Preflight stamps every provider's clock, so an order()-based capacity check
    read "no free capacity left today" for the next 60/rpm seconds — which made
    the upgrade sweep skip entirely and the gateway answer a spurious 429.
    """
    monkeypatch.setenv("A_KEY", "k")
    router, ledger = _router(tmp_path, _spec("a", env="A_KEY", quality=(62,)))
    assert router.best_available_quality() == 62
    ledger.note_call("a")                       # now inside the rpm window
    assert router.order() == []                 # nothing callable this instant
    assert router.best_available_quality() == 62  # …but the day is not spent
    assert router.has_capacity() is True
    assert [e.quality for e in router.with_budget()] == [62]


def test_capacity_can_be_scoped_to_one_requested_model(tmp_path, monkeypatch):
    # The gateway serves a single pinned model; unrelated providers' spare
    # capacity must not make an exhausted request look servable.
    monkeypatch.setenv("A_KEY", "k")
    monkeypatch.setenv("B_KEY", "k")
    router, ledger = _router(tmp_path,
                             _spec("a", env="A_KEY", quality=(70,), rpd=2),
                             _spec("b", env="B_KEY", quality=(50,), rpd=1000))
    for _ in range(2):
        ledger.note_call("a")
    pinned = [e for e in router.all_extractors() if e.provider == "a"]
    assert router.has_capacity() is True            # 'b' still has budget
    assert router.has_capacity(pinned) is False     # but 'a' does not


def test_upgrade_candidates_are_city_scoped_in_sql(tmp_path):
    """The city filter must run before LIMIT.

    The caller sweeps one country group at a time. Filtering after a global
    LIMIT can return nothing while thousands of eligible pages sit lower in the
    ordering.
    """
    db = _db(tmp_path)
    for h, city, q in (("de1", "Berlin", 1), ("de2", "Berlin", 2),
                       ("hu1", "Szentendre", 30)):
        update_cache_page(db, h, {
            "url": f"https://x/{h}", "city": city, "topic": "running",
            "extracted_at": "2026-08-01T00:00:00+00:00",
            "extract_fingerprint": "fp1", "extract_quality": q,
        }, create={"url": f"https://x/{h}"})
    # Worst-first globally would return the two Berlin rows and drop Szentendre.
    rows = get_upgradable_pages(db, 52, 2, "fp1", cities=["Szentendre"])
    assert [r["url_hash"] for r in rows] == ["hu1"]
    assert get_upgradable_pages(db, 52, 2, "fp1", cities=[]) == []


def test_upgrade_pair_log_renders_in_run_detail(tmp_path):
    """The sweep's pair log must carry every key run_detail.html touches.

    _new_pair_log's own docstring says so: the template compares
    `p.records_extracted > 0` under strict Jinja Undefined, and a hand-rolled
    partial dict raises UndefinedError — 500ing the whole admin page for any run
    that included a sweep.
    """
    from scraper.pipeline import _new_pair_log

    canonical = set(_new_pair_log("c", "t", []))
    entry = _new_pair_log("—", "quality_upgrade", [])
    entry.update({"urls_found": 3, "records_extracted": 2,
                  "extract_failed": 1, "cache_hits_extract": 2})
    assert canonical <= set(entry)
    for key in ("records_extracted", "queries", "cache_hits_scrape",
                "fetched_urls", "search_failed", "aborted"):
        assert key in entry


@pytest.mark.asyncio
async def test_retired_model_is_dropped_for_the_run_not_retried(monkeypatch, tmp_path):
    """HTTP 404/410 means the model is gone — retrying costs one request a page.

    Live rollout on 2026-08-16: Groq's qwen3-32b, Gemini 2.5-flash, three
    OpenRouter ":free" slugs and all of GitHub Models (410 retirement brownout)
    404'd on *every* enrichment record, because the generic >=400 branch raised
    the transient error type.
    """
    from scraper.extract import (ExtractorModelError, FallbackExtractor)

    monkeypatch.setenv("A_KEY", "k")
    # rpm 600 -> a 0.1s cooldown: both models sit on one provider clock, so the
    # chain must be able to wait it out rather than give up.
    spec = ProviderSpec(name="a", base_url="u", api_key_env="A_KEY",
                        models=(ModelSpec(model="a-m0", quality=60),
                                ModelSpec(model="a-m1", quality=40)),
                        rpm=600, rpd=1000)
    router, _ = _router(tmp_path, spec)
    calls = []

    class _Dead:
        provider, model, quality = "a", "a-m0", 60

        async def extract(self, *a, **kw):
            calls.append("dead")
            raise ExtractorModelError("a:a-m0 HTTP 404")

    class _Live:
        provider, model, quality = "a", "a-m1", 40

        async def extract(self, *a, **kw):
            calls.append("live")
            return []

    chain = FallbackExtractor(primaries=[_Dead(), _Live()], router=router)
    for i in range(3):
        await chain.extract(text="t", city="c", topic="running", locale="hu",
                            source_url=f"https://x/{i}")
    # Probed once, then never again; the live model served the rest.
    assert calls.count("dead") == 1
    assert calls.count("live") == 3
    # A stale catalogue entry is a config problem, not a provider outage.
    assert chain._consecutive_failures == 0
    assert chain.providers_down is False

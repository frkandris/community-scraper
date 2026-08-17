"""Quota-aware model router over the free-tier provider fleet.

Design follows the arXiv reading in
`docs/wiki/sources/2026-08-16-llm-routing-arxiv-research.md`:

* **Route before generating, do not cascade.** A cascade pays the cheap model's
  generation before every escalation decision; a pre-generation router beat the
  best cascade policy on 4 of 5 datasets (arXiv:2605.06350) precisely by not
  paying that. So: pick the best model that still has budget, call it once.
* **Budget at the window level, not per query.** Per-query routing cannot control
  batch-level spend under non-uniform load (arXiv:2603.26796). Free tiers are
  hard per-day capacities, so the ledger is the constraint the router optimizes
  against — not a nice-to-have counter.
* **Learn the real limit from 429s.** Most free providers stopped publishing
  their numbers; a config constant is a guess, an observed ceiling is a fact.
* **Break quality ties by remaining quota**, not by score decimals — published
  scores carry more uncertainty than the gaps between them (arXiv:2606.13221).
* **New work outranks re-work.** Re-extraction is only ever a filler for quota
  that would otherwise expire unused.

The router does not replace `FallbackExtractor`; it *orders and filters* the
list handed to it. That keeps one failure path (circuit breaker, typed errors,
retry semantics) for every provider.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path

import structlog

from .db import get_provider_usage, record_provider_call
from .providers import (ProviderCatalogue, ProviderSpec, OpenAICompatExtractor,
                        build_extractors, load_catalogue)

log = structlog.get_logger()

#: Stop using a provider at this fraction of its daily allowance, leaving room
#: for the enrichment job and admin-triggered work that share the same key.
_DAILY_HEADROOM = 0.95


def utc_day(now: datetime | None = None) -> str:
    return (now or datetime.now(timezone.utc)).strftime("%Y-%m-%d")


class QuotaLedger:
    """Per-provider daily budget, persisted so restarts cannot forge capacity.

    Free allowances are calendar-day budgets. Holding them in memory would hand
    a provider a fresh 14,400 requests on every container restart, and the
    provider would answer with hard 429s the router could not have predicted.
    """

    #: Re-read the DB after this many locally counted calls. The enrichment job
    #: runs concurrently with ai_only in the same off-peak window and holds its
    #: own ledger; without a periodic refresh neither sees the other's spend and
    #: together they burn roughly 2× the budget before either notices. The DB
    #: counters are atomic, so re-reading is all that is needed.
    _RELOAD_EVERY = 25

    #: Per-provider monotonic timestamp of the last attempt, for rpm pacing.
    #: **Class-level, so every ledger in the process shares it.** The gateway
    #: builds a fresh router per request; with per-instance state each HTTP call
    #: would start with an empty clock and rpm pacing would not exist on that
    #: path at all, while the long-lived pipeline router honoured it. The
    #: provider sees one client per container, so the container is the right
    #: granularity.
    #:
    #: In memory rather than in the DB on purpose: a per-minute limit is
    #: meaningless across a restart, unlike the daily counters.
    _last_call: dict[str, float] = {}

    def __init__(self, db_path: Path | None, day: str | None = None):
        self.db_path = db_path
        #: None → follow the wall clock, so a run crossing midnight UTC rolls
        #: onto the new day's allowance. A fixed value pins the day (tests).
        self._fixed_day = day
        self.day = day or utc_day()
        self._usage: dict[str, dict] = {}
        #: Calls claimed but not yet reported, per provider. Per instance, not
        #: shared like `_last_call`: a reservation belongs to a call this
        #: ledger is waiting on. Must exist before `reload`, which re-applies
        #: them over the freshly-read database counts.
        self._reserved: dict[str, int] = {}
        self._since_reload = 0
        self.reload()

    def reload(self) -> None:
        self._since_reload = 0
        if not self.db_path:
            self._usage = {}
        else:
            try:
                self._usage = get_provider_usage(self.db_path, self.day)
            except Exception as exc:  # a broken ledger must not stop extraction
                log.warning("quota_ledger_unreadable", error=str(exc))
                self._usage = {}
        # Reservations for calls still in flight are not in the database yet —
        # `note_call` writes them when the call returns. Re-applying them keeps
        # a periodic reload from handing the same capacity out twice.
        for provider, count in self._reserved.items():
            if count > 0:
                self._row(provider)["calls"] = int(
                    self._row(provider).get("calls") or 0) + count

    def _sync(self) -> None:
        """Roll onto a new UTC day and pick up other processes' spend.

        The ai_only window runs 16:35 → 00:20 UTC. Without the roll, the run
        keeps reading yesterday's row after midnight: providers look exhausted
        for the first 20 minutes of a day whose allowance just reset, and the
        calls are attributed to the wrong date.
        """
        if not self._fixed_day:
            today = utc_day()
            if today != self.day:
                log.info("quota_ledger_day_rollover", from_day=self.day, to_day=today)
                self.day = today
                self._last_call.clear()
                self.reload()
                return
        if self._since_reload >= self._RELOAD_EVERY:
            self.reload()

    def _row(self, provider: str) -> dict:
        return self._usage.setdefault(
            provider,
            {"calls": 0, "failures": 0, "rate_limits": 0,
             "observed_limit": None, "blocked_until": 0.0, "last_error": None},
        )

    def budget(self, spec: ProviderSpec) -> int:
        """Effective daily allowance: the observed ceiling when we have proven
        one, otherwise the configured (published, possibly stale) number."""
        observed = self._row(spec.name).get("observed_limit")
        limit = observed if observed else spec.rpd
        return max(0, int(limit * _DAILY_HEADROOM))

    def used(self, provider: str) -> int:
        return int(self._row(provider).get("calls") or 0)

    def remaining(self, spec: ProviderSpec) -> int:
        return max(0, self.budget(spec) - self.used(spec.name))

    def blocked(self, provider: str) -> bool:
        """True while a 429's Retry-After window is still open.

        `blocked_until` is a wall-clock epoch, not `time.monotonic()`, because
        it has to survive a process restart — monotonic clocks reset.
        """
        return time.time() < float(self._row(provider).get("blocked_until") or 0)

    def paced(self, spec: ProviderSpec) -> bool:
        """False while `rpm` says the next call is too soon."""
        return self.pace_wait(spec) <= 0

    def pace_wait(self, spec: ProviderSpec) -> float:
        """Seconds until this provider's rpm window reopens (0 = ready now).

        Pacing is a *wait*, never a veto. Treating it as unavailability makes
        `_call` find nothing to try, which the circuit breaker counts as a
        failure — at the shipped `rpm: 30` that is 24 consecutive "failures" per
        minute and an aborted run within seconds.
        """
        last = self._last_call.get(spec.name)
        if last is None:
            return 0.0
        return max(0.0, spec.min_interval_s - (time.monotonic() - last))

    def available(self, spec: ProviderSpec) -> bool:
        self._sync()
        return (self.remaining(spec) > 0
                and not self.blocked(spec.name)
                and self.paced(spec))

    #: A 429 must ask us to wait at least this long before it is read as a spent
    #: daily allowance. Deliberately far above a minute: Groq and OpenRouter
    #: return multi-minute Retry-After for *token*-per-minute limits, and at a
    #: 2-minute threshold a TPM 429 at call 200 would pin observed_limit to 200
    #: and end the provider's day — persisted, and only ever lowered. A genuine
    #: daily cap points at the next UTC midnight, which is hours away.
    _DAILY_429_RETRY_AFTER = 1800.0

    def release_call(self, provider: str) -> None:
        """Give back a slot claimed for a call that never ran.

        Cancellation is the reason this exists: `asyncio.CancelledError` is a
        BaseException, so it slips past every `except Exception` between the
        reservation and `note_call`, and the slot would stay charged for the
        life of the process.
        """
        if self._reserved.get(provider):
            self._reserved[provider] -= 1
            row = self._row(provider)
            row["calls"] = max(0, int(row.get("calls") or 0) - 1)

    def reserve_call(self, provider: str) -> None:
        """Take a request slot before issuing the call, not after it returns.

        `note_call` records the outcome, which is too late to keep concurrent
        callers apart: while a call is in flight the provider still looks idle
        and under budget, so every waiting task picks the same one, blows its
        rpm together and collects a fleet's worth of 429s. Claiming the slot at
        selection time is what makes the second task look at the next provider.

        Serial callers are unaffected: the same slot is claimed either way, only
        earlier, and rpm is a limit on requests *started* per minute anyway.
        """
        self._sync()
        row = self._row(provider)
        row["calls"] = int(row.get("calls") or 0) + 1
        self._reserved[provider] = self._reserved.get(provider, 0) + 1
        self._last_call[provider] = time.monotonic()

    def note_call(
        self, provider: str, *, ok: bool = True, rate_limited: bool = False,
        retry_after: float | None = None, error: str | None = None,
        spec: ProviderSpec | None = None, reserved: bool = False,
    ) -> None:
        """Record one attempt. Counts even when it failed — a rejected request
        still consumed a slot at most providers, and undercounting is exactly
        how a router walks into a hard block."""
        self._sync()
        row = self._row(provider)
        if reserved:
            # The slot was claimed by reserve_call, which also stamped the
            # pacing clock at call *start*. Counting again would double the
            # day's usage, and re-stamping would push the next call to rpm
            # seconds after this one finished rather than after it began. The
            # reservation is settled here: the call is about to be persisted.
            if self._reserved.get(provider):
                self._reserved[provider] -= 1
        else:
            row["calls"] = int(row.get("calls") or 0) + 1
            self._last_call[provider] = time.monotonic()
        self._since_reload += 1
        if not ok:
            row["failures"] = int(row.get("failures") or 0) + 1
        blocked_until = None
        observed_limit = None
        if rate_limited:
            row["rate_limits"] = int(row.get("rate_limits") or 0) + 1
            wait = float(retry_after or 60)
            blocked_until = time.time() + wait
            row["blocked_until"] = blocked_until
            # Only a *daily* refusal tells us anything about the daily ceiling.
            # Free tiers publish both rpm (10-30) and rpd (150-14400) and 429 on
            # the per-minute limit constantly; recording the day's call count on
            # one of those would collapse Gemini's 1500/day to 15 after a single
            # minute-limit hit. A long Retry-After, or already being near the
            # configured rpd, is what distinguishes the two.
            # Near the configured allowance, a 429 of any length is much more
            # likely to be the daily cap than a burst limit.
            near_daily = spec is not None and row["calls"] >= 0.8 * self.budget(spec)
            if wait >= self._DAILY_429_RETRY_AFTER or near_daily:
                observed_limit = row["calls"]
                prev = row.get("observed_limit")
                row["observed_limit"] = min(prev, observed_limit) if prev else observed_limit
            else:
                log.info("provider_minute_limit", provider=provider, wait_s=round(wait, 1))
        if error:
            row["last_error"] = error[:200]
        if not self.db_path:
            return
        try:
            record_provider_call(
                self.db_path, self.day, provider, ok=ok, rate_limited=rate_limited,
                blocked_until=blocked_until, error=error, observed_limit=observed_limit,
            )
        except Exception as exc:
            log.warning("quota_ledger_write_failed", provider=provider, error=str(exc))

    def snapshot(self, catalogue: ProviderCatalogue) -> list[dict]:
        """Per-provider state for the admin page and the daily email."""
        out = []
        for spec in catalogue.providers:
            row = self._row(spec.name)
            out.append({
                "name": spec.name,
                "paid": spec.paid,
                "enabled": spec.enabled,
                "configured": spec.configured,
                "key_env": spec.api_key_env,
                "best_quality": max((m.quality for m in spec.models), default=0),
                "models": [{"model": m.model, "quality": m.quality} for m in spec.models],
                "budget": self.budget(spec),
                "used": self.used(spec.name),
                "remaining": self.remaining(spec),
                "rate_limits": int(row.get("rate_limits") or 0),
                "failures": int(row.get("failures") or 0),
                "observed_limit": row.get("observed_limit"),
                "blocked": self.blocked(spec.name),
                "last_error": row.get("last_error"),
            })
        return sorted(out, key=lambda p: (p["paid"], -p["best_quality"]))


class ModelRouter:
    """Orders the extractor fleet for one run and attributes calls to quota."""

    def __init__(
        self,
        catalogue: ProviderCatalogue,
        ledger: QuotaLedger,
        extractors: list[OpenAICompatExtractor],
    ):
        self.catalogue = catalogue
        self.ledger = ledger
        self._all = extractors
        self._specs = {p.name: p for p in catalogue.providers}

    @property
    def enabled(self) -> bool:
        return self.catalogue.router.enabled and bool(self._all)

    def order(self) -> list[OpenAICompatExtractor]:
        """Extractors that still have budget, best quality first.

        Ties are broken by remaining quota rather than by score decimals: the
        gap between two published scores is usually smaller than the uncertainty
        in either of them, while remaining quota is measured, not estimated.
        """
        usable = [e for e in self._all if self.can_use(e) and e.provider in self._specs]
        usable.sort(key=lambda e: (-e.quality,
                                   -self.ledger.remaining(self._specs[e.provider])))
        return usable

    def all_extractors(self) -> list[OpenAICompatExtractor]:
        """The whole fleet, best-quality first, regardless of current quota.

        For building a long-lived chain: `order()` is a point-in-time view, and
        a run spanning the off-peak window must be able to pick a provider back
        up after the ledger rolls onto a new day.
        """
        return sorted(self._all, key=lambda e: -e.quality)

    def has_capacity(self, scope: list | None = None) -> bool:
        """True when at least one provider still has *daily* budget left.

        Distinct from `order()` being non-empty, which is also false while every
        provider is merely paced or inside a short back-off. Callers use this to
        tell "we are done for today" from "wait a moment".

        `scope` narrows the question to a specific set of extractors — the
        gateway serves one explicitly requested model, and unrelated providers'
        spare capacity must not make that request look servable.
        """
        self.ledger._sync()
        pool = scope if scope is not None else self._all
        return any(self.ledger.remaining(spec) > 0
                   for spec in {self._specs[e.provider] for e in pool
                                if e.provider in self._specs})

    def with_budget(self) -> list[OpenAICompatExtractor]:
        """Extractors whose provider still has *daily budget*, best first.

        Pacing-blind on purpose. `order()` answers "what can I call this
        instant", which is the right question for picking a provider but the
        wrong one for planning: an rpm cooldown is a two-second wait, not an
        exhausted allowance. Conflating them made the upgrade sweep read "no
        capacity" right after preflight stamped every provider's clock, and made
        the gateway answer 429 "no quota left today" to any caller faster than
        one request every two seconds.
        """
        usable = [e for e in self._all
                  if e.provider in self._specs
                  and self.ledger.remaining(self._specs[e.provider]) > 0]
        return sorted(usable, key=lambda e: -e.quality)

    def best_available_quality(self) -> int:
        """Best quality reachable today — ignoring momentary rpm cooldowns."""
        with_budget = self.with_budget()
        return with_budget[0].quality if with_budget else 0

    def reserve(self, extractor) -> bool:
        """Claim a request slot for this extractor's provider. True if claimed."""
        provider = getattr(extractor, "provider", None)
        if not provider:
            return False
        self.ledger.reserve_call(provider)
        return True

    def release(self, extractor) -> None:
        """Give back a slot claimed for a call that never reported an outcome."""
        provider = getattr(extractor, "provider", None)
        if provider:
            self.ledger.release_call(provider)

    def note(self, extractor, **kwargs) -> None:
        """Attribute one call to the extractor's provider bucket."""
        provider = getattr(extractor, "provider", None)
        if provider:
            # The spec lets the ledger tell a per-minute 429 from a spent daily
            # allowance — see QuotaLedger.note_call.
            self.ledger.note_call(provider, spec=self._specs.get(provider), **kwargs)

    def spec_for(self, extractor) -> ProviderSpec | None:
        """Public accessor so FallbackExtractor need not reach into _specs."""
        return self._specs.get(getattr(extractor, "provider", ""))

    def can_use(self, extractor) -> bool:
        """Is this extractor's provider within budget, pacing and back-off?"""
        spec = self.spec_for(extractor)
        return spec is None or self.ledger.available(spec)

    def pace_wait(self, extractor) -> float:
        """Seconds until this extractor's provider is rpm-ready (0 = now).

        Returns 0 for anything that is unavailable for a *different* reason —
        spent budget or a 429 back-off — so callers do not mistake those for
        something a short sleep can fix.
        """
        spec = self.spec_for(extractor)
        if spec is None:
            return 0.0
        if self.ledger.remaining(spec) <= 0 or self.ledger.blocked(spec.name):
            return 0.0
        return self.ledger.pace_wait(spec)

    def shortest_pace_wait(self) -> float:
        """Shortest rpm wait across the fleet, or 0 when none is merely paced."""
        waits = [w for w in (self.pace_wait(e) for e in self._all) if w > 0]
        return min(waits) if waits else 0.0

    def quality_of(self, extractor) -> int:
        return int(getattr(extractor, "quality", 0) or 0)

    def upgrade_threshold(self) -> int:
        """Cached-extraction quality below which a re-run is worth a request.

        A page is only re-extracted when the best model we can currently reach
        scores at least `upgrade_min_gain` points above whatever produced the
        cached result. Below that the expected gain does not justify spending a
        request that new pages could use.

        Can legitimately be 0 when the best available model scores at or under
        `upgrade_min_gain` — that means "nothing is worth upgrading", which the
        candidate query expresses naturally (`quality < 0` matches nothing).
        Callers must not read 0 as "no capacity"; use `best_available_quality()`
        for that.
        """
        best = self.best_available_quality()
        gain = max(0, self.catalogue.router.upgrade_min_gain)
        return max(0, best - gain)


def build_router(
    db_path: Path | None,
    *,
    temperature: float = 0.1,
    timeout_seconds: int = 60,
    max_text_chars: int = 8000,
    rate_limit_seconds: float = 1.0,
    fingerprint_model: str | None = None,
    catalogue: ProviderCatalogue | None = None,
) -> ModelRouter:
    """Assemble catalogue + ledger + extractor fleet.

    Returns a disabled router (empty fleet) when `router.enabled` is off or no
    provider has a key, so callers can fall back to the single-provider path
    without special-casing.
    """
    cat = catalogue or load_catalogue()
    ledger = QuotaLedger(db_path)
    if not cat.router.enabled:
        return ModelRouter(cat, ledger, [])
    extractors = build_extractors(
        cat,
        temperature=temperature,
        timeout_seconds=timeout_seconds,
        max_text_chars=max_text_chars,
        rate_limit_seconds=rate_limit_seconds,
        fingerprint_model=fingerprint_model,
    )
    if extractors:
        log.info("model_router_ready",
                 fleet=[f"{e.provider}:{e.model}" for e in extractors[:6]],
                 total=len(extractors))
    return ModelRouter(cat, ledger, extractors)

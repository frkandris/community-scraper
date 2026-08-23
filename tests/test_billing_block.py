"""A provider that answers HTTP 402 must stay out for the rest of the day.

On 2026-08-22 Cerebras answered `billing limit (HTTP 402)` to **all 283** calls
it received. The 402 marked the provider exhausted for the *run*, and the
continuous worker builds a new extractor every few minutes, so each run handed
its best pick — Cerebras sits first at quality 80 — to a provider with no
credit. The ledger showed `blocked: false` and 473 requests still "remaining".
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path

from scraper.db import init_db
from scraper.providers import ProviderSpec
from scraper.router import QuotaLedger


def _spec() -> ProviderSpec:
    return ProviderSpec(name="cerebras", base_url="https://x.test/v1",
                        api_key_env="CEREBRAS_API_KEY", models=(), rpd=1000, rpm=30)


def test_a_billing_refusal_blocks_the_provider(tmp_path: Path):
    db = tmp_path / "scraper.db"
    init_db(db)
    ledger = QuotaLedger(db)
    assert not ledger.blocked("cerebras")

    ledger.note_call("cerebras", ok=False, spec=_spec(),
                     error="OpenAICompatExtractor billing limit (HTTP 402)",
                     billing_blocked=True)
    assert ledger.blocked("cerebras")


def test_the_block_survives_a_new_ledger(tmp_path: Path):
    """The worker rebuilds everything every few minutes — in-memory is useless."""
    db = tmp_path / "scraper.db"
    init_db(db)
    QuotaLedger(db).note_call("cerebras", ok=False, spec=_spec(),
                              error="billing limit (HTTP 402)", billing_blocked=True)
    assert QuotaLedger(db).blocked("cerebras")


def test_the_block_expires_at_the_next_utc_midnight(tmp_path: Path):
    """Free allowances and trial credit both reset on the UTC day, not in an hour."""
    db = tmp_path / "scraper.db"
    init_db(db)
    ledger = QuotaLedger(db)
    ledger.note_call("cerebras", ok=False, spec=_spec(),
                     error="billing limit (HTTP 402)", billing_blocked=True)
    until = float(ledger._row("cerebras")["blocked_until"])
    midnight = (datetime.now(timezone.utc) + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0).timestamp()
    assert abs(until - midnight) < 2


def test_an_ordinary_failure_does_not_block(tmp_path: Path):
    """A 500 is transient. Blocking on it would retire the fleet in a bad minute."""
    db = tmp_path / "scraper.db"
    init_db(db)
    ledger = QuotaLedger(db)
    ledger.note_call("cerebras", ok=False, spec=_spec(), error="HTTP 500")
    assert not ledger.blocked("cerebras")

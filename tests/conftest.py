"""Shared test fixtures."""
import pytest


@pytest.fixture(autouse=True)
def _reset_pacing_clock():
    """Clear the router's process-wide rpm clock between tests.

    `QuotaLedger._last_call` is deliberately class-level so every ledger in the
    process shares it — the gateway builds a fresh router per request, and
    per-instance state would mean no rpm pacing on that path at all. The cost is
    that it also leaks across tests, so it is reset here rather than each test
    remembering to.
    """
    from scraper.router import QuotaLedger

    QuotaLedger._last_call.clear()
    yield
    QuotaLedger._last_call.clear()

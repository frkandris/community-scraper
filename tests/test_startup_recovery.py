"""Startup crash-recovery planning.

A mid-window deploy/restart kills the in-flight saver run (observed 2026-07-24/25:
the 01:00 search_only collector was cancelled hours in). The startup recovery must
resume the interrupted *bounded* run and box it to the same window its cron twin
would use — but must NOT launch a `full` run on a clean saver boot (that would run
DeepSeek outside the off-peak split). Legacy (saver-disabled) escalation is unchanged.
"""
from datetime import datetime, timezone

from scraper.main import _startup_plan, _startup_until, _within_window

SAVER = {
    "saver_enabled": True,
    "search_until": "16:20",
    "extract_until": "00:20",
}
LEGACY = {"saver_enabled": False}
NOW = datetime(2026, 7, 25, 6, 0, tzinfo=timezone.utc)  # mid search window


def _row(run_mode, finished_at="x", success=True):
    return {"id": 1, "run_mode": run_mode, "finished_at": finished_at, "success": success}


# ── _startup_until ─────────────────────────────────────────────────────────
def test_startup_until_maps_mode_to_saver_window():
    assert _startup_until("search_only", SAVER) == "16:20"
    assert _startup_until("ai_only", SAVER) == "00:20"
    assert _startup_until("full", SAVER) is None


# ── saver mode: crash-recovery net only ────────────────────────────────────
def test_saver_resumes_interrupted_search_only_boxed_to_window():
    mode, stop_at = _startup_plan(_row("search_only", finished_at=None), SAVER, NOW)
    assert mode == "search_only"
    assert stop_at == datetime(2026, 7, 25, 16, 20, tzinfo=timezone.utc)


def test_saver_resumes_failed_ai_only_boxed_to_extract_window():
    # last run failed (success=False) but finished — still recover it.
    night = datetime(2026, 7, 25, 20, 0, tzinfo=timezone.utc)
    mode, stop_at = _startup_plan(_row("ai_only", success=False), SAVER, night)
    assert mode == "ai_only"
    # extract window end 00:20 is the NEXT day relative to 20:00
    assert stop_at == datetime(2026, 7, 26, 0, 20, tzinfo=timezone.utc)


def test_saver_clean_boot_does_nothing():
    # A successful last run means a normal deploy — the crons drive the day,
    # startup must NOT escalate to a full (LLM) run.
    assert _startup_plan(_row("search_only", success=True), SAVER, NOW) == (None, None)
    assert _startup_plan(_row("ai_only", success=True), SAVER, NOW) == (None, None)


def test_saver_does_not_recover_a_warning_run():
    # 'warning' = the run finished, some pairs failed and were never cached. The
    # next scheduled run retries them; re-running here would just duplicate the
    # window's work (2026-07-31, see run-outcome-three-states).
    warned = {**_row("ai_only", success=True), "outcome": "warning"}
    assert _startup_plan(warned, SAVER, NOW) == (None, None)


def test_saver_recovers_an_aborted_run():
    night = datetime(2026, 7, 25, 20, 0, tzinfo=timezone.utc)
    aborted = {**_row("ai_only", success=False), "outcome": "aborted"}
    mode, stop_at = _startup_plan(aborted, SAVER, night)
    assert mode == "ai_only"
    assert stop_at == datetime(2026, 7, 26, 0, 20, tzinfo=timezone.utc)


def test_saver_ignores_interrupted_non_bounded_mode():
    # A stray interrupted `full`/legacy row must not trigger a full run under saver.
    assert _startup_plan(_row("full", finished_at=None), SAVER, NOW) == (None, None)


def test_saver_no_history_does_nothing():
    assert _startup_plan(None, SAVER, NOW) == (None, None)


def test_saver_skips_recovery_outside_the_interrupted_modes_window():
    # ai_only interrupted, but restart at 01:00 UTC is inside the COLLECTOR window,
    # not the extract window — resuming would run DeepSeek ~23 h and block the
    # 01:00 collector via the shared coordinator. Must skip.
    at_0100 = datetime(2026, 7, 25, 1, 0, tzinfo=timezone.utc)
    assert _startup_plan(_row("ai_only", finished_at=None), SAVER, at_0100) == (None, None)
    # search_only interrupted, but restart at 17:00 is inside the extract window —
    # must skip so it doesn't starve the nightly extractor.
    at_1700 = datetime(2026, 7, 25, 17, 0, tzinfo=timezone.utc)
    assert _startup_plan(_row("search_only", finished_at=None), SAVER, at_1700) == (None, None)


# ── _within_window ─────────────────────────────────────────────────────────
def test_within_window_same_day_and_midnight_cross():
    def at(h, m=0):
        return datetime(2026, 7, 25, h, m, tzinfo=timezone.utc)

    # collector 01:00 → 16:20 (same day)
    assert _within_window(at(6), "01:00", "16:20") is True
    assert _within_window(at(0, 30), "01:00", "16:20") is False
    assert _within_window(at(16, 30), "01:00", "16:20") is False
    # extractor 16:35 → 00:20 (crosses midnight)
    assert _within_window(at(20), "16:35", "00:20") is True
    assert _within_window(at(0, 10), "16:35", "00:20") is True
    assert _within_window(at(1), "16:35", "00:20") is False
    # unparseable bound → permissive (recover rather than silently skip)
    assert _within_window(at(3), None, "16:20") is True


# ── legacy (saver disabled): escalation unchanged, always unbounded ─────────
def test_legacy_interrupted_resumes_same_mode_unbounded():
    assert _startup_plan(_row("search_only", finished_at=None), LEGACY, NOW) == ("search_only", None)


def test_legacy_clean_escalates_ai_only_to_full():
    assert _startup_plan(_row("ai_only", success=True), LEGACY, NOW) == ("full", None)


def test_legacy_no_history_runs_full():
    assert _startup_plan(None, LEGACY, NOW) == ("full", None)

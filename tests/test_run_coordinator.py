import asyncio
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from scraper.db import init_db
from scraper.web import app as web_app
from scraper.web.state import AppState, app_state


@pytest.mark.asyncio
async def test_coordinator_allows_only_one_active_run():
    state = AppState()
    blocker = asyncio.Event()

    async def wait_forever():
        await blocker.wait()

    first = asyncio.create_task(wait_forever())
    assert state.run_coordinator.reserve("smart")
    state.run_coordinator.attach(first)

    assert not state.run_coordinator.reserve("revalidate")
    assert state._run_task is first
    assert state.current_run_mode == "smart"

    assert state.run_coordinator.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    await asyncio.sleep(0)
    assert not state.is_running
    assert state._run_task is None


@pytest.mark.asyncio
async def test_old_task_cannot_clear_newer_run_state():
    state = AppState()

    async def noop():
        await asyncio.sleep(0)

    old_task = asyncio.create_task(noop())
    new_task = asyncio.create_task(noop())
    assert state.run_coordinator.reserve("smart", new_task)

    assert not state.run_coordinator.release(old_task)
    assert state.is_running
    assert state._run_task is new_task

    await new_task
    await asyncio.sleep(0)
    assert not state.is_running
    assert state._run_task is None


@pytest.mark.asyncio
async def test_done_callback_clears_task_cancelled_before_coroutine_runs():
    state = AppState()

    async def never_started():
        await asyncio.sleep(10)

    task = asyncio.create_task(never_started())
    assert state.run_coordinator.reserve("revalidate")
    state.run_coordinator.attach(task)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0)
    assert not state.is_running
    assert state._run_task is None


def test_run_route_cannot_overwrite_existing_pipeline_task(tmp_path):
    class FakeTask:
        def __init__(self):
            self.callbacks = []

        def add_done_callback(self, callback):
            self.callbacks.append(callback)

        def done(self):
            return False

        def cancel(self):
            return None

    db = tmp_path / "scraper.db"
    init_db(db)
    task = FakeTask()
    old_db, old_cfg = app_state.db_path, app_state.pipeline_cfg
    try:
        app_state.db_path = db
        app_state.pipeline_cfg = object()
        assert app_state.run_coordinator.reserve("smart", task)
        with patch("scraper.web.app._ADMIN_PASSWORD", "testpass"):
            response = TestClient(web_app.app).post(
                "/admin/api/run",
                data={"run_mode": "full", "skip_scraped": "on", "skip_extracted": "on"},
                headers={
                    "Authorization": "Basic YWRtaW46dGVzdHBhc3M=",
                    "Host": "testserver",
                    "Origin": "http://testserver",
                },
            )

        assert response.status_code == 200
        assert response.json() == {"ok": False, "error": "already running"}
        assert app_state._run_task is task
        assert app_state.current_run_mode == "smart"
    finally:
        app_state.run_coordinator.release(task)
        app_state.db_path = old_db
        app_state.pipeline_cfg = old_cfg

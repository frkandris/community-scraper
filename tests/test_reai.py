import hashlib
from pathlib import Path
from unittest.mock import AsyncMock, patch
from scraper.db import init_db, save_cache_page
from scraper.models import CommunityRecord
from scraper.pipeline import PipelineConfig
from scraper.store import save_results
from scraper.web import app as web_app
from scraper.web.state import app_state
from fastapi.testclient import TestClient


def _db(tmp_path: Path) -> Path:
    p = tmp_path / "scraper.db"
    init_db(p)
    return p


def _cfg(db: Path) -> PipelineConfig:
    return PipelineConfig(
        searxng_url="http://localhost:8888",
        search_results_per_query=5,
        search_max_pages=2,
        search_rate_limit=1.0,
        fetch_timeout=15,
        fetch_min_text_length=100,
        fetch_max_concurrent=3,
        fetch_blocked_domains=[],
        db_path=db,
    )


def _community(db: Path, name="Budapest Futók") -> CommunityRecord:
    r = CommunityRecord(
        name=name, topic="running", city="Budapest", locale="hu",
        source_url="https://example.com/futok",
        extracted_at="2026-01-01T00:00:00+00:00",
    )
    save_results("Budapest", "running", [r], db)
    return r


_AUTH = "Basic YWRtaW46dGVzdHBhc3M="
_HEADERS = {
    "Authorization": _AUTH,
    "Host": "testserver",
    "Origin": "http://testserver",
}


def test_reai_endpoint_not_found_returns_ok_false(tmp_path):
    db = _db(tmp_path)
    old_db, old_cfg = app_state.db_path, app_state.pipeline_cfg
    try:
        app_state.db_path = db
        app_state.pipeline_cfg = _cfg(db)
        with patch("scraper.web.app._ADMIN_PASSWORD", "testpass"):
            resp = TestClient(web_app.app).post(
                "/admin/communities/nonexistentid/reai",
                headers=_HEADERS,
            )
        assert resp.status_code == 200
        assert resp.json()["ok"] is False
        assert resp.json()["error"] == "not_found"
    finally:
        app_state.db_path = old_db
        app_state.pipeline_cfg = old_cfg


def test_reai_endpoint_found_returns_ok_true(tmp_path):
    db = _db(tmp_path)
    r = _community(db)
    old_db, old_cfg = app_state.db_path, app_state.pipeline_cfg
    try:
        app_state.db_path = db
        app_state.pipeline_cfg = _cfg(db)
        with patch("scraper.web.app._ADMIN_PASSWORD", "testpass"), \
             patch("scraper.web.app.reextract_community", new_callable=AsyncMock) as mock_reai:
            resp = TestClient(web_app.app).post(
                f"/admin/communities/{r.community_id}/reai",
                headers=_HEADERS,
            )
        assert resp.status_code == 200
        assert resp.json()["ok"] is True
        assert mock_reai.called
    finally:
        app_state.db_path = old_db
        app_state.pipeline_cfg = old_cfg


def test_reextract_community_uses_cached_text(tmp_path):
    import asyncio
    from scraper.pipeline import reextract_community

    db = _db(tmp_path)
    r = _community(db)
    url_hash = hashlib.sha256(r.source_url.encode()).hexdigest()[:16]
    save_cache_page(db, {"url": r.source_url, "url_hash": url_hash, "raw_text": "Futó klub szöveg"})

    cfg = _cfg(db)
    with patch("scraper.pipeline.FallbackExtractor") as MockFallback, \
         patch("scraper.pipeline.fetch_and_clean", new_callable=AsyncMock) as mock_fetch:
        mock_extractor = AsyncMock()
        mock_extractor.extract = AsyncMock(return_value=[])
        MockFallback.return_value = mock_extractor
        result = asyncio.run(reextract_community(db, cfg, r.community_id))

    assert result is True
    mock_extractor.extract.assert_called_once()
    call_kwargs = mock_extractor.extract.call_args
    assert "Futó klub szöveg" in str(call_kwargs)
    mock_fetch.assert_not_called()

"""Sitemap <lastmod> is a *trustworthy* freshness signal.

The community upsert only advances `updated_at` on a real content change, so a
re-extraction that produces identical data does not churn every page's lastmod
(the 2026-06 corpus-instability lesson). The sitemap emits that date per
community URL.
"""
import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

from scraper.db import init_db
from scraper.models import CommunityRecord
from scraper.pipeline import CityConfig
from scraper.store import save_results
from scraper.web import app as web_app
from scraper.web.state import app_state

KOZ = {"host": "kozossegek.com"}


def _rec(desc, extracted_at="2026-01-01T00:00:00+00:00"):
    return CommunityRecord(
        name="Zenei Kör", topic="music", city="Budapest", locale="hu",
        source_url="https://a.test", extracted_at=extracted_at,
        description=desc,
    )


def _updated_at(db: Path) -> str:
    return sqlite3.connect(db).execute("SELECT updated_at FROM communities").fetchone()[0]


def test_updated_at_stable_on_unchanged_content(tmp_path: Path):
    db = tmp_path / "scraper.db"
    init_db(db)
    save_results("Budapest", "music", [_rec("Aktív zenei közösség Budapesten.")], db)
    first = _updated_at(db)
    # Re-extract: identical content but a FRESH extracted_at — updated_at must NOT
    # advance (extracted_at is excluded from the content fingerprint).
    save_results("Budapest", "music",
                 [_rec("Aktív zenei közösség Budapesten.", extracted_at="2026-07-27T10:00:00+00:00")], db)
    assert _updated_at(db) == first
    # Real change — updated_at advances.
    save_results("Budapest", "music", [_rec("Új leírás, más tartalom.")], db)
    assert _updated_at(db) != first


def test_sitemap_emits_lastmod_for_community(tmp_path: Path):
    db = tmp_path / "scraper.db"
    init_db(db)
    save_results("Budapest", "music", [_rec("Aktív zenei közösség Budapesten.")], db)
    old_db, old_cities = app_state.db_path, app_state.cities
    app_state.db_path = db
    app_state.cities = [CityConfig(name="Budapest", country="Hungary", locale="hu", search_variants=[])]
    try:
        xml = TestClient(web_app.app).get("/sitemap.xml", headers=KOZ).text
        assert "/budapest/zenei-kor</loc><lastmod>" in xml
    finally:
        app_state.db_path, app_state.cities = old_db, old_cities

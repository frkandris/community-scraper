from pathlib import Path

import pytest
import yaml

from scraper.search import (
    LOCALE_TO_DATAFORSEO_LOCATION,
    DataForSEOClient,
    SearchUnavailableError,
    build_queries,
)


def test_build_queries_uses_primary_and_secondary_city_variants():
    assert build_queries("Budapest", ["Budapest", "Budapest Hungary"], ["running", "club"]) == [
        "running Budapest",
        "club Budapest",
        "running Budapest Hungary",
    ]


def test_build_queries_handles_missing_terms_or_variants():
    assert build_queries("Budapest", ["Budapest"], []) == []
    assert build_queries("Budapest", [], ["running"]) == ["running Budapest"]


@pytest.mark.asyncio
async def test_standard_search_posts_configured_high_priority(monkeypatch):
    posted = {}

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def post(self, url, json, headers):
            posted["payload"] = json
            raise RuntimeError("stop after payload capture")

    monkeypatch.setattr("scraper.search.httpx.AsyncClient", lambda **kwargs: FakeClient())
    client = DataForSEOClient(
        "login", "password", mode="standard", standard_priority=2,
        rate_limit_seconds=0,
    )
    with pytest.raises(SearchUnavailableError):
        await client.search("running Stockholm", locale="sv")

    assert posted["payload"][0]["priority"] == 2


def test_every_city_locale_has_a_dataforseo_location():
    """task_post rejects location-less tasks (40501 "Invalid Field:
    'location_name'"), and the pipeline's fail-fast then kills the whole pass —
    the 2026-07-16..23 outage started at Bratislava (locale sk, unmapped)."""
    cities = yaml.safe_load(
        (Path(__file__).parent.parent / "config" / "cities.yaml").read_text(
            encoding="utf-8"))["cities"]
    locales = {str(c["locale"]).split("-")[0] for c in cities}
    unmapped = locales - set(LOCALE_TO_DATAFORSEO_LOCATION)
    assert not unmapped, f"locales without DataForSEO location_code: {sorted(unmapped)}"


@pytest.mark.asyncio
async def test_standard_search_falls_back_to_us_location_for_unknown_locale(monkeypatch):
    posted = {}

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def post(self, url, json, headers):
            posted["payload"] = json
            raise RuntimeError("stop after payload capture")

    monkeypatch.setattr("scraper.search.httpx.AsyncClient", lambda **kwargs: FakeClient())
    client = DataForSEOClient(
        "login", "password", mode="standard", rate_limit_seconds=0,
    )
    with pytest.raises(SearchUnavailableError):
        await client.search("running Atlantis", locale="xx")

    assert posted["payload"][0]["location_code"] == 2840


@pytest.mark.asyncio
async def test_standard_task_post_rejection_fails_fast_without_polling(monkeypatch):
    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {"status_code": 20000, "tasks": [{
                "id": "07230425-1757-0066-0000-dead",
                "status_code": 40501,
                "status_message": "Invalid Field: 'location_name'.",
            }]}

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def post(self, url, json, headers):
            return FakeResponse()

        async def get(self, url, headers):
            raise AssertionError("a rejected task must not be polled")

    monkeypatch.setattr("scraper.search.httpx.AsyncClient", lambda **kwargs: FakeClient())
    client = DataForSEOClient(
        "login", "password", mode="standard", rate_limit_seconds=0,
    )
    with pytest.raises(SearchUnavailableError, match="40501.*location_name"):
        await client.search("choir Bratislava", locale="sk")

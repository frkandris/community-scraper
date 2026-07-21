import pytest

from scraper.search import DataForSEOClient, SearchUnavailableError, build_queries


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

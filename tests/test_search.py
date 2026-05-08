from scraper.search import build_queries


def test_build_queries_uses_primary_and_secondary_city_variants():
    assert build_queries("Budapest", ["Budapest", "Budapest Hungary"], ["running", "club"]) == [
        "running Budapest",
        "club Budapest",
        "running Budapest Hungary",
    ]


def test_build_queries_handles_missing_terms_or_variants():
    assert build_queries("Budapest", ["Budapest"], []) == []
    assert build_queries("Budapest", [], ["running"]) == ["running Budapest"]

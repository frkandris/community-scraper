from scraper import fetch


def test_extract_text_respects_configured_minimum(monkeypatch):
    monkeypatch.setattr(fetch.trafilatura, "extract", lambda *_args, **_kwargs: "short")

    assert fetch._extract_text("", min_text_length=5) == "short"
    assert fetch._extract_text("", min_text_length=6) is None

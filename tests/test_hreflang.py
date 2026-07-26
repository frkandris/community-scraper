"""hreflang alternates on shared static pages, and NOT on content pages."""
from fastapi.testclient import TestClient

from scraper.web import app as web_app
from scraper.web.i18n import _hreflang_alternates


def test_hreflang_pairs_for_static_pages():
    for path in ("/", "/map", "/terkep", "/people", "/emberek"):
        alts = _hreflang_alternates(path)
        langs = {a["lang"] for a in alts}
        assert langs == {"hu", "en", "x-default"}, path
    # home is symmetric
    home = {a["lang"]: a["href"] for a in _hreflang_alternates("/")}
    assert home["hu"] == "https://kozossegek.com/"
    assert home["en"] == "https://meetapedia.com/"
    assert home["x-default"] == "https://meetapedia.com/"
    # map is localized per edition
    m = {a["lang"]: a["href"] for a in _hreflang_alternates("/map")}
    assert m["hu"] == "https://kozossegek.com/terkep"
    assert m["en"] == "https://meetapedia.com/map"


def test_no_hreflang_for_content_or_redirecting_pages():
    # content page and not-yet-localized static aliases get no alternates
    assert _hreflang_alternates("/budapest") == []
    assert _hreflang_alternates("/budapest/zenei-kor") == []
    assert _hreflang_alternates("/about") == []


def test_hreflang_rendered_in_head():
    c = TestClient(web_app.app)
    html = c.get("/", headers={"host": "meetapedia.com"}).text
    assert html.count('rel="alternate" hreflang=') == 3
    assert 'hreflang="x-default"' in html

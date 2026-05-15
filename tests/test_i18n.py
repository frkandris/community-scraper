from unittest.mock import MagicMock

import pytest


def _req(host: str):
    req = MagicMock()
    req.headers.get.side_effect = lambda key, default="": host if key == "host" else default
    return req


def test_detect_site_meetapedia():
    from scraper.web.i18n import _detect_site
    assert _detect_site(_req("meetapedia.com")) == "meetapedia"


def test_detect_site_www_meetapedia():
    from scraper.web.i18n import _detect_site
    assert _detect_site(_req("www.meetapedia.com")) == "meetapedia"


def test_detect_site_meetapedia_with_port():
    from scraper.web.i18n import _detect_site
    assert _detect_site(_req("meetapedia.com:8000")) == "meetapedia"


def test_detect_site_kozossegek():
    from scraper.web.i18n import _detect_site
    assert _detect_site(_req("kozossegek.com")) == "kozossegek"


def test_detect_site_localhost_defaults_to_kozossegek():
    from scraper.web.i18n import _detect_site
    assert _detect_site(_req("localhost:8000")) == "kozossegek"


def test_detect_site_empty_defaults_to_kozossegek():
    from scraper.web.i18n import _detect_site
    assert _detect_site(_req("")) == "kozossegek"


def test_lang_context_meetapedia_sets_en_and_site_info():
    from scraper.web.i18n import lang_context
    ctx = lang_context(_req("meetapedia.com"))
    assert ctx["lang"] == "en"
    assert ctx["site"] == "meetapedia"
    assert ctx["site_name"] == "meetapedia.com"
    assert ctx["site_url"] == "https://meetapedia.com"


def test_lang_context_kozossegek_sets_hu_and_site_info():
    from scraper.web.i18n import lang_context
    ctx = lang_context(_req("kozossegek.com"))
    assert ctx["lang"] == "hu"
    assert ctx["site"] == "kozossegek"
    assert ctx["site_name"] == "közösségek.com"
    assert ctx["site_url"] == "https://közösségek.com"


def test_lang_context_includes_locale():
    from scraper.web.i18n import lang_context
    hu_ctx = lang_context(_req("kozossegek.com"))
    en_ctx = lang_context(_req("meetapedia.com"))
    assert hu_ctx["locale"] == "hu_HU"
    assert en_ctx["locale"] == "en_US"


def test_make_t_substitutes_defaults():
    from scraper.web.i18n import make_t, _T
    _T["en"]["_fmt_test"] = "site is {site_name}"
    try:
        t = make_t("en", site_name="testsite.com")
        assert t("_fmt_test") == "site is testsite.com"
    finally:
        del _T["en"]["_fmt_test"]


def test_make_t_kwargs_override_defaults():
    from scraper.web.i18n import make_t, _T
    _T["en"]["_fmt_test2"] = "site is {site_name}"
    try:
        t = make_t("en", site_name="default.com")
        assert t("_fmt_test2", site_name="override.com") == "site is override.com"
    finally:
        del _T["en"]["_fmt_test2"]

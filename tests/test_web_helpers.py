from scraper.web.app import _BasicAuth, _safe_redirect_target
from scraper.web.schema import records_to_jsonld


def test_safe_redirect_target_allows_only_local_paths():
    assert _safe_redirect_target("/admin/cache", "/") == "/admin/cache"
    assert _safe_redirect_target("//evil.test", "/") == "/"
    assert _safe_redirect_target("https://evil.test", "/") == "/"


def test_same_origin_admin_write_rejects_cross_origin_posts():
    scope = {"method": "POST"}

    assert _BasicAuth._same_origin_admin_write(
        scope,
        {b"host": b"example.com", b"origin": b"https://example.com"},
    )
    assert not _BasicAuth._same_origin_admin_write(
        scope,
        {b"host": b"example.com", b"origin": b"https://evil.test"},
    )
    assert not _BasicAuth._same_origin_admin_write(scope, {b"host": b"example.com"})


def test_jsonld_escapes_script_end_tags():
    raw = records_to_jsonld([
        {
            "name": "</script><script>alert(1)</script>",
            "topic": "running",
            "city": "Budapest",
            "locale": "hu",
            "source_url": "https://example.com",
            "extracted_at": "2026-01-01T00:00:00+00:00",
        }
    ])

    assert "</script>" not in raw
    assert "<\\/script>" in raw

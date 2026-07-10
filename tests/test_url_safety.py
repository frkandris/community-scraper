import socket
from unittest.mock import patch

import httpx
import pytest
import respx

from scraper.fetch import fetch_and_clean
from scraper.url_safety import (
    UnsafeURLError,
    assert_safe_public_url,
    is_public_http_url,
)


def _dns_answer(address: str, port: int = 443):
    family = socket.AF_INET6 if ":" in address else socket.AF_INET
    return [(family, socket.SOCK_STREAM, 6, "", (address, port))]


def test_public_url_syntax_rejects_local_and_credential_urls():
    assert is_public_http_url("https://example.com/community")
    assert not is_public_http_url("file:///etc/passwd")
    assert not is_public_http_url("http://localhost/admin")
    assert not is_public_http_url("http://service.internal/admin")
    assert not is_public_http_url("http://user:password@example.com/")
    assert not is_public_http_url("http://127.0.0.1/admin")
    assert not is_public_http_url("http://169.254.169.254/latest/meta-data")


@pytest.mark.asyncio
async def test_dns_resolution_rejects_private_answers():
    with patch(
        "scraper.url_safety.socket.getaddrinfo",
        return_value=_dns_answer("10.0.0.8"),
    ):
        with pytest.raises(UnsafeURLError, match="non-public"):
            await assert_safe_public_url("https://example.com/community")


@pytest.mark.asyncio
async def test_dns_resolution_accepts_global_answers():
    with patch(
        "scraper.url_safety.socket.getaddrinfo",
        return_value=_dns_answer("93.184.216.34"),
    ):
        await assert_safe_public_url("https://example.com/community")


@pytest.mark.asyncio
@respx.mock
async def test_fetch_blocks_redirect_to_private_ip():
    route = respx.get("https://example.com/start").mock(
        return_value=httpx.Response(
            302,
            headers={"Location": "http://127.0.0.1/admin"},
        )
    )
    with patch(
        "scraper.url_safety.socket.getaddrinfo",
        return_value=_dns_answer("93.184.216.34"),
    ):
        result = await fetch_and_clean("https://example.com/start", blocked_domains=[])

    assert route.called
    assert result is None


@pytest.mark.asyncio
@respx.mock
async def test_fetch_allows_valid_public_html():
    route = respx.get("https://example.com/page").mock(
        return_value=httpx.Response(
            200,
            headers={"Content-Type": "text/html; charset=utf-8"},
            text="<p>Public community page</p>",
        )
    )
    with patch(
        "scraper.url_safety.socket.getaddrinfo",
        return_value=_dns_answer("93.184.216.34"),
    ), patch("scraper.fetch._extract_text", return_value="Public community page"):
        result = await fetch_and_clean("https://example.com/page", blocked_domains=[])

    assert route.called
    assert result == "Public community page"

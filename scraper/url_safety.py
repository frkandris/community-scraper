"""SSRF-resistant validation for every server-side web fetch."""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from urllib.parse import SplitResult, urlsplit


class UnsafeURLError(ValueError):
    pass


_BLOCKED_HOST_SUFFIXES = (".localhost", ".local", ".internal")


def parse_public_http_url(url: str) -> SplitResult:
    """Validate URL syntax without performing network I/O."""
    if not isinstance(url, str) or not url or len(url) > 2048:
        raise UnsafeURLError("URL is empty or too long")
    if any(char.isspace() or ord(char) < 32 for char in url):
        raise UnsafeURLError("URL contains whitespace or control characters")

    try:
        parsed = urlsplit(url)
        host = (parsed.hostname or "").rstrip(".").lower()
        port = parsed.port
    except ValueError as exc:
        raise UnsafeURLError("URL has an invalid host or port") from exc

    if parsed.scheme not in {"http", "https"}:
        raise UnsafeURLError("Only HTTP(S) URLs are allowed")
    if not host:
        raise UnsafeURLError("URL has no host")
    if parsed.username is not None or parsed.password is not None:
        raise UnsafeURLError("Credential-bearing URLs are not allowed")
    if port is not None and not 1 <= port <= 65535:
        raise UnsafeURLError("URL port is out of range")
    if host == "localhost" or host.endswith(_BLOCKED_HOST_SUFFIXES):
        raise UnsafeURLError("Local hostnames are not allowed")

    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        if "." not in host:
            raise UnsafeURLError("Single-label hostnames are not allowed")
    else:
        if not address.is_global:
            raise UnsafeURLError("Non-public IP addresses are not allowed")

    return parsed


def is_public_http_url(url: str) -> bool:
    try:
        parse_public_http_url(url)
        return True
    except UnsafeURLError:
        return False


async def assert_safe_public_url(url: str) -> None:
    """Require every DNS answer for the URL to be globally routable."""
    parsed = parse_public_http_url(url)
    host = parsed.hostname or ""
    try:
        literal = ipaddress.ip_address(host)
    except ValueError:
        literal = None
    if literal is not None:
        return  # syntax validation already required literal.is_global

    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    try:
        answers = await asyncio.to_thread(
            socket.getaddrinfo,
            host,
            port,
            type=socket.SOCK_STREAM,
        )
    except OSError as exc:
        raise UnsafeURLError("Host could not be resolved") from exc

    addresses = {
        ipaddress.ip_address(answer[4][0])
        for answer in answers
        if answer[0] in {socket.AF_INET, socket.AF_INET6}
    }
    if not addresses:
        raise UnsafeURLError("Host did not resolve to an IP address")
    if any(not address.is_global for address in addresses):
        raise UnsafeURLError("Host resolves to a non-public IP address")

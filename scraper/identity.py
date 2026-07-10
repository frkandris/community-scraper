"""Stable, Unicode-safe identity keys and public slugs."""

from __future__ import annotations

import hashlib
import re
import unicodedata


def _canonical(value: str) -> str:
    """Normalize identity text without discarding non-Latin scripts."""
    normalized = unicodedata.normalize("NFKC", str(value)).casefold().strip()
    return re.sub(r"\s+", " ", normalized)


def normalized_match_key(value: str) -> str:
    """Unicode-aware loose key for case/punctuation-insensitive comparisons."""
    return "".join(char for char in _canonical(value) if char.isalnum())


def _record_key(prefix: str, *parts: str) -> str:
    canonical = "\x1f".join(_canonical(part) for part in parts)
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:24]
    return f"{prefix}2:{digest}"


def community_record_key(name: str, city: str, topic: str) -> str:
    return _record_key("c", name, city, topic)


def venue_record_key(name: str, city: str) -> str:
    return _record_key("v", name, city)


def person_record_key(name: str, city: str, role: str, community_name: str) -> str:
    return _record_key("p", name, city, role, community_name)


def public_slug(text: str) -> str:
    """Return an ASCII URL slug, with a stable fallback for non-Latin text."""
    canonical = _canonical(text)
    ascii_text = (
        unicodedata.normalize("NFKD", canonical)
        .encode("ascii", "ignore")
        .decode("ascii")
    )
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_text.lower()).strip("-")

    has_untransliterated_alnum = any(
        char.isalnum()
        and ord(char) > 127
        and not unicodedata.normalize("NFKD", char).encode("ascii", "ignore")
        for char in canonical
    )
    if not has_untransliterated_alnum:
        return slug

    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:10]
    return f"{slug}-{digest}" if slug else f"u-{digest}"

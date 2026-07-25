"""The fingerprint_model pin: a provider-side model rename must not invalidate
the extraction cache (deepseek-chat → deepseek-v4-flash, 2026-07)."""
from scraper.extract import (
    DeepSeekExtractor,
    get_extract_fingerprint,
    get_person_fingerprint,
    get_venue_fingerprint,
)


def _extractor(**kw) -> DeepSeekExtractor:
    return DeepSeekExtractor(api_key="k", model="deepseek-v4-flash", **kw)


def test_pinned_fingerprints_match_old_model_name():
    pinned = _extractor(fingerprint_model="deepseek-chat")
    assert pinned.model == "deepseek-v4-flash"  # wire model is the new name
    assert pinned.model_fingerprint == get_extract_fingerprint("deepseek-chat")
    assert pinned.venue_fingerprint == get_venue_fingerprint("deepseek-chat")
    assert pinned.person_fingerprint == get_person_fingerprint("deepseek-chat")


def test_unpinned_fingerprints_follow_wire_model():
    plain = _extractor()
    assert plain.fingerprint_model == "deepseek-v4-flash"
    assert plain.model_fingerprint == get_extract_fingerprint("deepseek-v4-flash")
    assert plain.model_fingerprint != get_extract_fingerprint("deepseek-chat")

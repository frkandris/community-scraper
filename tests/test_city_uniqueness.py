"""Every configured city must have a unique name AND a unique public_slug.

Slugs accent-fold (Münster→munster), so two cities that differ only by umlaut
(Münster/Munster, Löhne/Lohne) would collapse to one slug and `_city_from_slug`
would resolve every link to whichever comes first — silently hiding one city's
pages. The German import (2026-07-26) disambiguates such collisions with a state
suffix; this test locks that invariant for future imports.
"""
from collections import Counter

import yaml

from scraper.config import CONFIG_DIR
from scraper.identity import public_slug


def _cities():
    return yaml.safe_load((CONFIG_DIR / "cities.yaml").read_text(encoding="utf-8"))["cities"]


def test_city_names_are_unique():
    names = [c["name"] for c in _cities()]
    dups = [n for n, k in Counter(names).items() if k > 1]
    assert not dups, f"duplicate city names: {dups}"


def test_city_slugs_are_unique():
    slugs = Counter(public_slug(c["name"]) for c in _cities())
    dups = [s for s, k in slugs.items() if k > 1]
    assert not dups, f"colliding public_slugs (accent-fold): {dups}"

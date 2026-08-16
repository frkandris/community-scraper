#!/usr/bin/env python3
"""Import settlements for one country from Wikidata into config/cities.yaml.

Additive by design: existing entries are never rewritten, only genuinely new
settlements are appended to the country's section. That keeps hand-tuned
`search_variants` and `topic_tier` values — and the crawl coverage they imply —
intact across re-runs. See docs/wiki/pages/operations/importing-city-lists.md.

Usage
-----
    python scripts/import_cities.py hungary  --min-pop 1000 --apply
    python scripts/import_cities.py indonesia --min-pop 100000 --apply

Without --apply it prints a dry-run summary and writes nothing.

Coordinates for newly added cities are printed as a ready-to-paste block for
`CITY_COORDS` in scraper/web/app.py (the map page needs them); pass
--write-coords to splice them in automatically.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CITIES_YAML = ROOT / "config" / "cities.yaml"
APP_PY = ROOT / "scraper" / "web" / "app.py"

SPARQL_URL = "https://query.wikidata.org/sparql"
USER_AGENT = "meetapedia-city-import/1.0 (https://meetapedia.com; office@strt.hu)"


@dataclass(frozen=True)
class CountrySpec:
    """Everything country-specific the importer needs."""
    key: str                 # CLI name
    yaml_country: str        # value written as `country:`
    locale: str              # value written as `locale:`
    section_comment: str     # the `  # X` marker that opens the section
    wikidata_query: str      # must SELECT ?item ?name ?pop, optionally ?coord
    country_suffix: str      # appended as a disambiguating search variant
    # Below this population a settlement is imported with `topic_tier: core`
    # (only the core_topics are searched — niche topics cannot pay off in a
    # village). Existing entries keep whatever tier they already have.
    core_tier_below: int
    # Trimmed off the ?region label before it is used as a "(…)" name suffix.
    region_noise: tuple[str, ...] = ()
    # SPARQL with one `%s` for a VALUES list, returning ?name ?region. Run only
    # for slug collisions.
    region_query: str = ""


# `wdt:P939` is the KSH settlement code: every Hungarian settlement has one and
# nothing else does, which makes it a far tighter filter than P31/P279* chains.
_HU_QUERY = """
SELECT ?item ?name ?pop ?coord WHERE {
  ?item wdt:P939 ?ksh ; wdt:P1082 ?pop .
  ?item rdfs:label ?name . FILTER(LANG(?name) = "hu")
  OPTIONAL { ?item wdt:P625 ?coord }
}
"""

# Region lookup is a *separate*, tiny query run only for the handful of names
# whose slug collides. Joining it into the main query blew the public endpoint's
# timeout (the P131+ path over 3k settlements returns a truncated body).
_HU_REGION_QUERY = """
SELECT ?name ?region WHERE {
  VALUES ?label { %s }
  ?item wdt:P939 ?ksh ; rdfs:label ?name .
  FILTER(LANG(?name) = "hu") FILTER(STR(?name) = STR(?label))
  ?item wdt:P131/wdt:P131 ?r . ?r wdt:P31 wd:Q188604 .   # magyarországi vármegye
  ?r rdfs:label ?region . FILTER(LANG(?region) = "hu")
}
"""

_ID_REGION_QUERY = """
SELECT ?name ?region WHERE {
  VALUES ?label { %s }
  ?item wdt:P17 wd:Q252 ; rdfs:label ?name .
  FILTER(LANG(?name) = "id") FILTER(STR(?name) = STR(?label))
  ?item wdt:P131 ?r . ?r rdfs:label ?region . FILTER(LANG(?region) = "id")
}
"""

# Indonesia's city tier is the `kota` (Q3199141) — 93 of them, essentially all
# above 100k people. Asking instead for "human settlement in Indonesia" via
# P31/P279* times the public endpoint out (504), so this pins the exact class.
# English labels, matching the rest of the international list (Munich, not
# München); the "Kota " prefix is trimmed below.
_ID_QUERY = """
SELECT ?item ?name ?pop ?coord WHERE {
  ?item wdt:P31 wd:Q3199141 ; wdt:P17 wd:Q252 ; wdt:P1082 ?pop .
  ?item rdfs:label ?name . FILTER(LANG(?name) = "en")
  OPTIONAL { ?item wdt:P625 ?coord }
}
"""

COUNTRIES: dict[str, CountrySpec] = {
    "hungary": CountrySpec(
        key="hungary", yaml_country="Hungary", locale="hu",
        section_comment="# Hungary", wikidata_query=_HU_QUERY,
        country_suffix="Magyarország", core_tier_below=10_000,
        region_noise=("vármegye", "megye"), region_query=_HU_REGION_QUERY,
    ),
    "indonesia": CountrySpec(
        key="indonesia", yaml_country="Indonesia", locale="id",
        section_comment="# SE Asia", wikidata_query=_ID_QUERY,
        country_suffix="Indonesia", core_tier_below=250_000,
        region_noise=("Provinsi", "Kabupaten"), region_query=_ID_REGION_QUERY,
    ),
}

# Wikidata carries Budapest's 23 districts with their own KSH codes. They are
# not settlements for our purposes — Budapest itself is already a city.
_EXCLUDE_NAME = re.compile(r"(kerülete?$|^Budapest [IVXL]+)")

# Administrative prefixes that are part of the Wikidata label but not of the
# city's name as anyone writes it ("Kota Bandung" → "Bandung").
_TRIM_PREFIX = re.compile(r"^(Kota|Kabupaten|City of|Municipality of)\s+")


def _sparql(query: str, attempts: int = 4) -> list[dict]:
    """POST a query to WDQS, retrying the endpoint's throttling responses.

    The public endpoint answers 429 (and sometimes 500) under concurrent load
    and honours Retry-After. Without a retry the importer dies halfway and
    leaves cities.yaml half-written.
    """
    body = urllib.parse.urlencode({"query": query}).encode()
    last: Exception | None = None
    for attempt in range(attempts):
        req = urllib.request.Request(
            SPARQL_URL, data=body,
            headers={"Accept": "application/sparql-results+json",
                     "User-Agent": USER_AGENT},
        )
        try:
            with urllib.request.urlopen(req, timeout=180) as resp:
                return json.load(resp)["results"]["bindings"]
        except urllib.error.HTTPError as exc:
            last = exc
            if exc.code not in (429, 500, 502, 503, 504):
                raise
            wait = int(exc.headers.get("Retry-After") or 0) or 15 * (attempt + 1)
            print(f"  … WDQS {exc.code}, retrying in {wait}s "
                  f"({attempt + 1}/{attempts})", file=sys.stderr)
            time.sleep(wait)
        except json.JSONDecodeError as exc:
            # A truncated body means the query exceeded the endpoint's internal
            # timeout — retrying the same query will not help.
            raise SystemExit(
                "WDQS returned a truncated response — the query is too "
                f"expensive for the public endpoint: {exc}"
            ) from exc
    raise SystemExit(f"WDQS unavailable after {attempts} attempts: {last}")


def _deaccent(s: str) -> str:
    """ASCII fold — the second search variant, for engines that ignore accents."""
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )


def _parse_point(wkt: str) -> tuple[float, float] | None:
    m = re.match(r"Point\(([-\d.]+) ([-\d.]+)\)", wkt or "")
    return (round(float(m.group(2)), 4), round(float(m.group(1)), 4)) if m else None


def fetch(spec: CountrySpec, min_pop: int) -> dict[str, dict]:
    """Return {name: {pop, lat, lng, region}}, deduplicated and size-filtered."""
    out: dict[str, dict] = {}
    for row in _sparql(spec.wikidata_query):
        name = _TRIM_PREFIX.sub("", row["name"]["value"].strip()).strip()
        if not name or _EXCLUDE_NAME.search(name):
            continue
        try:
            pop = int(float(row["pop"]["value"]))
        except (KeyError, ValueError):
            continue
        if pop < min_pop:
            continue
        coord = _parse_point(row.get("coord", {}).get("value", ""))
        prev = out.get(name)
        # An item can carry several preferred P1082 statements (census years).
        # Keep the largest — at a 1000-person threshold the choice is immaterial
        # and the max is deterministic.
        if prev is None or pop > prev["pop"]:
            out[name] = {"pop": pop, "lat": coord[0] if coord else None,
                         "lng": coord[1] if coord else None}
        elif prev.get("lat") is None and coord:
            prev["lat"], prev["lng"] = coord
    return out


def fetch_regions(spec: CountrySpec, names: list[str]) -> dict[str, str]:
    """{name: region} for the given settlements — one small VALUES query."""
    if not names or not spec.region_query:
        return {}
    values = " ".join('"%s"' % n.replace('"', '') for n in names)
    out: dict[str, str] = {}
    for row in _sparql(spec.region_query % values):
        region = row["region"]["value"].strip()
        for noise in spec.region_noise:
            region = re.sub(rf"^\s*{re.escape(noise)}\s+|\s*{re.escape(noise)}\s*$",
                            "", region)
        # Several administrative levels can match; the first is good enough for
        # a disambiguation suffix, and taking the first keeps it deterministic
        # given the endpoint's stable result ordering.
        out.setdefault(row["name"]["value"], region.strip())
    return out


def existing_names(text: str) -> set[str]:
    return {m.group(1).strip().strip('"\'')
            for m in re.finditer(r"^  - name: (.+)$", text, re.M)}


def disambiguate(spec: CountrySpec, new: dict[str, dict],
                 taken_slugs: set[str]) -> dict[str, dict]:
    """Rename new settlements whose public_slug would collide.

    public_slug() accent-folds, so Komló and Kömlő both become "komlo" and
    _city_from_slug would resolve every link to whichever comes first — one
    city's pages silently disappear. Same convention as the 2026-07 German
    import: keep the real name in search_variants, disambiguate the display
    name with a "(Region)" suffix. Locked by tests/test_city_uniqueness.py.
    """
    from scraper.identity import public_slug

    # Find the collisions first, then resolve regions for just those names.
    colliding, seen_slugs = [], set(taken_slugs)
    for name in sorted(new):
        slug = public_slug(name)
        if slug in seen_slugs:
            colliding.append(name)
        seen_slugs.add(slug)
    regions = fetch_regions(spec, colliding) if colliding else {}

    seen: dict[str, str] = {}
    renamed: dict[str, dict] = {}
    for name in sorted(new):
        info = dict(new[name])
        info.setdefault("search_name", name)
        slug = public_slug(name)
        if slug in taken_slugs or slug in seen:
            region = regions.get(name)
            if not region:
                print(f"  ! dropping {name!r}: slug {slug!r} collides and no "
                      f"region is known to disambiguate it")
                continue
            candidate = f"{name} ({region})"
            if public_slug(candidate) in taken_slugs or public_slug(candidate) in seen:
                print(f"  ! dropping {name!r}: {candidate!r} still collides")
                continue
            print(f"  ~ {name!r} → {candidate!r} (slug {slug!r} already taken)")
            name = candidate
            slug = public_slug(name)
        seen[slug] = name
        renamed[name] = info
    return renamed


def render(spec: CountrySpec, name: str, info: dict) -> str:
    # Searches must use the real settlement name, not the disambiguated one:
    # nobody writes "Kömlő (Heves)" on a club's web page.
    search_name = info.get("search_name", name)
    variants = [search_name]
    plain = _deaccent(search_name)
    if plain != search_name:
        variants.append(plain)
    variants.append(f"{search_name} {spec.country_suffix}")
    rendered = ", ".join(f'"{v}"' for v in variants)
    quoted = f'"{name}"' if not name.replace(" ", "").isalnum() else name
    block = (f"  - name: {quoted}\n"
             f"    country: {spec.yaml_country}\n"
             f"    locale: {spec.locale}\n"
             f"    search_variants: [{rendered}]\n")
    if info["pop"] < spec.core_tier_below:
        block += "    topic_tier: core\n"
    return block


def section_end(text: str, spec: CountrySpec) -> int:
    """Character offset just past the last entry of the country's section."""
    start = text.find(f"  {spec.section_comment}\n")
    if start < 0:
        raise SystemExit(f"section marker {spec.section_comment!r} not found")
    # The section runs until the next `  # ...` comment at the same indent.
    nxt = re.search(r"^  # ", text[start + 4:], re.M)
    return start + 4 + nxt.start() if nxt else len(text)


def splice_coords(new: dict[str, dict]) -> str:
    """Return the CITY_COORDS lines for cities that have coordinates."""
    lines = [f'    "{n}": ({i["lat"]}, {i["lng"]}),'
             for n, i in sorted(new.items()) if i["lat"] is not None]
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("country", choices=sorted(COUNTRIES))
    ap.add_argument("--min-pop", type=int, required=True)
    ap.add_argument("--apply", action="store_true", help="write config/cities.yaml")
    ap.add_argument("--write-coords", action="store_true",
                    help="also splice coordinates into CITY_COORDS in app.py")
    args = ap.parse_args()

    from scraper.identity import public_slug

    spec = COUNTRIES[args.country]
    found = fetch(spec, args.min_pop)
    text = CITIES_YAML.read_text(encoding="utf-8")
    have = existing_names(text)
    new = {n: i for n, i in found.items() if n not in have}
    new = disambiguate(spec, new, {public_slug(n) for n in have})

    print(f"{spec.yaml_country}: {len(found)} settlements ≥ {args.min_pop} people, "
          f"{len(have & set(found))} already listed, {len(new)} new")
    tiered = sum(1 for i in new.values() if i["pop"] < spec.core_tier_below)
    print(f"  {tiered} of the new ones get topic_tier: core (< {spec.core_tier_below})")
    missing = [n for n, i in new.items() if i["lat"] is None]
    if missing:
        print(f"  {len(missing)} without coordinates (no map pin): {missing[:10]}")

    if not new:
        return 0
    if not args.apply:
        print("\n--- sample ---")
        for n in sorted(new)[:3]:
            print(render(spec, n, new[n]), end="")
        print("\n(dry run — pass --apply to write)")
        return 0

    at = section_end(text, spec)
    # Alphabetical inside the appended block; the file as a whole stays grouped
    # by country, largest-first for the pre-existing entries.
    addition = "".join(render(spec, n, new[n]) for n in sorted(new))
    CITIES_YAML.write_text(text[:at] + addition + text[at:], encoding="utf-8")
    print(f"wrote {len(new)} entries to {CITIES_YAML}")

    coords = splice_coords(new)
    if args.write_coords and coords:
        app = APP_PY.read_text(encoding="utf-8")
        anchor = "CITY_COORDS: dict[str, tuple[float, float]] = {"
        if anchor not in app:
            print("CITY_COORDS anchor not found — paste manually:", file=sys.stderr)
            print(coords)
            return 1
        header = (f"\n    # {spec.yaml_country} — imported from Wikidata by "
                  f"scripts/import_cities.py (min pop {args.min_pop})")
        app = app.replace(anchor, anchor + header + "\n" + coords, 1)
        APP_PY.write_text(app, encoding="utf-8")
        print(f"spliced {coords.count(chr(10)) + 1} coordinates into CITY_COORDS")
    elif coords:
        print("\n--- CITY_COORDS additions (paste into app.py) ---")
        print(coords)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

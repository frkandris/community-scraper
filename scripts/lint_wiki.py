#!/usr/bin/env python3
"""Validate the LLM wiki's OKF structure and internal link graph."""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
WIKI_ROOT = REPO_ROOT / "docs" / "wiki"
REQUIRED_META = ("type", "title", "description", "tags", "timestamp")
SLUG_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
WIKILINK_RE = re.compile(r"\[\[([^]|#]+)(?:#[^]|]+)?(?:\|[^]]+)?\]\]")
INDEX_ENTRY_RE = re.compile(r"^- \[\[([^]|]+)(?:\|[^]]+)?\]\] — (.+)$", re.MULTILINE)


@dataclass(frozen=True)
class WikiPage:
    path: Path
    slug: str
    text: str
    metadata: dict


def _relative(path: Path) -> str:
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _load_page(path: Path, errors: list[str]) -> WikiPage | None:
    text = path.read_text(encoding="utf-8")
    match = re.match(r"^---\n(.*?)\n---\n", text, re.DOTALL)
    if not match:
        errors.append(f"{_relative(path)}: missing opening YAML frontmatter")
        return None
    try:
        metadata = yaml.safe_load(match.group(1))
    except yaml.YAMLError as exc:
        errors.append(f"{_relative(path)}: invalid YAML frontmatter: {exc}")
        return None
    if not isinstance(metadata, dict):
        errors.append(f"{_relative(path)}: frontmatter must be a mapping")
        return None

    slug = path.stem
    if not SLUG_RE.fullmatch(slug):
        errors.append(f"{_relative(path)}: filename is not kebab-case")
    for key in REQUIRED_META:
        if not metadata.get(key):
            errors.append(f"{_relative(path)}: missing frontmatter field {key!r}")
    if metadata.get("timestamp") and not isinstance(metadata["timestamp"], (str, date)):
        errors.append(f"{_relative(path)}: timestamp must be an ISO date")
    tags = metadata.get("tags")
    if tags and not isinstance(tags, list):
        errors.append(f"{_relative(path)}: tags must be a YAML list")

    body = text[match.end():]
    h1 = re.search(r"^# (.+)$", body, re.MULTILINE)
    if not h1:
        errors.append(f"{_relative(path)}: missing H1")
    elif metadata.get("title") and h1.group(1).strip() != str(metadata["title"]).strip():
        errors.append(
            f"{_relative(path)}: H1 {h1.group(1)!r} != title {metadata['title']!r}"
        )
    elif not body[h1.end():].lstrip().startswith("*"):
        errors.append(f"{_relative(path)}: H1 must be followed by an italic summary")
    if len(text.splitlines()) > 300:
        errors.append(f"{_relative(path)}: page exceeds 300 lines")

    resource = metadata.get("resource")
    if isinstance(resource, str) and resource and "://" not in resource:
        if not (REPO_ROOT / resource).exists():
            errors.append(f"{_relative(path)}: resource does not exist: {resource}")
    return WikiPage(path=path, slug=slug, text=text, metadata=metadata)


def lint_wiki(wiki_root: Path = WIKI_ROOT) -> list[str]:
    errors: list[str] = []
    page_files = sorted((wiki_root / "pages").rglob("*.md"))
    pages = [page for path in page_files if (page := _load_page(path, errors))]
    by_slug: dict[str, WikiPage] = {}
    for page in pages:
        if page.slug in by_slug:
            errors.append(
                f"duplicate page slug {page.slug!r}: "
                f"{_relative(by_slug[page.slug].path)}, {_relative(page.path)}"
            )
        by_slug[page.slug] = page

    index_path = wiki_root / "index.md"
    index_text = index_path.read_text(encoding="utf-8")
    if not re.match(r'^---\nokf_version: ["\']0\.1["\']\n---\n', index_text):
        errors.append(f"{_relative(index_path)}: missing okf_version 0.1 frontmatter")
    index_entries: dict[str, str] = {}
    for slug, description in INDEX_ENTRY_RE.findall(index_text):
        if slug in index_entries:
            errors.append(f"{_relative(index_path)}: duplicate entry {slug!r}")
        index_entries[slug] = description.strip()

    missing_index = sorted(set(by_slug) - set(index_entries))
    extra_index = sorted(set(index_entries) - set(by_slug))
    if missing_index:
        errors.append(f"{_relative(index_path)}: pages missing from index: {missing_index}")
    if extra_index:
        errors.append(f"{_relative(index_path)}: entries without pages: {extra_index}")
    for slug in sorted(set(by_slug) & set(index_entries)):
        description = str(by_slug[slug].metadata.get("description", "")).strip()
        if index_entries[slug] != description:
            errors.append(
                f"{_relative(index_path)}: description for {slug!r} does not mirror frontmatter"
            )

    inbound = {slug: 0 for slug in by_slug}
    for page in pages:
        for target in WIKILINK_RE.findall(page.text):
            if target not in by_slug:
                errors.append(f"{_relative(page.path)}: broken wikilink [[{target}]]")
            elif target != page.slug:
                inbound[target] += 1
    orphans = sorted(slug for slug, count in inbound.items() if count == 0)
    if orphans:
        errors.append(f"wiki graph has orphan pages (index/log excluded): {orphans}")

    log_path = wiki_root / "log.md"
    log_dates = re.findall(r"^## (\d{4}-\d{2}-\d{2})$", log_path.read_text(), re.MULTILINE)
    if log_dates != sorted(log_dates, reverse=True):
        errors.append(f"{_relative(log_path)}: date sections must be newest first")

    return errors


def sync_index_descriptions(wiki_root: Path = WIKI_ROOT) -> None:
    """Mechanically mirror page descriptions into existing index entries."""
    errors: list[str] = []
    pages = [
        page
        for path in sorted((wiki_root / "pages").rglob("*.md"))
        if (page := _load_page(path, errors))
    ]
    if errors:
        raise ValueError("cannot sync an invalid wiki:\n" + "\n".join(errors))
    descriptions = {
        page.slug: str(page.metadata["description"]).strip()
        for page in pages
    }
    index_path = wiki_root / "index.md"
    index_text = index_path.read_text(encoding="utf-8")

    def replace(match: re.Match) -> str:
        slug = match.group(1)
        if slug not in descriptions:
            return match.group(0)
        return f"- [[{slug}]] — {descriptions[slug]}"

    updated = INDEX_ENTRY_RE.sub(replace, index_text)
    index_path.write_text(updated, encoding="utf-8")


def main() -> int:
    if "--fix-index" in sys.argv[1:]:
        sync_index_descriptions()
    errors = lint_wiki()
    if errors:
        print(f"Wiki lint failed with {len(errors)} issue(s):")
        for error in errors:
            print(f"- {error}")
        return 1
    page_count = len(list((WIKI_ROOT / "pages").rglob("*.md")))
    print(f"Wiki lint passed: {page_count} pages, index and link graph are consistent.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

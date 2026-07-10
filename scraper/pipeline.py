import asyncio
import hashlib
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

import structlog

from .extract import (DeepSeekExtractor, ExtractorUnavailableError,
                      FallbackExtractor, get_extract_fingerprint,
                      get_person_fingerprint, get_venue_fingerprint)
from .false_positives import build_prompt_section
from .false_positives import load as load_false_positives
from .fetch import fetch_and_clean
from .search import (DataForSEOClient, FallbackSearchClient, SearchQuotaError,
                     SearchUnavailableError, build_queries)
from .db import get_search_cache, save_search_cache, get_collected_pairs, get_covered_pairs, upsert_venues, upsert_persons, delete_leader_persons_for_community, load_cache_page, find_community_by_id, get_fully_processed_pairs
from .store import save_results

if TYPE_CHECKING:
    from .cache import CacheManager
    from .models import CommunityRecord

log = structlog.get_logger()


def _needs_enrichment(record: "CommunityRecord") -> bool:
    return not record.website and not record.social_links and not record.contact


_TITLE_PREFIXES = frozenset({"dr", "dr.", "prof", "prof.", "ifj", "ifj.", "id", "id."})


def _is_name_segment(s: str) -> bool:
    """Return True if s looks like a name (not a role/description)."""
    s = s.strip()
    if not s:
        return False
    first = s.split()[0].rstrip(".").lower()
    if first in _TITLE_PREFIXES:
        return True
    return bool(re.match(r"^[A-ZÁÉÍÓÖŐÚÜŰ]", s))


def _parse_leader_field(leader: str) -> list[tuple[str, str]]:
    """
    Parse a free-text leader field into (name, role_description) pairs.

    Handles patterns like:
    - "Name, role"                    → one person
    - "Name1, Name2, Name3 (role)"   → multiple people, collective role
    - "Name1, role1; Name2, role2"   → two people with individual roles
    - "Name1, Name2"                  → multiple people, no role
    """
    leader = leader.strip()

    # Extract collective role from trailing parentheses when it has no uppercase
    # e.g. "Name1, Name2 (játékmesterek)" — but NOT "Name (Yang Yajian alias)"
    collective_role = ""
    m = re.search(r"\(([^)]+)\)\s*$", leader)
    if m and not re.search(r"[A-ZÁÉÍÓÖŐÚÜŰ]", m.group(1)):
        collective_role = m.group(1).strip()
        leader = leader[: m.start()].strip().rstrip(",").strip()

    results: list[tuple[str, str]] = []
    for seg in re.split(r"\s*;\s*", leader):
        seg = seg.strip()
        if not seg:
            continue

        comma_parts = [p.strip() for p in seg.split(",")]
        name_parts: list[str] = []
        role_parts: list[str] = []

        for part in comma_parts:
            if not part:
                continue
            if role_parts or not _is_name_segment(part):
                role_parts.append(part)
            else:
                name_parts.append(part)

        role_desc = ", ".join(role_parts).strip() or collective_role
        if not name_parts:
            continue
        if len(name_parts) == 1:
            results.append((name_parts[0], role_desc))
        else:
            # Multiple names in one segment — collective role applies to each
            for name in name_parts:
                results.append((name, collective_role))

    return results


def _persons_from_leaders(records: "list[CommunityRecord]", city_name: str, topic_name: str) -> list:
    from .models import PersonRecord
    from datetime import datetime, timezone
    extracted_at = datetime.now(timezone.utc).isoformat()
    persons = []
    for rec in records:
        if not rec.leader:
            continue
        for name, role_desc in _parse_leader_field(rec.leader):
            name = name.strip()
            if not name:
                continue
            try:
                persons.append(PersonRecord(
                    name=name,
                    role="leader",
                    bio=role_desc or None,
                    city=city_name,
                    topic=topic_name,
                    community_name=rec.name,
                    community_id=rec.community_id or "",
                    source_url=rec.source_url,
                    extracted_at=extracted_at,
                ))
            except Exception:
                pass
    return persons


async def _enrich_record(
    record: "CommunityRecord",
    searxng: "FallbackSearchClient",
    extractor: Any,
    config: "PipelineConfig",
    semaphore: asyncio.Semaphore,
    on_progress: "Callable[[str | None, str | None], None] | None" = None,
    timing: "dict | None" = None,
    enrich_log_entry: "dict | None" = None,
    enrich_fp_examples: str = "",
) -> "CommunityRecord":
    """timing dict accumulates {"scrape": s, "extract": s, "count": n} across calls."""
    query = f'"{record.name}" {record.city}'
    if enrich_log_entry is not None:
        enrich_log_entry["search_query"] = query
    try:
        results = await searxng.search_all([query], locale=record.locale, num_results=3)
    except Exception:
        return record

    for result in results[:2]:
        url_log: dict = {"url": result.url, "fetched": False, "success": False}
        if enrich_log_entry is not None:
            enrich_log_entry["research_urls"].append(url_log)

        if on_progress:
            on_progress("enrich_scrape", record.source_url)
        t0 = time.monotonic()
        text = await fetch_and_clean(
            result.url, config.fetch_blocked_domains,
            config.fetch_timeout, config.fetch_min_text_length,
            semaphore,
        )
        if timing is not None:
            timing["scrape"] += time.monotonic() - t0
        if on_progress:
            on_progress(None, None)
        if not text:
            continue

        url_log["fetched"] = True

        if on_progress:
            on_progress("enrich_extract", record.source_url)
        t0 = time.monotonic()
        try:
            enriched = await extractor.enrich(record, text, false_positive_examples=enrich_fp_examples)
        except ExtractorUnavailableError as exc:
            log.warning("enrich_unavailable_skipped", community=record.name, reason=str(exc))
            return record  # best-effort: record proceeds unenriched
        finally:
            if timing is not None:
                timing["extract"] += time.monotonic() - t0
            if on_progress:
                on_progress(None, None)

        if enriched.website or enriched.social_links or enriched.contact:
            if enrich_log_entry is not None:
                fields: list[str] = []
                if not record.website and enriched.website:
                    fields.append("website")
                if not record.contact and enriched.contact:
                    fields.append("contact")
                if not record.social_links and enriched.social_links:
                    fields.append("social_links")
                enrich_log_entry["enriched"] = True
                enrich_log_entry["fields_added"] = fields
                url_log["success"] = True
            if timing is not None:
                timing["count"] += 1
            log.info("enriched", community=record.name, city=record.city, source=result.url)
            return enriched

    return record


def _window_closed(stop_at) -> bool:
    """True when a stop_at deadline is set and has passed (aware UTC datetime)."""
    if stop_at is None:
        return False
    from datetime import datetime, timezone
    return datetime.now(timezone.utc) >= stop_at


def _new_pair_log(city_name: str, topic_name: str, queries: list[str]) -> dict:
    """Full-key pair log. run_detail.html sums/iterates these keys with strict
    Jinja Undefined — every pair log (including failure entries) must carry all
    of them."""
    return {
        "city": city_name,
        "topic": topic_name,
        "queries": queries,
        "search_cache_hit": False,
        "urls_found": 0,
        "fetched_urls": [],
        "fetch_failed": 0,
        "cache_hits_scrape": 0,
        "cache_hits_extract": 0,
        "records_extracted": 0,
        "search_failed": False,
        "extract_failed": 0,
    }


def _tier_allows(city: "CityConfig", topic_name: str, core_topics: list[str]) -> bool:
    """topic_tier='core' cities only run core_topics; empty core_topics disables tiering."""
    return city.topic_tier != "core" or not core_topics or topic_name in core_topics


@dataclass
class CityConfig:
    name: str
    locale: str
    search_variants: list[str]
    country: str = ""
    # "full" = all topics; "core" = only PipelineConfig.core_topics (small towns
    # where most niche topics can't yield results — cuts search + LLM spend).
    topic_tier: str = "full"


@dataclass
class TopicConfig:
    name: str
    search_terms: dict[str, list[str]]


@dataclass
class PipelineConfig:
    search_results_per_query: int
    search_max_pages: int
    search_rate_limit: float
    fetch_timeout: int
    fetch_min_text_length: int
    fetch_max_concurrent: int
    fetch_blocked_domains: list[str]
    db_path: Path
    fetch_playwright_domains: list[str] = field(default_factory=list)
    dataforseo_login: str = ""
    dataforseo_password: str = ""
    deepseek_api_key: str = ""
    deepseek_model: str = "deepseek-chat"
    deepseek_temperature: float = 0.1
    deepseek_timeout: int = 60
    deepseek_max_text_chars: int = 8000
    deepseek_rate_limit_seconds: float = 1.0
    cache_skip_scraped: bool = True
    cache_skip_extracted: bool = True
    search_cache_ttl_days: int = 7
    enrich_communities: bool = True
    dataforseo_mode: str = "live"  # "standard" = task queue, 70% cheaper, minutes latency
    # Topics still searched for topic_tier="core" cities; empty = tiering disabled.
    core_topics: list[str] = field(default_factory=list)


async def run_pipeline(
    cities: list[CityConfig],
    topics: list[TopicConfig],
    config: PipelineConfig,
    cache: "CacheManager | None" = None,
    run_mode: str = "full",
    skip_scraped: bool | None = None,
    skip_extracted: bool | None = None,
    run_communities: bool = True,
    run_venues: bool = True,
    run_persons: bool = True,
    on_progress: Callable[[str | None, str | None], None] | None = None,
    on_pair_start: "Callable[[str, str], None] | None" = None,
    stop_at: "Any | None" = None,
) -> tuple[list[dict], int]:
    """stop_at: optional aware datetime (UTC) — pair loops stop gracefully once
    reached, so a run can be boxed into a time window (e.g. DeepSeek off-peak)."""
    # run_mode="search_only": search + fetch + cache raw text, zero LLM calls.
    # Pairs collect cheaply (DataForSEO standard mode); a later ai_only run
    # extracts the cached pages when DeepSeek is in its off-peak window.
    if run_mode == "search_only":
        run_communities = run_venues = run_persons = False
    _skip_scraped = skip_scraped if skip_scraped is not None else config.cache_skip_scraped
    _skip_extracted = skip_extracted if skip_extracted is not None else config.cache_skip_extracted

    primaries = []
    if config.deepseek_api_key:
        primaries.append(DeepSeekExtractor(
            api_key=config.deepseek_api_key,
            model=config.deepseek_model,
            temperature=config.deepseek_temperature,
            timeout_seconds=config.deepseek_timeout,
            max_text_chars=config.deepseek_max_text_chars,
            rate_limit_seconds=config.deepseek_rate_limit_seconds,
        ))
    extractor: FallbackExtractor = FallbackExtractor(primaries=primaries)
    log.info("extractor", primaries=[p.model for p in primaries])

    run_stats: dict[str, dict[str, int]] = {}
    total_new = 0
    pair_logs: list[dict] = []

    # Pre-compute done pairs (searched + all pages extracted with current FP) to skip entirely.
    # Tier-filtered: "core"-tier cities only pair with core_topics (see CityConfig.topic_tier),
    # which also keeps the catch-up pass from re-searching tiered-out pairs.
    all_pairs = {(c.name, t.name) for c in cities for t in topics
                 if _tier_allows(c, t.name, config.core_topics)}
    model = primaries[0].model if primaries else config.deepseek_model
    current_fp = get_extract_fingerprint(model)
    venue_fp = get_venue_fingerprint(model)
    person_fp = get_person_fingerprint(model)
    done_pairs: set[tuple[str, str]] = set()
    if _skip_extracted:
        if run_mode == "search_only":
            done_pairs = get_collected_pairs(config.db_path, config.search_max_pages)
        else:
            done_pairs = get_fully_processed_pairs(
                config.db_path,
                current_fp,
                venue_fp,
                person_fp,
                run_communities=run_communities,
                run_venues=run_venues,
                run_persons=run_persons,
                max_pages=config.search_max_pages,
            )
    pairs_to_run = all_pairs - done_pairs
    skipped = len(all_pairs) - len(pairs_to_run)
    if skipped:
        log.info("pairs_skipped_fully_processed", count=skipped, remaining=len(pairs_to_run))

    if not pairs_to_run:
        log.info("pipeline_all_pairs_done", run_mode=run_mode)
        return pair_logs, total_new

    if run_mode == "ai_only":
        total_new, pair_logs = await _run_ai_only(
            cities, topics, config, extractor, cache, _skip_extracted, run_stats, on_progress,
            run_communities=run_communities, run_venues=run_venues, run_persons=run_persons,
            on_pair_start=on_pair_start, pairs_filter=pairs_to_run, stop_at=stop_at,
        )
    else:
        reai_new, reai_logs = (0, [])
        if run_mode != "search_only":  # search_only never touches the LLM
            reai_new, reai_logs = await _run_ai_only(
                cities, topics, config, extractor, cache, _skip_extracted, run_stats, on_progress,
                run_communities=run_communities, run_venues=run_venues, run_persons=run_persons,
                on_pair_start=on_pair_start, pairs_filter=pairs_to_run, stop_at=stop_at,
            )
        full_new, full_logs = await _run_full(
            cities, topics, config, extractor, cache, _skip_scraped, _skip_extracted, run_stats, on_progress,
            run_communities=run_communities, run_venues=run_venues, run_persons=run_persons,
            on_pair_start=on_pair_start, pairs_filter=pairs_to_run, stop_at=stop_at,
        )
        total_new = reai_new + full_new
        pair_logs = reai_logs + full_logs

    if run_mode in ("full", "search_only"):
        covered = get_covered_pairs(config.db_path)
        uncovered = all_pairs - covered - done_pairs
        if uncovered and not _window_closed(stop_at):
            log.info("catchup_pass_start", pairs=len(uncovered))
            catchup_new, catchup_logs = await _run_full(
                cities, topics, config, extractor, cache,
                _skip_scraped, _skip_extracted, run_stats, on_progress,
                run_communities=run_communities, run_venues=run_venues,
                run_persons=run_persons, pairs_filter=uncovered,
                on_pair_start=on_pair_start, stop_at=stop_at,
            )
            total_new += catchup_new
            pair_logs += catchup_logs
            log.info("catchup_pass_complete", new_records=catchup_new, pairs=len(uncovered))

    log.info("pipeline_complete", run_mode=run_mode, total_new_records=total_new)
    try:
        from .duplicates import detect_all
        await asyncio.to_thread(detect_all, config.db_path)
    except Exception as exc:
        log.warning("post_run_duplicate_scan_failed", error=str(exc))
    failed_search = sum(1 for p in pair_logs if p.get("search_failed"))
    failed_extract = sum(p.get("extract_failed", 0) for p in pair_logs)
    if failed_search or failed_extract:
        log.warning("run_completed_with_failures",
                    search_failed_pairs=failed_search,
                    extract_failed_pages=failed_extract,
                    note="failed items were NOT cached and will be retried next run")
    return pair_logs, total_new


async def _run_full(
    cities: list[CityConfig],
    topics: list[TopicConfig],
    config: PipelineConfig,
    extractor: FallbackExtractor,
    cache: "CacheManager | None",
    skip_scraped: bool,
    skip_extracted: bool,
    run_stats: dict,
    on_progress: Callable[[str | None, str | None], None] | None,
    run_communities: bool = True,
    run_venues: bool = True,
    run_persons: bool = True,
    pairs_filter: set[tuple[str, str]] | None = None,
    on_pair_start: "Callable[[str, str], None] | None" = None,
    stop_at: "Any | None" = None,
) -> tuple[int, list[dict]]:
    search_primaries: list = []
    if config.dataforseo_login and config.dataforseo_password:
        search_primaries.append(DataForSEOClient(
            config.dataforseo_login, config.dataforseo_password,
            rate_limit_seconds=config.search_rate_limit,
            mode=config.dataforseo_mode,
        ))
    searxng: FallbackSearchClient = FallbackSearchClient(primaries=search_primaries)
    log.info("search_client", primaries=[type(p).__name__ for p in search_primaries])
    semaphore = asyncio.Semaphore(config.fetch_max_concurrent)

    pw_fetcher = None
    if config.fetch_playwright_domains:
        from .playwright_fetch import PlaywrightFetcher
        pw_fetcher = PlaywrightFetcher(config.fetch_playwright_domains)
        await pw_fetcher.start()

    all_fps = load_false_positives(config.db_path)
    enrich_fp_section = build_prompt_section(all_fps, fp_type="enrichment")
    total_new = 0
    pair_logs: list[dict] = []

    for city in cities:
        run_stats[city.name] = {}
        for topic in topics:
            if pairs_filter is not None and (city.name, topic.name) not in pairs_filter:
                continue
            # Tier guard (belt-and-suspenders — all_pairs is already tier-filtered,
            # but direct _run_full callers may pass no filter).
            if not _tier_allows(city, topic.name, config.core_topics):
                continue
            if _window_closed(stop_at):
                log.info("run_window_closed", city=city.name, topic=topic.name)
                return total_new, pair_logs
            await asyncio.sleep(0)
            if on_pair_start:
                on_pair_start(city.name, topic.name)
            log.info("processing_pair", city=city.name, topic=topic.name)

            terms = topic.search_terms.get(city.locale) or topic.search_terms.get("en", [])
            queries = build_queries(city.name, city.search_variants, terms)

            use_search_cache = skip_scraped and config.search_cache_ttl_days > 0
            search_cache_hit = False
            cached_urls = get_search_cache(config.db_path, city.name, topic.name,
                                           config.search_cache_ttl_days) if use_search_cache else None
            if cached_urls is not None:
                urls = cached_urls[:config.search_max_pages]
                urls_found = len(urls)
                search_cache_hit = True
                log.info("search_cache_hit", city=city.name, topic=topic.name, urls=len(urls))
            else:
                if searxng.exhausted:
                    # Provider down/quota gone: skip WITHOUT caching, so the pair is
                    # retried next run instead of being recorded as "searched, empty".
                    pair_logs.append({**_new_pair_log(city.name, topic.name, queries),
                                      "search_failed": True})
                    log.warning("search_unavailable_pair_skipped", city=city.name, topic=topic.name)
                    continue
                try:
                    search_results = await searxng.search_all(
                        queries, locale=city.locale, num_results=config.search_results_per_query,
                        stop_after=config.search_max_pages * 2,
                    )
                except (SearchQuotaError, SearchUnavailableError) as exc:
                    pair_logs.append({**_new_pair_log(city.name, topic.name, queries),
                                      "search_failed": True})
                    log.warning("search_unavailable_pair_skipped", city=city.name,
                                topic=topic.name, reason=str(exc))
                    continue
                from .fetch import _is_blocked as _url_blocked
                search_results = [
                    r for r in search_results
                    if not _url_blocked(r.url, config.fetch_blocked_domains)
                ]
                log.info("search_done", city=city.name, topic=topic.name, urls=len(search_results))
                all_urls = [r.url for r in search_results]
                urls = all_urls[:config.search_max_pages]
                urls_found = len(search_results)
                # Always record the search — even in Full Refresh mode and even when it
                # found nothing. An unsaved empty result is re-paid on every run (and
                # twice per run via the catch-up pass); an empty cached list correctly
                # marks the pair as done. The full (uncapped) URL list is stored; reads
                # apply [:search_max_pages].
                save_search_cache(config.db_path, city.name, topic.name, all_urls, queries)

            fetched: list[tuple[str, str]] = []
            pair_log = {**_new_pair_log(city.name, topic.name, queries),
                        "search_cache_hit": search_cache_hit, "urls_found": urls_found}

            urls_to_fetch = []
            for url in urls:
                if cache and skip_scraped:
                    cached_text = cache.get_scraped(url)
                    if cached_text:
                        log.debug("cache_hit_scrape", url=url)
                        fetched.append((url, cached_text))
                        pair_log["cache_hits_scrape"] += 1
                        pair_log["fetched_urls"].append(url)
                        continue
                urls_to_fetch.append(url)

            async def _fetch_one(url: str) -> tuple[str, str | None, float]:
                if on_progress:
                    on_progress("scrape", url)
                t0 = time.monotonic()
                text = await fetch_and_clean(
                    url, config.fetch_blocked_domains,
                    config.fetch_timeout, config.fetch_min_text_length,
                    semaphore,
                    playwright_fetcher=pw_fetcher,
                )
                dur = time.monotonic() - t0
                if on_progress:
                    on_progress(None, None)
                return url, text, dur

            for url, text, scrape_dur in await asyncio.gather(*[_fetch_one(u) for u in urls_to_fetch]):
                if text:
                    if cache:
                        cache.save_scraped(url, text, city.name, topic.name,
                                           duration_s=scrape_dur, source_queries=queries)
                    fetched.append((url, text))
                    pair_log["fetched_urls"].append(url)
                else:
                    pair_log["fetch_failed"] += 1

            log.info("fetch_done", city=city.name, topic=topic.name, pages=len(fetched))

            records = []
            for url, text in fetched:
                community_names: list[str] = []

                # ── Community extraction (with fingerprint cache) ────────────
                community_cache_hit = False
                if cache and skip_extracted:
                    cached_records = cache.get_extracted(url, fingerprint=extractor.canonical_fingerprint)
                    if cached_records is not None:
                        log.debug("cache_hit_extract", url=url)
                        records.extend(cached_records)
                        community_names = [r.name for r in cached_records]
                        pair_log["cache_hits_extract"] += 1
                        pair_log["records_extracted"] += len(cached_records)
                        community_cache_hit = True

                if not community_cache_hit and run_communities:
                    if extractor.exhausted:
                        # LLM quota gone for this run: leave the page un-extracted
                        # (cached raw text stays; next run picks it up). Do NOT
                        # cache an empty result.
                        pair_log["extract_failed"] += 1
                        continue
                    if on_progress:
                        on_progress("extract", url)
                    t0 = time.monotonic()
                    try:
                        extracted = await extractor.extract(
                            text=text, city=city.name, topic=topic.name,
                            locale=city.locale, source_url=url,
                            false_positive_examples=build_prompt_section(
                                all_fps, city=city.name, topic=topic.name
                            ),
                        )
                    except ExtractorUnavailableError as exc:
                        pair_log["extract_failed"] += 1
                        log.warning("extract_unavailable_page_skipped", url=url, reason=str(exc))
                        continue
                    finally:
                        extract_dur = time.monotonic() - t0
                        if on_progress:
                            on_progress(None, None)

                    joinable = [r for r in extracted if r.joinable]
                    if len(joinable) < len(extracted):
                        log.info("joinability_filtered", url=url,
                                 kept=len(joinable), removed=len(extracted) - len(joinable))

                    enrich_timing = {"scrape": 0.0, "extract": 0.0, "count": 0, "needed": False}
                    final_records = []
                    enrich_logs: list[dict] = []
                    for record in joinable:
                        log_entry: dict = {
                            "community_name": record.name,
                            "search_query": None,
                            "research_urls": [],
                            "enriched": False,
                            "fields_added": [],
                        }
                        if config.enrich_communities and _needs_enrichment(record) and (record.confidence or 0.0) >= 0.7:
                            enrich_timing["needed"] = True
                            record = await _enrich_record(
                                record, searxng, extractor, config, semaphore,
                                on_progress, enrich_timing, log_entry,
                                enrich_fp_examples=enrich_fp_section,
                            )
                        enrich_logs.append(log_entry)
                        final_records.append(record)

                    if cache:
                        cache.save_extracted(url, final_records, duration_s=extract_dur,
                                             fingerprint=extractor.canonical_fingerprint,
                                             model=extractor.model)
                        if enrich_timing["needed"]:
                            cache.mark_enrich_scraped(url, enrich_timing["scrape"])
                            cache.mark_enrich_extracted(url, enrich_timing["count"],
                                                        enrich_timing["extract"], model=extractor.model)
                            cache.save_enrich_log(url, enrich_logs)

                    # NB: no per-URL save_results — the pair-final batch save below
                    # covers it; saving per URL re-ran O(n²) dedup + a full topic
                    # DELETE+reinsert + a city-wide duplicate scan for every page.
                    records.extend(final_records)
                    total_new += len(final_records)
                    pair_log["records_extracted"] += len(final_records)
                    log.info("extracted", url=url, found=len(extracted), kept=len(final_records))
                    community_names = [r.name for r in final_records]

                # ── Venue extraction (with fingerprint cache) ────────────────
                # Gated on community_names like person extraction: pages with no
                # communities (the majority) skip the venue LLM call entirely.
                # Cost gate: skip the venue LLM call on 0-community pages — but only
                # when the communities pass ran; a venues-only run (run_communities
                # off) must extract unconditionally or it would be a no-op.
                if run_venues and (community_names or not run_communities) and not (cache and cache.get_venue_extracted(
                        url, fingerprint=extractor.canonical_venue_fingerprint) is not None):
                    try:
                        _topic_slugs = [t.name for t in topics]
                        venues = await extractor.extract_venues(
                            text, city.name, city.locale, url, valid_topics=_topic_slugs)
                        if venues:
                            upsert_venues(config.db_path, [v.model_dump() for v in venues])
                            log.info("venues_extracted", url=url, found=len(venues))
                        if cache:
                            cache.save_venue_extracted(url, [v.model_dump() for v in venues],
                                                       fingerprint=extractor.canonical_venue_fingerprint,
                                                       model=extractor.model)
                    except Exception as exc:
                        log.warning("venues_extract_error", url=url, error=str(exc))

                # ── Person extraction (with fingerprint cache) ───────────────
                if community_names:
                    _person_cache = cache.get_person_extracted(
                        url, city.name, topic.name,
                        fingerprint=extractor.canonical_person_fingerprint) if cache else None
                    if _person_cache is not None:
                        if _person_cache:
                            log.debug("person_cache_hit", url=url, cached=len(_person_cache))
                    elif run_persons:
                        try:
                            persons = await extractor.extract_persons(
                                text, city.name, topic.name, city.locale, url, community_names,
                            )
                            if persons:
                                upsert_persons(config.db_path, [p.model_dump() for p in persons])
                                log.info("persons_extracted", url=url, found=len(persons))
                            else:
                                log.info("persons_extract_zero", url=url,
                                         communities=len(community_names))
                            if cache:
                                cache.save_person_extracted(url, city.name, topic.name,
                                                            [p.model_dump() for p in persons],
                                                            fingerprint=extractor.canonical_person_fingerprint,
                                                            model=extractor.model)
                        except Exception as exc:
                            log.warning("persons_extract_error", url=url, error=str(exc))

            # ── Synthesize PersonRecords from community leader fields ────────
            if run_persons:
                leader_persons = _persons_from_leaders(records, city.name, topic.name)
                if leader_persons:
                    # Replace old leader persons community-by-community so stale
                    # or multi-name entries don't accumulate across re-runs.
                    seen_communities: set[str] = set()
                    for p in leader_persons:
                        if p.community_name not in seen_communities:
                            delete_leader_persons_for_community(
                                config.db_path, p.community_name, city.name)
                            seen_communities.add(p.community_name)
                    upsert_persons(config.db_path, [p.model_dump() for p in leader_persons])
                    log.info("persons_from_leaders", city=city.name, topic=topic.name,
                             found=len(leader_persons))

            count = save_results(city.name, topic.name, records, config.db_path)
            run_stats[city.name][topic.name] = count
            pair_logs.append(pair_log)

    if pw_fetcher:
        await pw_fetcher.stop()
    return total_new, pair_logs


async def _run_ai_only(
    cities: list[CityConfig],
    topics: list[TopicConfig],
    config: PipelineConfig,
    extractor: FallbackExtractor,
    cache: "CacheManager | None",
    skip_extracted: bool,
    run_stats: dict,
    on_progress: Callable[[str | None, str | None], None] | None,
    run_communities: bool = True,
    run_venues: bool = True,
    run_persons: bool = True,
    on_pair_start: "Callable[[str, str], None] | None" = None,
    pairs_filter: "set[tuple[str, str]] | None" = None,
    stop_at: "Any | None" = None,
) -> tuple[int, list[dict]]:
    if not cache:
        log.warning("ai_only_mode_no_cache")
        return 0, []

    all_scraped = await asyncio.to_thread(cache.get_all_scraped)
    log.info("ai_only_start", cached_pages=len(all_scraped),
             run_communities=run_communities, run_venues=run_venues, run_persons=run_persons)

    city_topic_pages: dict[tuple[str, str], list[tuple[str, str]]] = {}
    for url, text, city, topic in all_scraped:
        city_topic_pages.setdefault((city, topic), []).append((url, text))

    total_new = 0
    pair_logs: list[dict] = []

    for city in cities:
        run_stats[city.name] = {}
        for topic in topics:
            if pairs_filter is not None and (city.name, topic.name) not in pairs_filter:
                continue
            if _window_closed(stop_at):
                log.info("run_window_closed", city=city.name, topic=topic.name)
                return total_new, pair_logs
            await asyncio.sleep(0)
            if on_pair_start:
                on_pair_start(city.name, topic.name)
            pages = city_topic_pages.get((city.name, topic.name), [])
            pair_log: dict = {
                "city": city.name,
                "topic": topic.name,
                "queries": [],
                "urls_found": len(pages),
                "fetched_urls": [url for url, _ in pages],
                "cache_hits_scrape": len(pages),
                "cache_hits_extract": 0,
                "records_extracted": 0,
            }

            if not pages:
                log.info("ai_only_no_cache", city=city.name, topic=topic.name)
                run_stats[city.name][topic.name] = 0
                pair_logs.append(pair_log)
                continue

            log.info("ai_only_processing", city=city.name, topic=topic.name, pages=len(pages))
            records = []
            for url, text in pages:
                await asyncio.sleep(0)
                community_names: list[str] = []

                # ── Community extraction (with fingerprint cache) ────────────
                # Always read from cache for community_names (helps person extraction).
                # Only run fresh extraction when run_communities=True and cache misses.
                community_cache_hit = False
                cached = cache.get_extracted(url, fingerprint=extractor.canonical_fingerprint)
                if cached is not None:
                    log.debug("cache_hit_extract", url=url)
                    records.extend(cached)
                    community_names = [r.name for r in cached]
                    pair_log["cache_hits_extract"] += 1
                    pair_log["records_extracted"] += len(cached)
                    community_cache_hit = True

                if not community_cache_hit and run_communities:
                    if extractor.exhausted:
                        pair_log["extract_failed"] = pair_log.get("extract_failed", 0) + 1
                        continue
                    if on_progress:
                        on_progress("extract", url)
                    try:
                        extracted = await extractor.extract(
                            text=text, city=city.name, topic=topic.name,
                            locale=city.locale, source_url=url,
                        )
                    except ExtractorUnavailableError as exc:
                        pair_log["extract_failed"] = pair_log.get("extract_failed", 0) + 1
                        log.warning("extract_unavailable_page_skipped", url=url, reason=str(exc))
                        continue
                    finally:
                        if on_progress:
                            on_progress(None, None)

                    joinable = [r for r in extracted if r.joinable]
                    if len(joinable) < len(extracted):
                        log.info("joinability_filtered", url=url,
                                 kept=len(joinable), removed=len(extracted) - len(joinable))

                    cache.save_extracted(url, joinable, fingerprint=extractor.canonical_fingerprint,
                                         model=extractor.model)

                    records.extend(joinable)
                    total_new += len(joinable)
                    pair_log["records_extracted"] += len(joinable)
                    log.info("extracted", url=url, found=len(extracted), kept=len(joinable))
                    community_names = [r.name for r in joinable]

                # ── Venue extraction (with fingerprint cache) ────────────────
                # Gated on community_names — except for venues-only runs (see _run_full).
                if run_venues and (community_names or not run_communities) and cache.get_venue_extracted(
                        url, fingerprint=extractor.canonical_venue_fingerprint) is None:
                    try:
                        _topic_slugs = [t.name for t in topics]
                        venues = await extractor.extract_venues(
                            text, city.name, city.locale, url, valid_topics=_topic_slugs)
                        if venues:
                            upsert_venues(config.db_path, [v.model_dump() for v in venues])
                            log.info("venues_extracted", url=url, found=len(venues))
                        cache.save_venue_extracted(url, [v.model_dump() for v in venues],
                                                   fingerprint=extractor.canonical_venue_fingerprint,
                                                   model=extractor.model)
                    except Exception as exc:
                        log.warning("venues_extract_error", url=url, error=str(exc))

                # ── Person extraction (with fingerprint cache) ───────────────
                if community_names:
                    _person_cache = cache.get_person_extracted(
                        url, city.name, topic.name, fingerprint=extractor.canonical_person_fingerprint)
                    if _person_cache is not None:
                        if _person_cache:
                            log.debug("person_cache_hit", url=url, cached=len(_person_cache))
                    elif run_persons:
                        try:
                            persons = await extractor.extract_persons(
                                text, city.name, topic.name, city.locale, url, community_names,
                            )
                            if persons:
                                upsert_persons(config.db_path, [p.model_dump() for p in persons])
                                log.info("persons_extracted", url=url, found=len(persons))
                            else:
                                log.info("persons_extract_zero", url=url,
                                         communities=len(community_names))
                            cache.save_person_extracted(url, city.name, topic.name,
                                                        [p.model_dump() for p in persons],
                                                        fingerprint=extractor.canonical_person_fingerprint,
                                                        model=extractor.model)
                        except Exception as exc:
                            log.warning("persons_extract_error", url=url, error=str(exc))

            # ── Synthesize PersonRecords from community leader fields ────────
            if run_persons:
                leader_persons = _persons_from_leaders(records, city.name, topic.name)
                if leader_persons:
                    seen_communities: set[str] = set()
                    for p in leader_persons:
                        if p.community_name not in seen_communities:
                            delete_leader_persons_for_community(
                                config.db_path, p.community_name, city.name)
                            seen_communities.add(p.community_name)
                    upsert_persons(config.db_path, [p.model_dump() for p in leader_persons])
                    log.info("persons_from_leaders", city=city.name, topic=topic.name,
                             found=len(leader_persons))

            count = save_results(city.name, topic.name, records, config.db_path)
            run_stats[city.name][topic.name] = count
            pair_logs.append(pair_log)

    return total_new, pair_logs


async def scrape_submitted_url(
    db_path: Path,
    config: "PipelineConfig",
    city: str,
    topic: str,
    url: str,
) -> bool:
    primaries = []
    if config.deepseek_api_key:
        primaries.append(DeepSeekExtractor(
            api_key=config.deepseek_api_key,
            model=config.deepseek_model,
            temperature=config.deepseek_temperature,
            timeout_seconds=config.deepseek_timeout,
            max_text_chars=config.deepseek_max_text_chars,
            rate_limit_seconds=config.deepseek_rate_limit_seconds,
        ))
    extractor: FallbackExtractor = FallbackExtractor(primaries=primaries)

    text = await fetch_and_clean(url, blocked_domains=[], timeout_seconds=15)
    if not text:
        log.warning("scrape_submitted_url_no_text", url=url)
        return False

    all_fps = load_false_positives(db_path)
    try:
        records = await extractor.extract(
            text=text, city=city, topic=topic, locale="hu", source_url=url,
            false_positive_examples=build_prompt_section(all_fps, city=city, topic=topic),
        )
    except ExtractorUnavailableError as exc:
        # BackgroundTasks has no error surface — log loudly; the submission stays
        # approved and can be re-run from the admin cache page.
        log.error("scrape_submitted_url_extract_failed", city=city, topic=topic,
                  url=url, reason=str(exc))
        return False
    save_results(city, topic, records, db_path)
    log.info("scrape_submitted_url_done", city=city, topic=topic, url=url, found=len(records))
    return True


async def reextract_community(
    db_path: Path,
    config: "PipelineConfig",
    community_id: str,
) -> bool:
    record = find_community_by_id(db_path, community_id)
    if not record:
        log.warning("reextract_community_not_found", community_id=community_id)
        return False

    source_url = record.get("source_url", "")
    if not source_url:
        log.warning("reextract_community_no_source_url", community_id=community_id)
        return False

    city = record.get("city", "")
    topic = record.get("topic", "")

    url_hash = hashlib.sha256(source_url.encode()).hexdigest()[:16]
    cached = load_cache_page(db_path, url_hash)
    text = cached.get("raw_text") if cached else None

    if not text:
        text = await fetch_and_clean(source_url, blocked_domains=[], timeout_seconds=15)
    if not text:
        log.warning("reextract_community_no_text", community_id=community_id, url=source_url)
        return False

    primaries = []
    if config.deepseek_api_key:
        primaries.append(DeepSeekExtractor(
            api_key=config.deepseek_api_key,
            model=config.deepseek_model,
            temperature=config.deepseek_temperature,
            timeout_seconds=config.deepseek_timeout,
            max_text_chars=config.deepseek_max_text_chars,
            rate_limit_seconds=config.deepseek_rate_limit_seconds,
        ))
    extractor: FallbackExtractor = FallbackExtractor(primaries=primaries)

    all_fps = load_false_positives(db_path)
    try:
        records = await extractor.extract(
            text=text, city=city, topic=topic, locale=record.get("locale", "hu"), source_url=source_url,
            false_positive_examples=build_prompt_section(all_fps, city=city, topic=topic),
        )
    except ExtractorUnavailableError as exc:
        log.error("reextract_community_extract_failed", community_id=community_id,
                  reason=str(exc))
        return False
    save_results(city, topic, records, db_path)
    log.info("reextract_community_done", community_id=community_id, found=len(records))
    return True


async def reextract_with_search_fallback(
    db_path: Path,
    config: "PipelineConfig",
    community_id: str,
) -> bool:
    """Re-extract from existing source; if still no description, search the web for this community.

    Search fallback helps both with missing descriptions and wrong-city assignments —
    fresh search results for "{name} {city}" naturally surface better source pages.
    """
    # Step 1: re-extract from cached/live source URL
    await reextract_community(db_path, config, community_id)

    record = find_community_by_id(db_path, community_id)
    if not record:
        return False
    if record.get("description"):
        return True  # description filled, done

    # Step 2: search the web for this community by name
    name = record.get("name", "")
    city = record.get("city", "")
    topic = record.get("topic", "")
    locale = record.get("locale", "hu")

    if not name or not city:
        return False

    search_primaries: list = []
    if config.dataforseo_login and config.dataforseo_password:
        search_primaries.append(DataForSEOClient(
            config.dataforseo_login, config.dataforseo_password,
            rate_limit_seconds=config.search_rate_limit,
        ))
    if not search_primaries:
        log.warning("reextract_search_fallback_no_client", community_id=community_id)
        return False

    searcher = FallbackSearchClient(primaries=search_primaries)
    query = f'"{name}" {city}'
    log.info("reextract_search_fallback", community_id=community_id, name=name, query=query)

    try:
        results = await searcher.search(query, locale=locale, num_results=5)
    except Exception as exc:
        log.warning("reextract_search_fallback_search_failed", community_id=community_id, error=str(exc))
        return False

    ext_primaries: list = []
    if config.deepseek_api_key:
        ext_primaries.append(DeepSeekExtractor(
            api_key=config.deepseek_api_key,
            model=config.deepseek_model,
            temperature=config.deepseek_temperature,
            timeout_seconds=config.deepseek_timeout,
            max_text_chars=config.deepseek_max_text_chars,
            rate_limit_seconds=config.deepseek_rate_limit_seconds,
        ))
    if not ext_primaries:
        return False

    extractor = FallbackExtractor(primaries=ext_primaries)
    all_fps = load_false_positives(db_path)
    fp_section = build_prompt_section(all_fps, city=city, topic=topic)
    blocked = set(config.fetch_blocked_domains)

    for result in results[:5]:
        url = result.url
        if any(d in url for d in blocked):
            continue
        try:
            text = await fetch_and_clean(url, blocked_domains=list(blocked), timeout_seconds=15)
        except Exception:
            continue
        if not text:
            continue
        try:
            extracted = await extractor.extract(
                text=text, city=city, topic=topic, locale=locale, source_url=url,
                false_positive_examples=fp_section,
            )
        except Exception as exc:
            log.warning("reextract_search_fallback_extract_failed", url=url, error=str(exc))
            continue
        if extracted:
            save_results(city, topic, extracted, db_path)
            refreshed = find_community_by_id(db_path, community_id)
            if refreshed and refreshed.get("description"):
                log.info("reextract_search_fallback_success", community_id=community_id, url=url)
                return True

    log.info("reextract_search_fallback_exhausted", community_id=community_id)
    return False

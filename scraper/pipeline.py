import asyncio
import hashlib
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

import structlog

from .extract import DeepSeekExtractor, FallbackExtractor, GroqExtractor
from .false_positives import build_prompt_section
from .false_positives import load as load_false_positives
from .fetch import fetch_and_clean
from .search import DataForSEOClient, FallbackSearchClient, GooglePlaywrightSearchClient, SerperSearchClient, build_queries
from .db import get_search_cache, save_search_cache, get_covered_pairs, upsert_venues, upsert_persons, delete_leader_persons_for_community, load_cache_page, find_community_by_id
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


@dataclass
class CityConfig:
    name: str
    locale: str
    search_variants: list[str]
    country: str = ""


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
    serper_api_key: str = ""
    dataforseo_login: str = ""
    dataforseo_password: str = ""
    deepseek_api_key: str = ""
    deepseek_model: str = "deepseek-chat"
    deepseek_temperature: float = 0.1
    deepseek_timeout: int = 60
    deepseek_max_text_chars: int = 8000
    deepseek_rate_limit_seconds: float = 1.0
    groq_api_key: str = ""
    groq_model: str = "llama-3.3-70b-versatile"
    groq_temperature: float = 0.1
    groq_timeout: int = 60
    groq_max_text_chars: int = 4000
    groq_rate_limit_seconds: float = 4.0
    cache_skip_scraped: bool = True
    cache_skip_extracted: bool = True
    search_cache_ttl_days: int = 7
    enrich_communities: bool = True


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
) -> tuple[list[dict], int]:
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
    if config.groq_api_key:
        primaries.append(GroqExtractor(
            api_key=config.groq_api_key,
            model=config.groq_model,
            temperature=config.groq_temperature,
            timeout_seconds=config.groq_timeout,
            max_text_chars=config.groq_max_text_chars,
            rate_limit_seconds=config.groq_rate_limit_seconds,
        ))
    extractor: FallbackExtractor = FallbackExtractor(primaries=primaries)
    log.info("extractor", primaries=[p.model for p in primaries])

    run_stats: dict[str, dict[str, int]] = {}
    total_new = 0
    pair_logs: list[dict] = []

    if run_mode == "ai_only":
        total_new, pair_logs = await _run_ai_only(
            cities, topics, config, extractor, cache, _skip_extracted, run_stats, on_progress,
            run_communities=run_communities, run_venues=run_venues, run_persons=run_persons,
        )
    else:
        reai_new, reai_logs = await _run_ai_only(
            cities, topics, config, extractor, cache, _skip_extracted, run_stats, on_progress,
            run_communities=run_communities, run_venues=run_venues, run_persons=run_persons,
        )
        full_new, full_logs = await _run_full(
            cities, topics, config, extractor, cache, _skip_scraped, _skip_extracted, run_stats, on_progress,
            run_communities=run_communities, run_venues=run_venues, run_persons=run_persons,
        )
        total_new = reai_new + full_new
        pair_logs = reai_logs + full_logs

    if run_mode == "full":
        covered = get_covered_pairs(config.db_path)
        all_pairs = {(c.name, t.name) for c in cities for t in topics}
        uncovered = all_pairs - covered
        if uncovered:
            log.info("catchup_pass_start", pairs=len(uncovered))
            catchup_new, catchup_logs = await _run_full(
                cities, topics, config, extractor, cache,
                _skip_scraped, _skip_extracted, run_stats, on_progress,
                run_communities=run_communities, run_venues=run_venues,
                run_persons=run_persons, pairs_filter=uncovered,
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
) -> tuple[int, list[dict]]:
    google_search = GooglePlaywrightSearchClient(rate_limit_seconds=8.0)
    await google_search.start()

    search_primaries: list = [google_search]
    if config.dataforseo_login and config.dataforseo_password:
        search_primaries.append(DataForSEOClient(
            config.dataforseo_login, config.dataforseo_password,
            rate_limit_seconds=config.search_rate_limit,
        ))
    if config.serper_api_key:
        search_primaries.append(SerperSearchClient(config.serper_api_key, rate_limit_seconds=config.search_rate_limit))
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
            await asyncio.sleep(0)
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
                search_results = await searxng.search_all(
                    queries, locale=city.locale, num_results=config.search_results_per_query,
                )
                log.info("search_done", city=city.name, topic=topic.name, urls=len(search_results))
                urls = [r.url for r in search_results][:config.search_max_pages]
                urls_found = len(search_results)
                if use_search_cache and urls:
                    save_search_cache(config.db_path, city.name, topic.name, urls, queries)

            fetched: list[tuple[str, str]] = []
            pair_log: dict = {
                "city": city.name,
                "topic": topic.name,
                "queries": queries,
                "search_cache_hit": search_cache_hit,
                "urls_found": urls_found,
                "fetched_urls": [],
                "fetch_failed": 0,
                "cache_hits_scrape": 0,
                "cache_hits_extract": 0,
                "records_extracted": 0,
            }

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
                    cached_records = cache.get_extracted(url, fingerprint=extractor.model_fingerprint)
                    if cached_records is not None:
                        log.debug("cache_hit_extract", url=url)
                        records.extend(cached_records)
                        community_names = [r.name for r in cached_records]
                        pair_log["cache_hits_extract"] += 1
                        pair_log["records_extracted"] += len(cached_records)
                        community_cache_hit = True

                if not community_cache_hit and run_communities:
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
                                             fingerprint=extractor.model_fingerprint,
                                             model=extractor.model)
                        if enrich_timing["needed"]:
                            cache.mark_enrich_scraped(url, enrich_timing["scrape"])
                            cache.mark_enrich_extracted(url, enrich_timing["count"],
                                                        enrich_timing["extract"], model=extractor.model)
                            cache.save_enrich_log(url, enrich_logs)

                    if final_records:
                        save_results(city.name, topic.name, final_records, config.db_path)

                    records.extend(final_records)
                    total_new += len(final_records)
                    pair_log["records_extracted"] += len(final_records)
                    log.info("extracted", url=url, found=len(extracted), kept=len(final_records))
                    community_names = [r.name for r in final_records]

                # ── Venue extraction (with fingerprint cache) ────────────────
                if run_venues and not (cache and cache.get_venue_extracted(
                        url, fingerprint=extractor.venue_fingerprint) is not None):
                    try:
                        _topic_slugs = [t.name for t in topics]
                        venues = await extractor.extract_venues(
                            text, city.name, city.locale, url, valid_topics=_topic_slugs)
                        if venues:
                            upsert_venues(config.db_path, [v.model_dump() for v in venues])
                            log.info("venues_extracted", url=url, found=len(venues))
                        if cache:
                            cache.save_venue_extracted(url, [v.model_dump() for v in venues],
                                                       fingerprint=extractor.venue_fingerprint,
                                                       model=extractor.model)
                    except Exception as exc:
                        log.warning("venues_extract_error", url=url, error=str(exc))

                # ── Person extraction (with fingerprint cache) ───────────────
                if community_names:
                    _person_cache = cache.get_person_extracted(
                        url, city.name, topic.name,
                        fingerprint=extractor.person_fingerprint) if cache else None
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
                                                            fingerprint=extractor.person_fingerprint,
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
    await google_search.stop()
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
            await asyncio.sleep(0)
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
                cached = cache.get_extracted(url, fingerprint=extractor.model_fingerprint)
                if cached is not None:
                    log.debug("cache_hit_extract", url=url)
                    records.extend(cached)
                    community_names = [r.name for r in cached]
                    pair_log["cache_hits_extract"] += 1
                    pair_log["records_extracted"] += len(cached)
                    community_cache_hit = True

                if not community_cache_hit and run_communities:
                    if on_progress:
                        on_progress("extract", url)
                    try:
                        extracted = await extractor.extract(
                            text=text, city=city.name, topic=topic.name,
                            locale=city.locale, source_url=url,
                        )
                    finally:
                        if on_progress:
                            on_progress(None, None)

                    joinable = [r for r in extracted if r.joinable]
                    if len(joinable) < len(extracted):
                        log.info("joinability_filtered", url=url,
                                 kept=len(joinable), removed=len(extracted) - len(joinable))

                    cache.save_extracted(url, joinable, fingerprint=extractor.model_fingerprint,
                                         model=extractor.model)

                    if joinable:
                        save_results(city.name, topic.name, joinable, config.db_path)

                    records.extend(joinable)
                    total_new += len(joinable)
                    pair_log["records_extracted"] += len(joinable)
                    log.info("extracted", url=url, found=len(extracted), kept=len(joinable))
                    community_names = [r.name for r in joinable]

                # ── Venue extraction (with fingerprint cache) ────────────────
                if run_venues and cache.get_venue_extracted(
                        url, fingerprint=extractor.venue_fingerprint) is None:
                    try:
                        _topic_slugs = [t.name for t in topics]
                        venues = await extractor.extract_venues(
                            text, city.name, city.locale, url, valid_topics=_topic_slugs)
                        if venues:
                            upsert_venues(config.db_path, [v.model_dump() for v in venues])
                            log.info("venues_extracted", url=url, found=len(venues))
                        cache.save_venue_extracted(url, [v.model_dump() for v in venues],
                                                   fingerprint=extractor.venue_fingerprint,
                                                   model=extractor.model)
                    except Exception as exc:
                        log.warning("venues_extract_error", url=url, error=str(exc))

                # ── Person extraction (with fingerprint cache) ───────────────
                if community_names:
                    _person_cache = cache.get_person_extracted(
                        url, city.name, topic.name, fingerprint=extractor.person_fingerprint)
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
                                                        fingerprint=extractor.person_fingerprint,
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
    if config.groq_api_key:
        primaries.append(GroqExtractor(
            api_key=config.groq_api_key,
            model=config.groq_model,
            temperature=config.groq_temperature,
            timeout_seconds=config.groq_timeout,
            max_text_chars=config.groq_max_text_chars,
            rate_limit_seconds=config.groq_rate_limit_seconds,
        ))
    extractor: FallbackExtractor = FallbackExtractor(primaries=primaries)

    text = await fetch_and_clean(url, blocked_domains=[], timeout_seconds=15)
    if not text:
        log.warning("scrape_submitted_url_no_text", url=url)
        return False

    all_fps = load_false_positives(db_path)
    records = await extractor.extract(
        text=text, city=city, topic=topic, locale="hu", source_url=url,
        false_positive_examples=build_prompt_section(all_fps, city=city, topic=topic),
    )
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
    if config.groq_api_key:
        primaries.append(GroqExtractor(
            api_key=config.groq_api_key,
            model=config.groq_model,
            temperature=config.groq_temperature,
            timeout_seconds=config.groq_timeout,
            max_text_chars=config.groq_max_text_chars,
            rate_limit_seconds=config.groq_rate_limit_seconds,
        ))
    extractor: FallbackExtractor = FallbackExtractor(primaries=primaries)

    all_fps = load_false_positives(db_path)
    records = await extractor.extract(
        text=text, city=city, topic=topic, locale=record.get("locale", "hu"), source_url=source_url,
        false_positive_examples=build_prompt_section(all_fps, city=city, topic=topic),
    )
    save_results(city, topic, records, db_path)
    log.info("reextract_community_done", community_id=community_id, found=len(records))
    return True

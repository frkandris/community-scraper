import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Callable

import structlog

from .extract import OllamaExtractor
from .false_positives import build_prompt_section
from .false_positives import load as load_false_positives
from .fetch import fetch_and_clean
from .search import BraveSearchClient, SearXNGClient, build_queries
from .store import save_results, update_metadata
from .vcs import commit_data

if TYPE_CHECKING:
    from .cache import CacheManager
    from .models import CommunityRecord

log = structlog.get_logger()


def _needs_enrichment(record: "CommunityRecord") -> bool:
    return not record.website and not record.social_links and not record.contact


async def _enrich_record(
    record: "CommunityRecord",
    searxng: "SearXNGClient | BraveSearchClient",
    extractor: "OllamaExtractor",
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
    searxng_url: str
    ollama_url: str
    ollama_model: str
    ollama_temperature: float
    ollama_timeout: int
    ollama_max_text_chars: int
    search_results_per_query: int
    search_max_pages: int
    search_rate_limit: float
    fetch_timeout: int
    fetch_min_text_length: int
    fetch_max_concurrent: int
    fetch_blocked_domains: list[str]
    commit_after_run: bool
    data_dir: Path
    repo_dir: Path
    brave_api_key: str = ""
    cache_skip_scraped: bool = True
    cache_skip_extracted: bool = True
    enrich_communities: bool = True


async def run_pipeline(
    cities: list[CityConfig],
    topics: list[TopicConfig],
    config: PipelineConfig,
    cache: "CacheManager | None" = None,
    run_mode: str = "full",
    skip_scraped: bool | None = None,
    skip_extracted: bool | None = None,
    on_progress: Callable[[str | None, str | None], None] | None = None,
) -> list[dict]:
    _skip_scraped = skip_scraped if skip_scraped is not None else config.cache_skip_scraped
    _skip_extracted = skip_extracted if skip_extracted is not None else config.cache_skip_extracted

    extractor = OllamaExtractor(
        base_url=config.ollama_url,
        model=config.ollama_model,
        temperature=config.ollama_temperature,
        timeout_seconds=config.ollama_timeout,
        max_text_chars=config.ollama_max_text_chars,
    )

    run_stats: dict[str, dict[str, int]] = {}
    total_new = 0
    pair_logs: list[dict] = []

    if run_mode == "ai_only":
        total_new, pair_logs = await _run_ai_only(
            cities, topics, config, extractor, cache, _skip_extracted, run_stats, on_progress
        )
    else:
        total_new, pair_logs = await _run_full(
            cities, topics, config, extractor, cache, _skip_scraped, _skip_extracted, run_stats, on_progress
        )

    update_metadata(run_stats, config.data_dir)
    log.info("pipeline_complete", run_mode=run_mode, total_new_records=total_new)

    if config.commit_after_run:
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        message = f"scraper: {timestamp} – {total_new} records ({run_mode})"
        commit_data(config.repo_dir, message)

    return pair_logs


async def _run_full(
    cities: list[CityConfig],
    topics: list[TopicConfig],
    config: PipelineConfig,
    extractor: OllamaExtractor,
    cache: "CacheManager | None",
    skip_scraped: bool,
    skip_extracted: bool,
    run_stats: dict,
    on_progress: Callable[[str | None, str | None], None] | None,
) -> tuple[int, list[dict]]:
    if config.brave_api_key:
        searxng: BraveSearchClient | SearXNGClient = BraveSearchClient(
            config.brave_api_key, rate_limit_seconds=config.search_rate_limit
        )
        log.info("search_client", backend="brave")
    else:
        searxng = SearXNGClient(config.searxng_url, rate_limit_seconds=config.search_rate_limit)
        log.info("search_client", backend="searxng")
    semaphore = asyncio.Semaphore(config.fetch_max_concurrent)
    all_fps = load_false_positives(config.data_dir)
    enrich_fp_section = build_prompt_section(all_fps, fp_type="enrichment")
    total_new = 0
    pair_logs: list[dict] = []

    for city in cities:
        run_stats[city.name] = {}
        for topic in topics:
            log.info("processing_pair", city=city.name, topic=topic.name)

            terms = topic.search_terms.get(city.locale) or topic.search_terms.get("en", [])
            queries = build_queries(city.name, city.search_variants, terms)

            search_results = await searxng.search_all(
                queries, locale=city.locale, num_results=config.search_results_per_query,
            )
            log.info("search_done", city=city.name, topic=topic.name, urls=len(search_results))

            urls = [r.url for r in search_results][:config.search_max_pages]
            fetched: list[tuple[str, str]] = []
            pair_log: dict = {
                "city": city.name,
                "topic": topic.name,
                "queries": queries,
                "urls_found": len(search_results),
                "fetched_urls": [],
                "fetch_failed": 0,
                "cache_hits_scrape": 0,
                "cache_hits_extract": 0,
                "records_extracted": 0,
            }

            for url in urls:
                if cache and skip_scraped:
                    cached_text = cache.get_scraped(url)
                    if cached_text:
                        log.debug("cache_hit_scrape", url=url)
                        fetched.append((url, cached_text))
                        pair_log["cache_hits_scrape"] += 1
                        pair_log["fetched_urls"].append(url)
                        continue

                if on_progress:
                    on_progress("scrape", url)
                t0 = time.monotonic()
                text = await fetch_and_clean(
                    url, config.fetch_blocked_domains,
                    config.fetch_timeout, config.fetch_min_text_length,
                    semaphore,
                )
                scrape_dur = time.monotonic() - t0
                if on_progress:
                    on_progress(None, None)
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
                if cache and skip_extracted:
                    cached_records = cache.get_extracted(url)
                    if cached_records is not None:
                        log.debug("cache_hit_extract", url=url)
                        records.extend(cached_records)
                        pair_log["cache_hits_extract"] += 1
                        pair_log["records_extracted"] += len(cached_records)
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
                finally:
                    extract_dur = time.monotonic() - t0
                    if on_progress:
                        on_progress(None, None)

                # Filter out non-joinable communities
                joinable = [r for r in extracted if r.joinable]
                if len(joinable) < len(extracted):
                    log.info("joinability_filtered", url=url,
                             kept=len(joinable), removed=len(extracted) - len(joinable))

                # Enrich records that have no contact info
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
                    if config.enrich_communities and _needs_enrichment(record):
                        enrich_timing["needed"] = True
                        record = await _enrich_record(
                            record, searxng, extractor, config, semaphore,
                            on_progress, enrich_timing, log_entry,
                            enrich_fp_examples=enrich_fp_section,
                        )
                    enrich_logs.append(log_entry)
                    final_records.append(record)

                if cache:
                    cache.save_extracted(url, final_records, duration_s=extract_dur)
                    if enrich_timing["needed"]:
                        cache.mark_enrich_scraped(url, enrich_timing["scrape"])
                        cache.mark_enrich_extracted(url, enrich_timing["count"], enrich_timing["extract"])
                        cache.save_enrich_log(url, enrich_logs)

                # Persist immediately — safe even if server restarts mid-run
                if final_records:
                    save_results(city.name, topic.name, final_records, config.data_dir)

                records.extend(final_records)
                total_new += len(final_records)
                pair_log["records_extracted"] += len(final_records)
                log.info("extracted", url=url, found=len(extracted), kept=len(final_records))

            # Final merge for the pair (also covers cache-hit records) and accurate count
            count = save_results(city.name, topic.name, records, config.data_dir)
            run_stats[city.name][topic.name] = count
            update_metadata(run_stats, config.data_dir)
            pair_logs.append(pair_log)

        # Commit after each city completes (all its topics)
        if config.commit_after_run:
            city_total = sum(run_stats.get(city.name, {}).values())
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            commit_data(config.repo_dir, f"scraper: {ts} – {city.name} – {city_total} records")

    return total_new, pair_logs


async def _run_ai_only(
    cities: list[CityConfig],
    topics: list[TopicConfig],
    config: PipelineConfig,
    extractor: OllamaExtractor,
    cache: "CacheManager | None",
    skip_extracted: bool,
    run_stats: dict,
    on_progress: Callable[[str | None, str | None], None] | None,
) -> tuple[int, list[dict]]:
    if not cache:
        log.warning("ai_only_mode_no_cache")
        return 0, []

    all_scraped = cache.get_all_scraped()
    log.info("ai_only_start", cached_pages=len(all_scraped))

    city_topic_pages: dict[tuple[str, str], list[tuple[str, str]]] = {}
    for url, text, city, topic in all_scraped:
        city_topic_pages.setdefault((city, topic), []).append((url, text))

    total_new = 0
    pair_logs: list[dict] = []

    for city in cities:
        run_stats[city.name] = {}
        for topic in topics:
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
                if skip_extracted:
                    cached = cache.get_extracted(url)
                    if cached is not None:
                        log.debug("cache_hit_extract", url=url)
                        records.extend(cached)
                        pair_log["cache_hits_extract"] += 1
                        pair_log["records_extracted"] += len(cached)
                        continue

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

                cache.save_extracted(url, extracted)

                if extracted:
                    save_results(city.name, topic.name, extracted, config.data_dir)

                records.extend(extracted)
                total_new += len(extracted)
                pair_log["records_extracted"] += len(extracted)
                log.info("extracted", url=url, found=len(extracted))

            count = save_results(city.name, topic.name, records, config.data_dir)
            run_stats[city.name][topic.name] = count
            update_metadata(run_stats, config.data_dir)
            pair_logs.append(pair_log)

        if config.commit_after_run:
            city_total = sum(run_stats.get(city.name, {}).values())
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            commit_data(config.repo_dir, f"scraper: {ts} – {city.name} – {city_total} records (ai_only)")

    return total_new, pair_logs

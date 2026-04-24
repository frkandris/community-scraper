from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import structlog

from .extract import OllamaExtractor
from .fetch import fetch_many
from .search import SearXNGClient, build_queries
from .store import save_results, update_metadata
from .vcs import commit_data

log = structlog.get_logger()


@dataclass
class CityConfig:
    name: str
    locale: str
    search_variants: list[str]


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


async def run_pipeline(
    cities: list[CityConfig],
    topics: list[TopicConfig],
    config: PipelineConfig,
) -> None:
    searxng = SearXNGClient(config.searxng_url, rate_limit_seconds=config.search_rate_limit)
    extractor = OllamaExtractor(
        base_url=config.ollama_url,
        model=config.ollama_model,
        temperature=config.ollama_temperature,
        timeout_seconds=config.ollama_timeout,
        max_text_chars=config.ollama_max_text_chars,
    )

    run_stats: dict[str, dict[str, int]] = {}
    total_records = 0

    for city in cities:
        run_stats[city.name] = {}
        for topic in topics:
            log.info("processing_pair", city=city.name, topic=topic.name)

            terms = topic.search_terms.get(city.locale) or topic.search_terms.get("en", [])
            queries = build_queries(city.name, city.search_variants, terms)

            search_results = await searxng.search_all(
                queries,
                locale=city.locale,
                num_results=config.search_results_per_query,
            )
            log.info("search_done", city=city.name, topic=topic.name, urls=len(search_results))

            urls = [r.url for r in search_results]
            fetched = await fetch_many(
                urls,
                blocked_domains=config.fetch_blocked_domains,
                max_pages=config.search_max_pages,
                timeout_seconds=config.fetch_timeout,
                min_text_length=config.fetch_min_text_length,
                max_concurrent=config.fetch_max_concurrent,
            )
            log.info("fetch_done", city=city.name, topic=topic.name, pages=len(fetched))

            records = []
            for url, text in fetched:
                extracted = await extractor.extract(
                    text=text,
                    city=city.name,
                    topic=topic.name,
                    locale=city.locale,
                    source_url=url,
                )
                records.extend(extracted)
                log.info("extracted", url=url, found=len(extracted))

            count = save_results(city.name, topic.name, records, config.data_dir)
            run_stats[city.name][topic.name] = count
            total_records += len(records)

    update_metadata(run_stats, config.data_dir)
    log.info("pipeline_complete", total_new_records=total_records)

    if config.commit_after_run:
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        message = f"scraper: {timestamp} – {total_records} records"
        commit_data(config.repo_dir, message)

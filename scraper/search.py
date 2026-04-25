import asyncio
import structlog
import httpx

from .models import SearchResult

log = structlog.get_logger()

LOCALE_TO_LANGUAGE = {
    "hu": "hu-HU",
    "en": "en-US",
    "de": "de-DE",
    "fr": "fr-FR",
    "es": "es-ES",
}


class SearXNGClient:
    def __init__(self, base_url: str, rate_limit_seconds: float = 1.5):
        self.base_url = base_url.rstrip("/")
        self.rate_limit_seconds = rate_limit_seconds
        self._last_request_time: float = 0.0

    async def search(
        self,
        query: str,
        locale: str = "en",
        num_results: int = 10,
    ) -> list[SearchResult]:
        await self._rate_limit()
        language = LOCALE_TO_LANGUAGE.get(locale, "en-US")
        params = {
            "q": query,
            "format": "json",
            "language": language,
            "pageno": 1,
        }
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.get(f"{self.base_url}/search", params=params)
                resp.raise_for_status()
                data = resp.json()
        except Exception as exc:
            log.warning("searxng_search_failed", query=query, error=str(exc))
            return []

        raw_count = len(data.get("results", []))
        if raw_count == 0:
            answers = data.get("answers", [])
            suggestions = data.get("suggestions", [])
            log.warning("searxng_empty_results", query=query, language=language,
                        answers=len(answers), suggestions=len(suggestions),
                        unresponsive=data.get("unresponsive_engines", []))
        else:
            log.debug("searxng_results", query=query, raw=raw_count)

        results = []
        for item in data.get("results", [])[:num_results]:
            results.append(SearchResult(
                url=item.get("url", ""),
                title=item.get("title", ""),
                snippet=item.get("content", ""),
            ))
        return results

    async def search_all(
        self,
        queries: list[str],
        locale: str = "en",
        num_results: int = 10,
    ) -> list[SearchResult]:
        seen_urls: set[str] = set()
        combined: list[SearchResult] = []
        for query in queries:
            results = await self.search(query, locale=locale, num_results=num_results)
            for r in results:
                if r.url not in seen_urls:
                    seen_urls.add(r.url)
                    combined.append(r)
        return combined

    async def _rate_limit(self) -> None:
        import time
        now = time.monotonic()
        elapsed = now - self._last_request_time
        if elapsed < self.rate_limit_seconds:
            await asyncio.sleep(self.rate_limit_seconds - elapsed)
        self._last_request_time = time.monotonic()


def build_queries(
    city_name: str,
    search_variants: list[str],
    topic_terms: list[str],
) -> list[str]:
    queries = []
    primary_variant = search_variants[0]
    for term in topic_terms[:2]:
        queries.append(f"{term} {primary_variant}")
    if len(search_variants) > 1:
        queries.append(f"{topic_terms[0]} {search_variants[1]}")
    return queries

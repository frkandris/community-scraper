import asyncio
import re
import structlog
import httpx
from urllib.parse import parse_qs, urlparse, unquote

from .models import SearchResult

log = structlog.get_logger()


class SearchQuotaError(Exception):
    """Raised when the search API returns a rate-limit or payment-required error."""


LOCALE_TO_LANGUAGE = {
    "hu": "hu-HU",
    "en": "en-US",
    "de": "de-DE",
    "fr": "fr-FR",
    "es": "es-ES",
    "it": "it-IT",
    "pt": "pt-BR",
    "nl": "nl-NL",
    "pl": "pl-PL",
    "sv": "sv-SE",
    "da": "da-DK",
    "fi": "fi-FI",
    "no": "nb-NO",
    "cs": "cs-CZ",
    "ro": "ro-RO",
    "tr": "tr-TR",
    "ru": "ru-RU",
    "uk": "uk-UA",
    "zh": "zh-CN",
    "ja": "ja-JP",
    "ko": "ko-KR",
    "ar": "ar-SA",
    "id": "id-ID",
    "vi": "vi-VN",
}

LOCALE_TO_SERPER = {
    "hu": ("hu", "hu"),
    "en": ("us", "en"),
    "de": ("de", "de"),
    "fr": ("fr", "fr"),
    "es": ("es", "es"),
    "it": ("it", "it"),
    "pt": ("br", "pt"),
    "nl": ("nl", "nl"),
    "pl": ("pl", "pl"),
    "sv": ("se", "sv"),
    "da": ("dk", "da"),
    "fi": ("fi", "fi"),
    "no": ("no", "no"),
    "tr": ("tr", "tr"),
    "ja": ("jp", "ja"),
    "ko": ("kr", "ko"),
    "zh": ("cn", "zh"),
}

# DataForSEO location codes: https://api.dataforseo.com/v3/serp/google/locations
# (GET endpoint returns the full list; these are the most common ones)
LOCALE_TO_DATAFORSEO_LOCATION: dict[str, int] = {
    "hu": 2348,   # Hungary
    "de": 2276,   # Germany
    "fr": 2250,   # France
    "it": 2380,   # Italy
    "es": 2724,   # Spain
    "nl": 2528,   # Netherlands
    "pl": 2616,   # Poland
    "sv": 2752,   # Sweden
    "da": 2208,   # Denmark
    "fi": 2246,   # Finland
    "no": 2578,   # Norway
    "cs": 2203,   # Czech Republic
    "ro": 2642,   # Romania
    "tr": 2792,   # Turkey
    "ru": 2643,   # Russia
    "uk": 2804,   # Ukraine
    "pt": 2076,   # Brazil
    "en": 2840,   # United States (default for English)
}

# Brave Search only accepts a fixed list of country codes; unmapped locales fall back to US.
LOCALE_TO_BRAVE_COUNTRY = {
    "en": "US",
    "de": "DE",
    "fr": "FR",
    "es": "ES",
    "it": "IT",
    "pt": "BR",
    "nl": "NL",
    "sv": "SE",
    "da": "DK",
    "fi": "FI",
    "no": "NO",
    "pl": "PL",
    "tr": "TR",
    "ar": "SA",
    "zh": "CN",
    "ja": "JP",
    "ko": "KR",
    "ru": "RU",
}


class SerperSearchClient:
    """Serper.dev Google Search API — reliable from datacenter IPs."""

    _BASE = "https://google.serper.dev/search"

    def __init__(self, api_key: str, rate_limit_seconds: float = 1.0):
        self.api_key = api_key
        self.rate_limit_seconds = rate_limit_seconds
        self._last_request_time: float = 0.0

    async def search(
        self,
        query: str,
        locale: str = "en",
        num_results: int = 10,
    ) -> list[SearchResult]:
        await self._rate_limit()
        gl, hl = LOCALE_TO_SERPER.get(locale, ("us", "en"))
        payload = {"q": query, "num": min(num_results, 10), "gl": gl, "hl": hl}
        headers = {"X-API-KEY": self.api_key, "Content-Type": "application/json"}
        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                resp = await client.post(self._BASE, json=payload, headers=headers)
                if resp.status_code in (402, 429):
                    raise SearchQuotaError(f"Serper HTTP {resp.status_code}")
                if resp.status_code == 400:
                    body = resp.text
                    if "credit" in body.lower() or "quota" in body.lower():
                        raise SearchQuotaError(f"Serper credits exhausted: {body[:100]}")
                    log.warning("serper_search_failed", query=query, status=400, body=body[:300])
                    return []
                if resp.status_code >= 400:
                    log.warning("serper_search_failed", query=query,
                                status=resp.status_code, body=resp.text[:300])
                    return []
                data = resp.json()
        except SearchQuotaError:
            raise
        except Exception as exc:
            log.warning("serper_search_failed", query=query, error=str(exc))
            return []

        items = data.get("organic", [])
        log.debug("serper_results", query=query, raw=len(items))
        return [
            SearchResult(
                url=item.get("link", ""),
                title=item.get("title") or "",
                snippet=item.get("snippet") or "",
            )
            for item in items[:num_results]
            if item.get("link")
        ]

    async def search_all(
        self,
        queries: list[str],
        locale: str = "en",
        num_results: int = 10,
    ) -> list[SearchResult]:
        seen_urls: set[str] = set()
        combined: list[SearchResult] = []
        for query in queries:
            for r in await self.search(query, locale=locale, num_results=num_results):
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


class DataForSEOClient:
    """DataForSEO Google Organic SERP — live mode, $2/1K queries.
    Works reliably from datacenter IPs; raises SearchQuotaError on depleted credits.
    Auth: Basic HTTP with login:password (base64-encoded).
    """

    _BASE = "https://api.dataforseo.com/v3/serp/google/organic/live/regular"

    def __init__(self, login: str, password: str, rate_limit_seconds: float = 1.0):
        import base64
        self.rate_limit_seconds = rate_limit_seconds
        self._last_request_time: float = 0.0
        raw = f"{login}:{password}".encode()
        self._auth_header = f"Basic {base64.b64encode(raw).decode()}"

    async def search(
        self,
        query: str,
        locale: str = "en",
        num_results: int = 10,
    ) -> list[SearchResult]:
        await self._rate_limit()
        locale = str(locale)  # guard against PyYAML parsing "no" as bool False
        # DataForSEO uses bare ISO 639-1 language codes
        lang = locale.split("-")[0] if "-" in locale else locale
        task: dict = {"keyword": query, "language_code": lang, "depth": min(num_results, 100)}
        location_code = LOCALE_TO_DATAFORSEO_LOCATION.get(lang)
        if location_code:
            task["location_code"] = location_code
        payload = [task]
        headers = {"Authorization": self._auth_header, "Content-Type": "application/json"}
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(self._BASE, json=payload, headers=headers)
        except Exception as exc:
            log.warning("dataforseo_request_failed", query=query, error=str(exc))
            return []

        if resp.status_code == 402:
            raise SearchQuotaError("DataForSEO: insufficient credits (HTTP 402)")
        if resp.status_code == 429:
            raise SearchQuotaError("DataForSEO: rate limited (HTTP 429)")
        if resp.status_code >= 400:
            log.warning("dataforseo_http_error", query=query, status=resp.status_code)
            return []

        try:
            data = resp.json()
        except Exception:
            log.warning("dataforseo_bad_json", query=query)
            return []

        top = data.get("status_code", 0)
        if top == 40201:
            raise SearchQuotaError("DataForSEO: insufficient credits (40201)")
        if top not in (20000, 20100):
            log.warning("dataforseo_api_error", query=query, status_code=top,
                        message=data.get("status_message", ""))
            return []

        results: list[SearchResult] = []
        for task in data.get("tasks", []):
            task_status = task.get("status_code", 0)
            if task_status == 40201:
                raise SearchQuotaError("DataForSEO: task quota exhausted (40201)")
            for result in task.get("result") or []:
                for item in result.get("items") or []:
                    if item.get("type") != "organic":
                        continue
                    url = item.get("url", "")
                    if not url:
                        continue
                    results.append(SearchResult(
                        url=url,
                        title=item.get("title") or "",
                        snippet=item.get("description") or "",
                    ))
        log.info("dataforseo_results", query=query, found=len(results))
        return results[:num_results]

    async def search_all(
        self,
        queries: list[str],
        locale: str = "en",
        num_results: int = 10,
    ) -> list[SearchResult]:
        seen_urls: set[str] = set()
        combined: list[SearchResult] = []
        for query in queries:
            for r in await self.search(query, locale=locale, num_results=num_results):
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


class FallbackSearchClient:
    """Tries primaries left-to-right (DataForSEO → Serper)."""

    def __init__(self, primaries: list):
        self.primaries = primaries
        self._exhausted = [False] * len(primaries)

    async def search(self, query: str, locale: str = "en",
                     num_results: int = 10) -> list[SearchResult]:
        for i, primary in enumerate(self.primaries):
            if self._exhausted[i]:
                continue
            try:
                return await primary.search(query, locale=locale, num_results=num_results)
            except SearchQuotaError as exc:
                log.warning("search_quota_exhausted", provider=type(primary).__name__, reason=str(exc))
                self._exhausted[i] = True
        return []

    async def search_all(self, queries: list[str], locale: str = "en",
                         num_results: int = 10) -> list[SearchResult]:
        for i, primary in enumerate(self.primaries):
            if self._exhausted[i]:
                continue
            try:
                results = await primary.search_all(queries, locale=locale, num_results=num_results)
                if results:
                    return results
                log.info("search_empty_try_next", provider=type(primary).__name__, queries=queries)
            except SearchQuotaError as exc:
                log.warning("search_quota_exhausted", provider=type(primary).__name__, reason=str(exc))
                self._exhausted[i] = True
        return []


def build_queries(
    city_name: str,
    search_variants: list[str],
    topic_terms: list[str],
) -> list[str]:
    if not city_name or not topic_terms:
        return []

    queries = []
    variants = search_variants or [city_name]
    primary_variant = variants[0]
    for term in topic_terms[:2]:
        queries.append(f"{term} {primary_variant}")
    if len(variants) > 1:
        queries.append(f"{topic_terms[0]} {variants[1]}")
    return queries

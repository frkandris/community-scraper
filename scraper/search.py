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


class BraveSearchClient:
    """Brave Search API — works from datacenter IPs, no CAPTCHA."""

    _BASE = "https://api.search.brave.com/res/v1/web/search"

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
        country = LOCALE_TO_BRAVE_COUNTRY.get(locale, "US")
        params = {
            "q": query,
            "count": min(num_results, 20),
            "country": country,
            "search_lang": locale if len(locale) == 2 else "en",
        }
        headers = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "X-Subscription-Token": self.api_key,
        }
        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                resp = await client.get(self._BASE, params=params, headers=headers)
                if resp.status_code in (402, 429):
                    raise SearchQuotaError(f"Brave HTTP {resp.status_code}")
                if resp.status_code >= 400:
                    log.warning("brave_search_failed", query=query,
                                status=resp.status_code, body=resp.text[:300])
                    return []
                data = resp.json()
        except SearchQuotaError:
            raise
        except Exception as exc:
            log.warning("brave_search_failed", query=query, error=str(exc))
            return []

        items = data.get("web", {}).get("results", [])
        log.debug("brave_results", query=query, raw=len(items))
        return [
            SearchResult(
                url=item.get("url", ""),
                title=item.get("title", ""),
                snippet=item.get("description", ""),
            )
            for item in items[:num_results]
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
            log.warning("searxng_empty_results", query=query, language=language,
                        unresponsive=data.get("unresponsive_engines", []))
        else:
            log.debug("searxng_results", query=query, raw=raw_count)

        return [
            SearchResult(
                url=item.get("url", ""),
                title=item.get("title", ""),
                snippet=item.get("content", ""),
            )
            for item in data.get("results", [])[:num_results]
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


class DuckDuckGoClient:
    """DuckDuckGo HTML scraping — free, no API key, works from datacenter IPs.

    Uses html.duckduckgo.com/html/ which is a plain HTML endpoint without
    JS challenges or CAPTCHA. Raises SearchQuotaError after repeated blocks
    so FallbackSearchClient can move on to SearXNG.
    """

    _BASE = "https://html.duckduckgo.com/html/"

    # DDG kl= locale codes
    _LOCALE_KL: dict[str, str] = {
        "hu": "hu-hu", "en": "en-us", "de": "de-de", "fr": "fr-fr",
        "es": "es-es", "it": "it-it", "pt": "pt-br", "nl": "nl-nl",
        "pl": "pl-pl", "sv": "sv-se", "da": "da-dk", "fi": "fi-fi",
        "no": "no-no", "cs": "cs-cz", "ro": "ro-ro", "tr": "tr-tr",
        "ru": "ru-ru", "uk": "uk-ua", "zh": "zh-cn", "ja": "ja-jp",
        "ko": "ko-kr", "ar": "ar-xa", "id": "id-id", "vi": "vi-vn",
    }

    # Regex against DDG HTML result blocks
    _TITLE_RE = re.compile(
        r'class="result__a"[^>]+href="([^"]*)"[^>]*>(.*?)</a>',
        re.DOTALL | re.IGNORECASE,
    )
    _SNIPPET_RE = re.compile(
        r'class="result__snippet"[^>]*>(.*?)</(?:a|div|span)>',
        re.DOTALL | re.IGNORECASE,
    )

    def __init__(self, rate_limit_seconds: float = 2.0):
        self.rate_limit_seconds = rate_limit_seconds
        self._last_request_time: float = 0.0
        self._consecutive_empty: int = 0
        self._MAX_EMPTY = 20  # treat as blocked after this many consecutive empty runs

    async def search(
        self,
        query: str,
        locale: str = "en",
        num_results: int = 10,
    ) -> list[SearchResult]:
        if self._consecutive_empty >= self._MAX_EMPTY:
            raise SearchQuotaError("DuckDuckGo: too many empty responses, assuming blocked")

        await self._rate_limit()
        kl = self._LOCALE_KL.get(locale, "en-us")
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            ),
            "Accept-Language": f"{locale},{locale}-*;q=0.9,en;q=0.5",
            "Accept": "text/html,application/xhtml+xml",
        }
        try:
            async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
                resp = await client.post(
                    self._BASE,
                    data={"q": query, "kl": kl, "b": "", "df": ""},
                    headers=headers,
                )
                if resp.status_code >= 400:
                    self._consecutive_empty += 1
                    log.warning("ddg_search_failed", query=query, status=resp.status_code)
                    return []
                results = self._parse(resp.text, num_results)
        except SearchQuotaError:
            raise
        except Exception as exc:
            self._consecutive_empty += 1
            log.warning("ddg_search_failed", query=query, error=str(exc))
            return []

        if results:
            self._consecutive_empty = 0
            log.debug("ddg_results", query=query, found=len(results))
        else:
            self._consecutive_empty += 1
            log.debug("ddg_empty", query=query, consecutive=self._consecutive_empty)
        return results

    def _parse(self, html: str, limit: int) -> list[SearchResult]:
        title_matches = self._TITLE_RE.findall(html)
        snippet_matches = self._SNIPPET_RE.findall(html)
        results: list[SearchResult] = []
        for i, (href, raw_title) in enumerate(title_matches):
            # DDG wraps URLs as https://duckduckgo.com/l/?uddg=ENCODED_URL
            try:
                qs = parse_qs(urlparse(href).query)
                url = unquote(qs.get("uddg", [""])[0]) or href
            except Exception:
                url = href
            if not url.startswith("http"):
                continue
            title = re.sub(r"<[^>]+>", "", raw_title).strip()
            snippet = ""
            if i < len(snippet_matches):
                snippet = re.sub(r"<[^>]+>", "", snippet_matches[i]).strip()
            results.append(SearchResult(url=url, title=title, snippet=snippet))
            if len(results) >= limit:
                break
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
                title=item.get("title", ""),
                snippet=item.get("snippet", ""),
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


class FallbackSearchClient:
    """Tries primaries left-to-right; on quota exhaustion falls back to SearXNG."""

    def __init__(self, primaries: list, fallback: SearXNGClient):
        self.primaries = primaries
        self.fallback = fallback
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
        return await self.fallback.search(query, locale=locale, num_results=num_results)

    async def search_all(self, queries: list[str], locale: str = "en",
                         num_results: int = 10) -> list[SearchResult]:
        for i, primary in enumerate(self.primaries):
            if self._exhausted[i]:
                continue
            try:
                return await primary.search_all(queries, locale=locale, num_results=num_results)
            except SearchQuotaError as exc:
                log.warning("search_quota_exhausted", provider=type(primary).__name__, reason=str(exc))
                self._exhausted[i] = True
        return await self.fallback.search_all(queries, locale=locale, num_results=num_results)


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

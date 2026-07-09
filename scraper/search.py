import asyncio
import structlog
import httpx

from .models import SearchResult

log = structlog.get_logger()


class SearchQuotaError(Exception):
    """Raised when the search API returns a rate-limit or payment-required error."""


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

class DataForSEOClient:
    """DataForSEO Google Organic SERP.

    Two modes:
    - "live" (default): live/regular endpoint, $2/1K queries, instant.
    - "standard": task_post + task_get polling, $0.6/1K queries (70% cheaper),
      but results take ~0.5–5 minutes. Fine for the batch pipeline; keep "live"
      for anything latency-sensitive (e.g. enrichment).

    Works reliably from datacenter IPs; raises SearchQuotaError on depleted credits.
    Auth: Basic HTTP with login:password (base64-encoded).
    """

    _BASE = "https://api.dataforseo.com/v3/serp/google/organic/live/regular"
    _TASK_POST = "https://api.dataforseo.com/v3/serp/google/organic/task_post"
    _TASK_GET = "https://api.dataforseo.com/v3/serp/google/organic/task_get/regular"
    _STANDARD_POLL_SECONDS = 10.0
    _STANDARD_TIMEOUT_SECONDS = 300.0

    def __init__(self, login: str, password: str, rate_limit_seconds: float = 1.0,
                 mode: str = "live"):
        import base64
        self.rate_limit_seconds = rate_limit_seconds
        self.mode = mode if mode in ("live", "standard") else "live"
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
        if self.mode == "standard":
            return await self._search_standard(task, query, num_results)
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

        results = self._parse_tasks(data)
        log.info("dataforseo_results", query=query, found=len(results))
        return results[:num_results]

    @staticmethod
    def _parse_tasks(data: dict) -> list[SearchResult]:
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
        return results

    async def _search_standard(self, task: dict, query: str,
                               num_results: int) -> list[SearchResult]:
        """Queue-mode search: task_post ($0.6/1K) then poll task_get until done."""
        headers = {"Authorization": self._auth_header, "Content-Type": "application/json"}
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(self._TASK_POST, json=[{**task, "priority": 1}],
                                         headers=headers)
        except Exception as exc:
            log.warning("dataforseo_task_post_failed", query=query, error=str(exc))
            return []
        if resp.status_code in (402, 429):
            raise SearchQuotaError(f"DataForSEO: HTTP {resp.status_code}")
        try:
            data = resp.json()
        except Exception:
            return []
        if data.get("status_code") == 40201:
            raise SearchQuotaError("DataForSEO: insufficient credits (40201)")
        tasks = data.get("tasks") or []
        task_id = tasks[0].get("id") if tasks else None
        if tasks and tasks[0].get("status_code") == 40201:
            raise SearchQuotaError("DataForSEO: task quota exhausted (40201)")
        if not task_id:
            log.warning("dataforseo_task_post_no_id", query=query,
                        status=data.get("status_code"))
            return []

        import time
        deadline = time.monotonic() + self._STANDARD_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            await asyncio.sleep(self._STANDARD_POLL_SECONDS)
            try:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    poll = await client.get(f"{self._TASK_GET}/{task_id}", headers=headers)
                pdata = poll.json()
            except Exception as exc:
                log.warning("dataforseo_task_get_failed", query=query, error=str(exc))
                continue
            ptasks = pdata.get("tasks") or []
            status = ptasks[0].get("status_code", 0) if ptasks else 0
            if status == 20000:
                results = self._parse_tasks(pdata)
                log.info("dataforseo_results", query=query, found=len(results), mode="standard")
                return results[:num_results]
            if status == 40201:
                raise SearchQuotaError("DataForSEO: task quota exhausted (40201)")
            # 40601/40602 = task queued / in progress — keep polling
        log.warning("dataforseo_task_timeout", query=query, task_id=task_id)
        return []

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
    """Tries primaries left-to-right. Providers are marked permanently exhausted
    on quota error — retrying won't help if credits are gone. (Currently the only
    provider is DataForSEO; the wrapper stays for the per-run exhaustion state and
    so a second provider can be added back with one line.)
    """

    def __init__(self, primaries: list):
        self.primaries = primaries
        self._blocked_until: list[float] = [0.0] * len(primaries)  # inf = exhausted

    def _is_provider_blocked(self, i: int) -> bool:
        import time
        return time.monotonic() < self._blocked_until[i]

    @property
    def exhausted(self) -> bool:
        """True when no provider is configured or every provider hit a quota
        error this run. Callers must skip searching (and must NOT record an
        empty search_cache entry — the pair was not actually searched)."""
        return not self.primaries or all(b == float("inf") for b in self._blocked_until)

    def _block_provider(self, i: int, primary) -> None:
        self._blocked_until[i] = float("inf")
        log.warning("search_quota_exhausted", provider=type(primary).__name__)

    async def search(self, query: str, locale: str = "en",
                     num_results: int = 10) -> list[SearchResult]:
        for i, primary in enumerate(self.primaries):
            if self._is_provider_blocked(i):
                continue
            try:
                return await primary.search(query, locale=locale, num_results=num_results)
            except SearchQuotaError as exc:
                log.warning("search_quota_error", provider=type(primary).__name__, reason=str(exc))
                self._block_provider(i, primary)
        return []

    async def search_all(self, queries: list[str], locale: str = "en",
                         num_results: int = 10,
                         stop_after: int | None = None) -> list[SearchResult]:
        """Run queries left-to-right across providers, deduplicating by URL.

        stop_after: stop issuing further (paid) queries once this many unique
        results have been collected. The pipeline caps fetched pages anyway, so
        extra queries past that point only cost money. None = run all queries.

        Failover semantics: quota error blocks the provider and the *remaining*
        queries move to the next one (already-collected results are kept). If a
        provider ran all remaining queries and the combined total is still empty,
        the next provider retries the full set.
        """
        if self.exhausted:
            raise SearchQuotaError("no search provider available")

        combined: list[SearchResult] = []
        seen_urls: set[str] = set()
        remaining = list(queries)
        hit_quota = False

        for i, primary in enumerate(self.primaries):
            if self._is_provider_blocked(i) or not remaining:
                continue
            provider_done: list[str] = []
            try:
                for query in remaining:
                    if stop_after is not None and len(combined) >= stop_after:
                        log.info("search_stop_after_reached", collected=len(combined),
                                 skipped_queries=len(remaining) - len(provider_done))
                        return combined
                    for r in await primary.search(query, locale=locale, num_results=num_results):
                        if r.url not in seen_urls:
                            seen_urls.add(r.url)
                            combined.append(r)
                    provider_done.append(query)
            except SearchQuotaError as exc:
                log.warning("search_quota_error", provider=type(primary).__name__, reason=str(exc))
                self._block_provider(i, primary)
                hit_quota = True
                remaining = [q for q in remaining if q not in provider_done]
                continue
            if combined:
                return combined
            log.info("search_empty_try_next", provider=type(primary).__name__, queries=queries)
            # total empty → let the next provider retry the full set
            remaining = list(queries)
        if not combined and hit_quota:
            # Nothing was successfully searched — this is a provider failure,
            # not a legitimate empty result. Callers must not cache it.
            raise SearchQuotaError("search providers exhausted before any result")
        return combined


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

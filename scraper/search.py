import asyncio
import re
import structlog
import httpx
from urllib.parse import parse_qs, urlparse, unquote, quote_plus

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


class GooglePlaywrightSearchClient:
    """Headless Chromium Google search — free, no API key, 8 s between requests.
    Raises SearchQuotaError on CAPTCHA so FallbackSearchClient rolls to DataForSEO.
    Requires playwright + chromium (already installed via Dockerfile).
    """

    _CAPTCHA_MARKERS = ("/sorry/", "recaptcha", "g-recaptcha", "unusual traffic")

    _DEFAULT_UA = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )

    def __init__(self, rate_limit_seconds: float = 8.0, headless: bool = True,
                 user_data_dir: str | None = None, context_locale: str = "en-US",
                 user_agent: str | None = None):
        self.rate_limit_seconds = rate_limit_seconds
        self.headless = headless
        # A persistent user-data-dir keeps consent cookies and browsing history
        # across runs so the profile looks human — the main defense against
        # Google's automation CAPTCHA. None → ephemeral context (old behavior).
        self.user_data_dir = user_data_dir
        self.context_locale = context_locale
        self.user_agent = user_agent or self._DEFAULT_UA
        self._pw = None
        self._browser = None
        self._context = None
        self._last_request_time: float = 0.0
        self._consent_done = False

    def _stealth_script(self) -> str:
        primary = self.context_locale.split("-")[0]
        langs = [self.context_locale, primary] if primary != self.context_locale else [self.context_locale]
        import json as _json
        return (
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
            f"Object.defineProperty(navigator,'languages',{{get:()=>{_json.dumps(langs)}}});"
            "Object.defineProperty(navigator,'plugins',{get:()=>[1,2,3,4,5]});"
            "window.chrome=window.chrome||{runtime:{}};"
        )

    async def start(self) -> None:
        try:
            from playwright.async_api import async_playwright
            self._pw = await async_playwright().start()
            # --disable-blink-features=AutomationControlled removes the
            # navigator.webdriver=true flag Google keys on; the init script masks
            # the remaining automation tells.
            args = [
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ]
            ctx_opts = {
                "user_agent": self.user_agent,
                "locale": self.context_locale,
                "viewport": {"width": 1280, "height": 800},
            }
            if self.user_data_dir:
                self._context = await self._pw.chromium.launch_persistent_context(
                    self.user_data_dir, headless=self.headless, args=args, **ctx_opts,
                )
                self._browser = None  # persistent context owns the browser
            else:
                self._browser = await self._pw.chromium.launch(headless=self.headless, args=args)
                self._context = await self._browser.new_context(**ctx_opts)
            await self._context.add_init_script(self._stealth_script())
            log.info("google_playwright_search_started", persistent=bool(self.user_data_dir))
        except Exception as exc:
            log.warning("google_playwright_search_start_failed", error=str(exc))

    async def stop(self) -> None:
        try:
            if self._context:
                await self._context.close()
            if self._browser:
                await self._browser.close()
            if self._pw:
                await self._pw.stop()
        except Exception as exc:
            log.debug("google_playwright_search_stop_error", error=str(exc))

    async def search(
        self,
        query: str,
        locale: str = "en",
        num_results: int = 10,
    ) -> list[SearchResult]:
        if not self._context:
            return []
        await self._rate_limit()

        hl = locale.split("-")[0] if "-" in locale else locale
        if hl == "no":
            hl = "nb"
        gl = LOCALE_TO_SERPER.get(hl, ("us", "en"))[0]
        # NB: no &num= param — Google actively CAPTCHA-blocks the num parameter as a
        # scraping tell (since ~2025). The default page returns ~10 results anyway;
        # callers cap with results[:num_results].
        url = (
            f"https://www.google.com/search"
            f"?q={quote_plus(query)}"
            f"&hl={hl}&gl={gl}"
        )

        page = None
        try:
            page = await self._context.new_page()
            await page.goto(url, timeout=20_000, wait_until="domcontentloaded")
            await asyncio.sleep(1.0)

            if not self._consent_done:
                await self._accept_consent(page)

            content = await page.content()
            if any(m in content.lower() for m in self._CAPTCHA_MARKERS):
                log.warning("google_playwright_captcha", query=query)
                raise SearchQuotaError("Google blocked: CAPTCHA detected")

            results = await self._parse_results(page, num_results)
            log.info("google_playwright_results", query=query, found=len(results))
            return results
        except SearchQuotaError:
            raise
        except Exception as exc:
            log.warning("google_playwright_search_failed", query=query, error=str(exc))
            return []
        finally:
            if page:
                try:
                    await page.close()
                except Exception:
                    pass

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

    async def _accept_consent(self, page) -> None:
        try:
            btn = page.locator(
                'button:has-text("Accept all"), '
                'button[id="L2AGLb"], '
                'button:has-text("Alle akzeptieren"), '
                'button:has-text("Tout accepter"), '
                'button:has-text("Elfogad")'
            )
            if await btn.first.is_visible(timeout=2000):
                await btn.first.click()
                await page.wait_for_load_state("domcontentloaded")
                await asyncio.sleep(0.5)
                self._consent_done = True
                log.info("google_playwright_consent_accepted")
        except Exception:
            pass

    async def _parse_results(self, page, num_results: int) -> list[SearchResult]:
        results: list[SearchResult] = []
        links = await page.locator("#search a:has(h3)").all()
        for link in links:
            href = await link.get_attribute("href")
            if not href or not href.startswith("http") or "google.com" in href:
                continue
            try:
                title = await link.locator("h3").first.inner_text(timeout=500)
            except Exception:
                continue
            if not title:
                continue
            snippet = ""
            try:
                snippet_el = (
                    link.locator("..")
                    .locator('[data-sncf], [style*="line-clamp"], .VwiC3b')
                    .first
                )
                snippet = await snippet_el.inner_text(timeout=500)
            except Exception:
                pass
            results.append(SearchResult(url=href, title=title, snippet=snippet))
            if len(results) >= num_results:
                break
        return results

    async def _rate_limit(self) -> None:
        import time
        now = time.monotonic()
        elapsed = now - self._last_request_time
        if elapsed < self.rate_limit_seconds:
            await asyncio.sleep(self.rate_limit_seconds - elapsed)
        self._last_request_time = time.monotonic()


class FallbackSearchClient:
    """Tries primaries left-to-right (Google Playwright → DataForSEO → Serper).

    GooglePlaywrightSearchClient gets a 15-minute cooldown on CAPTCHA so a single
    block early in the run doesn't kill Playwright for the remaining hundreds of
    city/topic pairs. API-based providers (DataForSEO, Serper) are marked
    permanently exhausted on quota error — retrying won't help if credits are gone.
    """

    PLAYWRIGHT_COOLDOWN_SECONDS = 15 * 60

    def __init__(self, primaries: list):
        self.primaries = primaries
        # 0.0 = not blocked; float('inf') = permanent; future timestamp = cooldown
        self._blocked_until: list[float] = [0.0] * len(primaries)

    def _is_provider_blocked(self, i: int) -> bool:
        import time
        return time.monotonic() < self._blocked_until[i]

    def _block_provider(self, i: int, primary) -> None:
        import time
        if isinstance(primary, GooglePlaywrightSearchClient):
            until = time.monotonic() + self.PLAYWRIGHT_COOLDOWN_SECONDS
            self._blocked_until[i] = until
            log.warning("search_quota_cooldown", provider=type(primary).__name__,
                        retry_in_minutes=self.PLAYWRIGHT_COOLDOWN_SECONDS // 60)
        else:
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
                         num_results: int = 10) -> list[SearchResult]:
        for i, primary in enumerate(self.primaries):
            if self._is_provider_blocked(i):
                continue
            try:
                results = await primary.search_all(queries, locale=locale, num_results=num_results)
                if results:
                    return results
                log.info("search_empty_try_next", provider=type(primary).__name__, queries=queries)
            except SearchQuotaError as exc:
                log.warning("search_quota_error", provider=type(primary).__name__, reason=str(exc))
                self._block_provider(i, primary)
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

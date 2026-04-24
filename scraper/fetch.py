import asyncio
from urllib.parse import urlparse

import html2text
import httpx
import structlog
import trafilatura

log = structlog.get_logger()

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


def _is_blocked(url: str, blocked_domains: list[str]) -> bool:
    try:
        host = urlparse(url).netloc.lower()
        return any(domain in host for domain in blocked_domains)
    except Exception:
        return False


def _extract_text(html: str) -> str | None:
    text = trafilatura.extract(html, include_comments=False, include_tables=False)
    if text and len(text) >= 100:
        return text
    converter = html2text.HTML2Text()
    converter.ignore_links = True
    converter.ignore_images = True
    fallback = converter.handle(html).strip()
    return fallback if len(fallback) >= 100 else None


async def fetch_and_clean(
    url: str,
    blocked_domains: list[str],
    timeout_seconds: int = 15,
    min_text_length: int = 100,
    semaphore: asyncio.Semaphore | None = None,
) -> str | None:
    if _is_blocked(url, blocked_domains):
        log.debug("fetch_blocked", url=url)
        return None

    async def _fetch() -> str | None:
        try:
            async with httpx.AsyncClient(
                timeout=timeout_seconds,
                follow_redirects=True,
                headers={"User-Agent": USER_AGENT},
            ) as client:
                resp = await client.get(url)
                resp.raise_for_status()
                if "text/html" not in resp.headers.get("content-type", ""):
                    return None
                return _extract_text(resp.text)
        except Exception as exc:
            log.debug("fetch_failed", url=url, error=str(exc))
            return None

    if semaphore:
        async with semaphore:
            return await _fetch()
    return await _fetch()


async def fetch_many(
    urls: list[str],
    blocked_domains: list[str],
    max_pages: int = 5,
    timeout_seconds: int = 15,
    min_text_length: int = 100,
    max_concurrent: int = 3,
) -> list[tuple[str, str]]:
    semaphore = asyncio.Semaphore(max_concurrent)
    tasks = [
        fetch_and_clean(url, blocked_domains, timeout_seconds, min_text_length, semaphore)
        for url in urls[:max_pages]
    ]
    results = await asyncio.gather(*tasks)
    return [(url, text) for url, text in zip(urls[:max_pages], results) if text]

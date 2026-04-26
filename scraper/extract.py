import asyncio
import json
import time
from datetime import datetime, timezone

import httpx
import structlog

from .models import CommunityRecord

log = structlog.get_logger()


class ExtractorQuotaError(Exception):
    """Raised when the LLM API returns a rate-limit or payment-required error."""


SYSTEM_PROMPT = """\
You are a data extraction assistant. Identify community groups and clubs from web page text.

Extract ONLY genuine ongoing community groups, clubs, or associations — NOT individual events, \
news articles, or commercial businesses.

The page may be in any language. Always output field values in the original language of the page.

For 'confidence': 0.9 if the group clearly matches the topic and city, 0.5 if somewhat related \
but uncertain, 0.1 if it barely qualifies.

For 'joinable': set true only if ALL of these apply:
  - the group meets or organizes activities on a regular, recurring basis
  - it is open to new members from the general public (not invite-only or audition-only)
  - it has a group identity (not just a venue, gym, or place you can visit)
Set joinable to false for: professional/competitive ensembles, paid instruction courses where you \
are a student not a member, venues or sports facilities, one-time or annual events.

Extract these additional fields when clearly stated on the page (leave null/empty if not found):
- 'founding_year': integer year founded (e.g. 1987). Only if explicitly stated.
- 'member_count': member count as string (e.g. "80", "200+", "~50 fő"). Only if stated.
- 'fee': cost in original currency (e.g. "Ingyenes", "3000 Ft/év", "€10/hó"). Use the page's \
  language. Set to "Ingyenes"/"Free"/etc. only if page explicitly says it is free.
- 'age_range': age requirements if stated (e.g. "18+", "Mindenki", "55+").
- 'skill_level': skill/experience level (e.g. "Minden szint", "Kezdőknek is", "Haladó szint").
- 'join_process': how to join (e.g. "Nyílt csatlakozás", "Email szükséges", "Meghallgatón").
- 'leader': name and/or role of the organizer/leader (e.g. "Kovács János, karmester").
- 'email': primary contact email address (must contain @).
- 'phone': primary phone number.
- 'tags': 1–5 specific subtopic keywords in the page language \
  (e.g. for running: ["trail", "maraton", "terepfutás"]).
- 'language': primary language(s) of the group (e.g. "Magyar", "English", "Deutsch/Magyar").

If nothing on the page is a real community group, return an empty communities array.
"""

USER_PROMPT_TEMPLATE = """\
Extract all {topic} community groups located in or near {city} from the following web page text.
The page was found at: {source_url}

--- PAGE TEXT START ---
{page_text}
--- PAGE TEXT END ---
"""

EXTRACTION_SCHEMA = {
    "type": "object",
    "properties": {
        "communities": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name":             {"type": "string"},
                    "description":      {"type": "string"},
                    "meeting_schedule": {"type": "string"},
                    "location":         {"type": "string"},
                    "contact":          {"type": "string"},
                    "website":          {"type": "string"},
                    "social_links":     {"type": "array", "items": {"type": "string"}},
                    "confidence":       {"type": "number"},
                    "joinable":         {"type": "boolean"},
                    "founding_year":    {"type": "integer"},
                    "member_count":     {"type": "string"},
                    "fee":              {"type": "string"},
                    "age_range":        {"type": "string"},
                    "skill_level":      {"type": "string"},
                    "join_process":     {"type": "string"},
                    "leader":           {"type": "string"},
                    "email":            {"type": "string"},
                    "phone":            {"type": "string"},
                    "tags":             {"type": "array", "items": {"type": "string"}},
                    "language":         {"type": "string"},
                },
                "required": ["name", "confidence", "joinable"],
            },
        }
    },
    "required": ["communities"],
}

ENRICH_SYSTEM_PROMPT = """\
Extract contact information for a specific named community group from a web page.
Return only fields where the page has clear evidence. Leave others as empty string or empty array.
"""

ENRICH_SCHEMA = {
    "type": "object",
    "properties": {
        "website":      {"type": "string"},
        "contact":      {"type": "string"},
        "social_links": {"type": "array", "items": {"type": "string"}},
        "email":        {"type": "string"},
        "phone":        {"type": "string"},
    },
    "required": ["website", "contact", "social_links", "email", "phone"],
}


def _apply_enrich(record: "CommunityRecord", enrichment: dict) -> "CommunityRecord":
    updates: dict = {}
    if not record.website and enrichment.get("website"):
        updates["website"] = enrichment["website"]
    if not record.contact and enrichment.get("contact"):
        updates["contact"] = enrichment["contact"]
    if not record.social_links and enrichment.get("social_links"):
        updates["social_links"] = enrichment["social_links"]
    if not record.email and enrichment.get("email"):
        updates["email"] = enrichment["email"]
    if not record.phone and enrichment.get("phone"):
        updates["phone"] = enrichment["phone"]
    if updates:
        log.debug("enrich_merged", community=record.name, fields=list(updates))
        return record.model_copy(update=updates)
    return record


def _parse_communities(
    raw: str,
    city: str,
    topic: str,
    locale: str,
    source_url: str,
) -> list[CommunityRecord]:
    try:
        items = json.loads(raw).get("communities", [])
        if not isinstance(items, list):
            return []
    except json.JSONDecodeError as exc:
        log.warning("llm_json_parse_failed", source_url=source_url,
                    error=str(exc), raw=raw[:200])
        return []

    records = []
    extracted_at = datetime.now(timezone.utc).isoformat()
    for item in items:
        if not isinstance(item, dict) or not item.get("name"):
            continue
        try:
            record = CommunityRecord(
                name=item["name"],
                topic=topic,
                city=city,
                locale=locale,
                description=item.get("description") or None,
                meeting_schedule=item.get("meeting_schedule") or None,
                location=item.get("location") or None,
                contact=item.get("contact") or None,
                website=item.get("website") or None,
                social_links=item.get("social_links") or [],
                source_url=source_url,
                extracted_at=extracted_at,
                confidence=item.get("confidence"),
                joinable=item.get("joinable", True),
                founding_year=item.get("founding_year") or None,
                member_count=item.get("member_count") or None,
                fee=item.get("fee") or None,
                age_range=item.get("age_range") or None,
                skill_level=item.get("skill_level") or None,
                join_process=item.get("join_process") or None,
                leader=item.get("leader") or None,
                email=item.get("email") or None,
                phone=item.get("phone") or None,
                tags=item.get("tags") or [],
                language=item.get("language") or None,
            )
            records.append(record)
        except Exception as exc:
            log.warning("record_validation_failed", item=item, error=str(exc))
    return records


class OllamaExtractor:
    def __init__(
        self,
        base_url: str,
        model: str = "qwen2.5:7b",
        temperature: float = 0.1,
        timeout_seconds: int = 180,
        max_text_chars: int = 6000,
    ):
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.temperature = temperature
        self.timeout_seconds = timeout_seconds
        self.max_text_chars = max_text_chars

    async def extract(
        self,
        text: str,
        city: str,
        topic: str,
        locale: str,
        source_url: str,
        false_positive_examples: str = "",
    ) -> list[CommunityRecord]:
        truncated = text[: self.max_text_chars]
        user_message = USER_PROMPT_TEMPLATE.format(
            topic=topic,
            city=city,
            source_url=source_url,
            page_text=truncated,
        )
        system = SYSTEM_PROMPT + false_positive_examples
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user_message},
            ],
            "stream": False,
            "format": EXTRACTION_SCHEMA,
            "options": {"temperature": self.temperature},
        }
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                resp = await client.post(f"{self.base_url}/api/chat", json=payload)
                resp.raise_for_status()
                data = resp.json()
        except Exception as exc:
            log.warning("ollama_request_failed", url=source_url, error=str(exc))
            return []

        raw = data.get("message", {}).get("content", "")
        return _parse_communities(raw, city, topic, locale, source_url)

    async def enrich(self, record: CommunityRecord, page_text: str,
                     false_positive_examples: str = "") -> CommunityRecord:
        """Try to fill in missing contact fields from an additional page."""
        user_message = (
            f"Community group: '{record.name}' in {record.city}\n\n"
            f"--- PAGE TEXT ---\n{page_text[:self.max_text_chars]}"
        )
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": ENRICH_SYSTEM_PROMPT + false_positive_examples},
                {"role": "user", "content": user_message},
            ],
            "stream": False,
            "format": ENRICH_SCHEMA,
            "options": {"temperature": 0.0},
        }
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                resp = await client.post(f"{self.base_url}/api/chat", json=payload)
                resp.raise_for_status()
                data = resp.json()
            raw = data.get("message", {}).get("content", "")
            enrichment = json.loads(raw)
            return _apply_enrich(record, enrichment)
        except Exception as exc:
            log.debug("enrich_failed", community=record.name, error=str(exc))
        return record


_GROQ_EXTRACT_SUFFIX = (
    "\n\nRespond ONLY with a valid JSON object: "
    "{\"communities\": [{\"name\": \"...\", \"confidence\": 0.9, \"joinable\": true, ...}]}"
)
_GROQ_ENRICH_SUFFIX = (
    "\n\nRespond ONLY with a valid JSON object with exactly these keys: "
    "website, contact, social_links, email, phone."
)

_GROQ_MAX_RETRIES = 3
_GROQ_RETRY_DEFAULT_WAIT = 60  # seconds if no Retry-After header


class GroqExtractor:
    _BASE_URL = "https://api.groq.com/openai/v1"

    def __init__(
        self,
        api_key: str,
        model: str = "llama-3.3-70b-versatile",
        temperature: float = 0.1,
        timeout_seconds: int = 60,
        max_text_chars: int = 4000,
        rate_limit_seconds: float = 4.0,
    ):
        self.api_key = api_key
        self.model = model
        self.temperature = temperature
        self.timeout_seconds = timeout_seconds
        self.max_text_chars = max_text_chars
        self.rate_limit_seconds = rate_limit_seconds
        self._last_request_time: float = 0.0

    def _headers(self) -> dict:
        return {"Authorization": f"Bearer {self.api_key}"}

    async def _rate_limit(self) -> None:
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < self.rate_limit_seconds:
            await asyncio.sleep(self.rate_limit_seconds - elapsed)
        self._last_request_time = time.monotonic()

    async def _post(self, payload: dict, label: str) -> dict:
        for attempt in range(_GROQ_MAX_RETRIES):
            await self._rate_limit()
            try:
                async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                    resp = await client.post(
                        f"{self._BASE_URL}/chat/completions",
                        json=payload,
                        headers=self._headers(),
                    )
            except Exception as exc:
                log.warning("groq_request_failed", label=label, error=str(exc))
                return {}

            if resp.status_code == 402:
                raise ExtractorQuotaError("Groq billing limit reached (HTTP 402)")

            if resp.status_code == 429:
                retry_after = float(resp.headers.get("retry-after", _GROQ_RETRY_DEFAULT_WAIT))
                if attempt < _GROQ_MAX_RETRIES - 1:
                    log.warning("groq_rate_limited_retrying",
                                label=label, attempt=attempt + 1,
                                wait_s=retry_after)
                    await asyncio.sleep(retry_after)
                    continue
                raise ExtractorQuotaError(
                    f"Groq rate limit persists after {_GROQ_MAX_RETRIES} retries"
                )

            if resp.status_code >= 400:
                log.warning("groq_request_failed", label=label,
                            status=resp.status_code, body=resp.text[:200])
                return {}

            return resp.json()

        return {}

    async def extract(
        self,
        text: str,
        city: str,
        topic: str,
        locale: str,
        source_url: str,
        false_positive_examples: str = "",
    ) -> list[CommunityRecord]:
        truncated = text[: self.max_text_chars]
        user_message = USER_PROMPT_TEMPLATE.format(
            topic=topic,
            city=city,
            source_url=source_url,
            page_text=truncated,
        )
        system = SYSTEM_PROMPT + false_positive_examples + _GROQ_EXTRACT_SUFFIX
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user_message},
            ],
            "temperature": self.temperature,
            "response_format": {"type": "json_object"},
        }
        data = await self._post(payload, label=source_url)
        raw = data.get("choices", [{}])[0].get("message", {}).get("content", "")
        return _parse_communities(raw, city, topic, locale, source_url)

    async def enrich(self, record: CommunityRecord, page_text: str,
                     false_positive_examples: str = "") -> CommunityRecord:
        user_message = (
            f"Community group: '{record.name}' in {record.city}\n\n"
            f"--- PAGE TEXT ---\n{page_text[:self.max_text_chars]}"
        )
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": ENRICH_SYSTEM_PROMPT + false_positive_examples + _GROQ_ENRICH_SUFFIX},
                {"role": "user", "content": user_message},
            ],
            "temperature": 0.0,
            "response_format": {"type": "json_object"},
        }
        try:
            data = await self._post(payload, label=record.name)
            raw = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            enrichment = json.loads(raw)
            return _apply_enrich(record, enrichment)
        except ExtractorQuotaError:
            raise
        except Exception as exc:
            log.debug("groq_enrich_failed", community=record.name, error=str(exc))
        return record


class FallbackExtractor:
    """Tries primary extractor; on quota exhaustion permanently switches to fallback."""

    def __init__(self, primary: GroqExtractor, fallback: OllamaExtractor):
        self.primary = primary
        self.fallback = fallback
        self._exhausted = False

    async def extract(self, text: str, city: str, topic: str, locale: str,
                      source_url: str, false_positive_examples: str = "") -> list[CommunityRecord]:
        if not self._exhausted:
            try:
                return await self.primary.extract(
                    text, city, topic, locale, source_url, false_positive_examples
                )
            except ExtractorQuotaError as exc:
                log.warning("groq_quota_exhausted_switching_to_ollama", reason=str(exc))
                self._exhausted = True
        return await self.fallback.extract(
            text, city, topic, locale, source_url, false_positive_examples
        )

    async def enrich(self, record: CommunityRecord, page_text: str,
                     false_positive_examples: str = "") -> CommunityRecord:
        if not self._exhausted:
            try:
                return await self.primary.enrich(record, page_text, false_positive_examples)
            except ExtractorQuotaError as exc:
                log.warning("groq_quota_exhausted_switching_to_ollama", reason=str(exc))
                self._exhausted = True
        return await self.fallback.enrich(record, page_text, false_positive_examples)

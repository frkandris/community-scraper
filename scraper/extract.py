import asyncio
import hashlib
import json
import time
from datetime import datetime, timezone

import httpx
import structlog

from .models import CommunityRecord, PersonRecord, VenueRecord

log = structlog.get_logger()


def _prompt_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:12]


class ExtractorQuotaError(Exception):
    """Raised when the LLM API quota is permanently exhausted (billing limit / daily cap)."""


class ExtractorRateLimitError(Exception):
    """Raised when the primary extractor is temporarily rate-limited."""
    def __init__(self, wait_seconds: float):
        self.wait_seconds = wait_seconds
        super().__init__(f"Rate limited for {wait_seconds:.0f}s")


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
- 'history': 1–3 sentence background story or history of the group if the page describes it \
  (e.g. "1998-ban alapították, azóta 200 tagot számlál"). Leave null if not mentioned.
- 'frequency': how often the group meets as a short phrase in the page language \
  (e.g. "Heti", "Kéthetente", "Havonta", "Hetente kétszer", "Weekly", "Monthly"). \
  Leave null if not mentioned or unclear.

If nothing on the page is a real community group, return an empty communities array.
"""

USER_PROMPT_TEMPLATE = """\
Extract all {topic} community groups located in or near {city} from the following web page text.
The page was found at: {source_url}

IMPORTANT: Only include communities that are actually based in {city} or its immediate surroundings. \
If the page is a national/regional directory listing clubs from many different cities, \
skip communities that are clearly located elsewhere. When in doubt, set confidence below 0.5.

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
                    "history":          {"type": "string"},
                    "frequency":        {"type": "string"},
                },
                "required": ["name", "confidence", "joinable"],
            },
        }
    },
    "required": ["communities"],
}

VENUE_SYSTEM_PROMPT = """\
You are a data extraction assistant. Extract physical venues from web page text.

A venue is a real physical location (café, bar, park, community center, library, \
church hall, sports hall, studio, etc.) that explicitly hosts or welcomes community groups.

Only extract venues if the page clearly mentions that community groups meet there \
or that the place welcomes groups. Do NOT extract generic addresses or event listings.

For 'venue_type' use one of: café | bar | park | cultural_center | library | \
church | sports_hall | studio | coworking | restaurant | other

For 'welcomed_topics': list only topics clearly mentioned \
(e.g. ["running", "yoga", "board_games"] — use the English slug form).

Output field values in the original language of the page.
If no venues found, return an empty venues array.
"""

VENUE_USER_PROMPT_TEMPLATE = """\
Extract physical venues that host community groups in {city} from the following page.
Source URL: {source_url}

--- PAGE TEXT ---
{page_text}
--- END ---
"""

VENUE_SCHEMA = {
    "type": "object",
    "properties": {
        "venues": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name":              {"type": "string"},
                    "address":           {"type": "string"},
                    "venue_type":        {"type": "string"},
                    "welcomed_topics":   {"type": "array", "items": {"type": "string"}},
                    "description":       {"type": "string"},
                    "website":           {"type": "string"},
                    "social_links":      {"type": "array", "items": {"type": "string"}},
                    "email":             {"type": "string"},
                    "phone":             {"type": "string"},
                },
                "required": ["name"],
            },
        }
    },
    "required": ["venues"],
}


PERSON_SYSTEM_PROMPT = """\
You are a data extraction assistant. Extract named people from web page text \
who are clearly associated with a community group as a leader, instructor, or speaker.

Roles:
- 'leader': founder, coordinator, organizer of the group
- 'instructor': coach, teacher, conductor, trainer who regularly leads sessions
- 'speaker': guest presenter or one-time speaker at a community event

Only extract people who are named (not anonymous). Do NOT invent names.
Output field values in the original language of the page.
If no relevant people are found, return an empty persons array.
"""

PERSON_USER_PROMPT_TEMPLATE = """\
Extract people (leaders, instructors, speakers) associated with {topic} community groups \
in {city} from the following page.
Source URL: {source_url}

Known communities on this page (for reference):
{community_names}

--- PAGE TEXT ---
{page_text}
--- END ---
"""

PERSON_SCHEMA = {
    "type": "object",
    "properties": {
        "persons": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name":           {"type": "string"},
                    "role":           {"type": "string"},   # leader | instructor | speaker
                    "community_name": {"type": "string"},
                    "bio":            {"type": "string"},
                    "email":          {"type": "string"},
                    "website":        {"type": "string"},
                    "social_links":   {"type": "array", "items": {"type": "string"}},
                },
                "required": ["name", "role", "community_name"],
            },
        }
    },
    "required": ["persons"],
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


def _parse_venues(raw: str, city: str, locale: str, source_url: str) -> list[VenueRecord]:
    try:
        items = json.loads(raw).get("venues", [])
        if not isinstance(items, list):
            return []
    except json.JSONDecodeError:
        return []
    records = []
    extracted_at = datetime.now(timezone.utc).isoformat()
    for item in items:
        if not isinstance(item, dict) or not item.get("name"):
            continue
        try:
            records.append(VenueRecord(
                name=item["name"],
                city=city,
                locale=locale,
                address=item.get("address") or None,
                venue_type=item.get("venue_type") or None,
                welcomed_topics=item.get("welcomed_topics") or [],
                description=item.get("description") or None,
                website=item.get("website") or None,
                social_links=item.get("social_links") or [],
                email=item.get("email") or None,
                phone=item.get("phone") or None,
                source_url=source_url,
                extracted_at=extracted_at,
            ))
        except Exception as exc:
            log.warning("venue_validation_failed", item=item, error=str(exc))
    return records


def _parse_persons(
    raw: str, city: str, topic: str, locale: str, source_url: str,
) -> list[PersonRecord]:
    try:
        items = json.loads(raw).get("persons", [])
        if not isinstance(items, list):
            return []
    except json.JSONDecodeError:
        return []
    records = []
    extracted_at = datetime.now(timezone.utc).isoformat()
    for item in items:
        if not isinstance(item, dict) or not item.get("name") or not item.get("community_name"):
            continue
        try:
            records.append(PersonRecord(
                name=item["name"],
                role=item.get("role") or "leader",
                city=city,
                topic=topic,
                community_name=item["community_name"],
                bio=item.get("bio") or None,
                email=item.get("email") or None,
                website=item.get("website") or None,
                social_links=item.get("social_links") or [],
                source_url=source_url,
                extracted_at=extracted_at,
            ))
        except Exception as exc:
            log.warning("person_validation_failed", item=item, error=str(exc))
    return records


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
                history=item.get("history") or None,
                frequency=item.get("frequency") or None,
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

    @property
    def model_fingerprint(self) -> str:
        return _prompt_hash(SYSTEM_PROMPT + self.model)

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

    async def extract_venues(
        self, text: str, city: str, locale: str, source_url: str,
    ) -> list[VenueRecord]:
        user_message = VENUE_USER_PROMPT_TEMPLATE.format(
            city=city, source_url=source_url, page_text=text[:self.max_text_chars],
        )
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": VENUE_SYSTEM_PROMPT},
                {"role": "user", "content": user_message},
            ],
            "stream": False,
            "format": VENUE_SCHEMA,
            "options": {"temperature": 0.0},
        }
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                resp = await client.post(f"{self.base_url}/api/chat", json=payload)
                resp.raise_for_status()
                data = resp.json()
            raw = data.get("message", {}).get("content", "")
            return _parse_venues(raw, city, locale, source_url)
        except Exception as exc:
            log.debug("extract_venues_failed", url=source_url, error=str(exc))
        return []

    async def extract_persons(
        self, text: str, city: str, topic: str, locale: str, source_url: str,
        community_names: list[str] | None = None,
    ) -> list[PersonRecord]:
        names_str = "\n".join(f"- {n}" for n in (community_names or [])) or "(none known)"
        user_message = PERSON_USER_PROMPT_TEMPLATE.format(
            city=city, topic=topic, source_url=source_url,
            community_names=names_str,
            page_text=text[:self.max_text_chars],
        )
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": PERSON_SYSTEM_PROMPT},
                {"role": "user", "content": user_message},
            ],
            "stream": False,
            "format": PERSON_SCHEMA,
            "options": {"temperature": 0.0},
        }
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                resp = await client.post(f"{self.base_url}/api/chat", json=payload)
                resp.raise_for_status()
                data = resp.json()
            raw = data.get("message", {}).get("content", "")
            return _parse_persons(raw, city, topic, locale, source_url)
        except Exception as exc:
            log.debug("extract_persons_failed", url=source_url, error=str(exc))
        return []

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


_API_EXTRACT_SUFFIX = (
    "\n\nRespond ONLY with a valid JSON object: "
    "{\"communities\": [{\"name\": \"...\", \"confidence\": 0.9, \"joinable\": true, ...}]}"
)
_API_ENRICH_SUFFIX = (
    "\n\nRespond ONLY with a valid JSON object with exactly these keys: "
    "website, contact, social_links, email, phone."
)
# Keep old names as aliases for backward compat
_GROQ_EXTRACT_SUFFIX = _API_EXTRACT_SUFFIX
_GROQ_ENRICH_SUFFIX  = _API_ENRICH_SUFFIX

_API_RETRY_DEFAULT_WAIT = 60


class _ApiExtractor:
    """Base class for OpenAI-compatible API extractors (DeepSeek, Groq, …)."""

    _BASE_URL: str = ""

    def __init__(
        self,
        api_key: str,
        model: str,
        temperature: float = 0.1,
        timeout_seconds: int = 60,
        max_text_chars: int = 6000,
        rate_limit_seconds: float = 1.0,
    ):
        self.api_key = api_key
        self.model = model
        self.temperature = temperature
        self.timeout_seconds = timeout_seconds
        self.max_text_chars = max_text_chars
        self.rate_limit_seconds = rate_limit_seconds
        self._last_request_time: float = 0.0

    @property
    def model_fingerprint(self) -> str:
        return _prompt_hash(SYSTEM_PROMPT + self.model)

    def _headers(self) -> dict:
        return {"Authorization": f"Bearer {self.api_key}"}

    async def _rate_limit(self) -> None:
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < self.rate_limit_seconds:
            await asyncio.sleep(self.rate_limit_seconds - elapsed)
        self._last_request_time = time.monotonic()

    async def _post(self, payload: dict, label: str) -> dict:
        await self._rate_limit()
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                resp = await client.post(
                    f"{self._BASE_URL}/chat/completions",
                    json=payload,
                    headers=self._headers(),
                )
        except Exception as exc:
            log.warning("api_request_failed", provider=self.__class__.__name__, label=label, error=str(exc))
            return {}
        if resp.status_code == 402:
            raise ExtractorQuotaError(f"{self.__class__.__name__} billing limit (HTTP 402)")
        if resp.status_code == 429:
            retry_after = float(resp.headers.get("retry-after", _API_RETRY_DEFAULT_WAIT))
            raise ExtractorRateLimitError(retry_after)
        if resp.status_code >= 400:
            log.warning("api_request_failed", provider=self.__class__.__name__, label=label,
                        status=resp.status_code, body=resp.text[:200])
            return {}
        return resp.json()

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
            topic=topic, city=city, source_url=source_url, page_text=truncated,
        )
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT + false_positive_examples + _API_EXTRACT_SUFFIX},
                {"role": "user",   "content": user_message},
            ],
            "temperature": self.temperature,
            "response_format": {"type": "json_object"},
        }
        data = await self._post(payload, label=source_url)
        raw = data.get("choices", [{}])[0].get("message", {}).get("content", "")
        return _parse_communities(raw, city, topic, locale, source_url)

    async def extract_venues(
        self, text: str, city: str, locale: str, source_url: str,
    ) -> list[VenueRecord]:
        user_message = VENUE_USER_PROMPT_TEMPLATE.format(
            city=city, source_url=source_url, page_text=text[:self.max_text_chars],
        )
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": VENUE_SYSTEM_PROMPT + "\n\nRespond ONLY with valid JSON: {\"venues\": [...]}"},
                {"role": "user",   "content": user_message},
            ],
            "temperature": 0.0,
            "response_format": {"type": "json_object"},
        }
        try:
            data = await self._post(payload, label=source_url)
            raw = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            return _parse_venues(raw, city, locale, source_url)
        except ExtractorQuotaError:
            raise
        except Exception as exc:
            log.debug("api_extract_venues_failed", provider=self.__class__.__name__,
                      url=source_url, error=str(exc))
        return []

    async def extract_persons(
        self, text: str, city: str, topic: str, locale: str, source_url: str,
        community_names: list[str] | None = None,
    ) -> list[PersonRecord]:
        names_str = "\n".join(f"- {n}" for n in (community_names or [])) or "(none known)"
        user_message = PERSON_USER_PROMPT_TEMPLATE.format(
            city=city, topic=topic, source_url=source_url,
            community_names=names_str,
            page_text=text[:self.max_text_chars],
        )
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": PERSON_SYSTEM_PROMPT + "\n\nRespond ONLY with valid JSON: {\"persons\": [...]}"},
                {"role": "user",   "content": user_message},
            ],
            "temperature": 0.0,
            "response_format": {"type": "json_object"},
        }
        try:
            data = await self._post(payload, label=source_url)
            raw = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            return _parse_persons(raw, city, topic, locale, source_url)
        except ExtractorQuotaError:
            raise
        except Exception as exc:
            log.debug("api_extract_persons_failed", provider=self.__class__.__name__,
                      url=source_url, error=str(exc))
        return []

    async def enrich(self, record: CommunityRecord, page_text: str,
                     false_positive_examples: str = "") -> CommunityRecord:
        user_message = (
            f"Community group: '{record.name}' in {record.city}\n\n"
            f"--- PAGE TEXT ---\n{page_text[:self.max_text_chars]}"
        )
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": ENRICH_SYSTEM_PROMPT + false_positive_examples + _API_ENRICH_SUFFIX},
                {"role": "user",   "content": user_message},
            ],
            "temperature": 0.0,
            "response_format": {"type": "json_object"},
        }
        try:
            data = await self._post(payload, label=record.name)
            raw = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            return _apply_enrich(record, json.loads(raw))
        except ExtractorQuotaError:
            raise
        except Exception as exc:
            log.debug("api_enrich_failed", provider=self.__class__.__name__,
                      community=record.name, error=str(exc))
        return record


class DeepSeekExtractor(_ApiExtractor):
    _BASE_URL = "https://api.deepseek.com/v1"

    def __init__(
        self,
        api_key: str,
        model: str = "deepseek-chat",
        temperature: float = 0.1,
        timeout_seconds: int = 60,
        max_text_chars: int = 8000,
        rate_limit_seconds: float = 1.0,
    ):
        super().__init__(api_key, model, temperature, timeout_seconds, max_text_chars, rate_limit_seconds)


class GroqExtractor(_ApiExtractor):
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
        super().__init__(api_key, model, temperature, timeout_seconds, max_text_chars, rate_limit_seconds)


_GROQ_RETRY_DEFAULT_WAIT = _API_RETRY_DEFAULT_WAIT


class FallbackExtractor:
    """Chain of API extractors with OllamaExtractor as final fallback.

    Tries primaries left-to-right (e.g. DeepSeek → Groq) then Ollama.
    - ExtractorQuotaError  → permanent skip for that provider
    - ExtractorRateLimitError → temporary skip; retried after wait_seconds
    """

    def __init__(self, primaries: list, fallback: OllamaExtractor):
        self.primaries = primaries
        self.fallback = fallback
        self._exhausted = [False] * len(primaries)
        self._blocked_until = [0.0] * len(primaries)

    def _available(self, idx: int) -> bool:
        return not self._exhausted[idx] and time.monotonic() >= self._blocked_until[idx]

    def _first_available(self) -> int | None:
        for i in range(len(self.primaries)):
            if self._available(i):
                return i
        return None

    @property
    def model_fingerprint(self) -> str:
        idx = self._first_available()
        return self.primaries[idx].model_fingerprint if idx is not None else self.fallback.model_fingerprint

    @property
    def model(self) -> str:
        idx = self._first_available()
        return self.primaries[idx].model if idx is not None else self.fallback.model

    async def extract(self, text: str, city: str, topic: str, locale: str,
                      source_url: str, false_positive_examples: str = "") -> list[CommunityRecord]:
        for i, primary in enumerate(self.primaries):
            if not self._available(i):
                continue
            try:
                return await primary.extract(text, city, topic, locale, source_url, false_positive_examples)
            except ExtractorRateLimitError as exc:
                self._blocked_until[i] = time.monotonic() + exc.wait_seconds
                log.warning("extractor_rate_limited", provider=primary.__class__.__name__,
                            label=source_url, wait_s=exc.wait_seconds)
            except ExtractorQuotaError as exc:
                log.warning("extractor_quota_exhausted", provider=primary.__class__.__name__, reason=str(exc))
                self._exhausted[i] = True
        return await self.fallback.extract(text, city, topic, locale, source_url, false_positive_examples)

    async def enrich(self, record: CommunityRecord, page_text: str,
                     false_positive_examples: str = "") -> CommunityRecord:
        for i, primary in enumerate(self.primaries):
            if not self._available(i):
                continue
            try:
                return await primary.enrich(record, page_text, false_positive_examples)
            except ExtractorRateLimitError as exc:
                self._blocked_until[i] = time.monotonic() + exc.wait_seconds
                log.warning("extractor_rate_limited", provider=primary.__class__.__name__,
                            label=record.name, wait_s=exc.wait_seconds)
            except ExtractorQuotaError as exc:
                log.warning("extractor_quota_exhausted", provider=primary.__class__.__name__, reason=str(exc))
                self._exhausted[i] = True
        return await self.fallback.enrich(record, page_text, false_positive_examples)

    async def extract_venues(self, text: str, city: str, locale: str,
                             source_url: str) -> list[VenueRecord]:
        for i, primary in enumerate(self.primaries):
            if not self._available(i):
                continue
            try:
                return await primary.extract_venues(text, city, locale, source_url)
            except ExtractorRateLimitError as exc:
                self._blocked_until[i] = time.monotonic() + exc.wait_seconds
                log.warning("extractor_rate_limited", provider=primary.__class__.__name__,
                            label=source_url, wait_s=exc.wait_seconds)
            except ExtractorQuotaError as exc:
                log.warning("extractor_quota_exhausted", provider=primary.__class__.__name__, reason=str(exc))
                self._exhausted[i] = True
        return await self.fallback.extract_venues(text, city, locale, source_url)

    async def extract_persons(self, text: str, city: str, topic: str, locale: str,
                              source_url: str,
                              community_names: list[str] | None = None) -> list[PersonRecord]:
        for i, primary in enumerate(self.primaries):
            if not self._available(i):
                continue
            try:
                return await primary.extract_persons(text, city, topic, locale, source_url, community_names)
            except ExtractorRateLimitError as exc:
                self._blocked_until[i] = time.monotonic() + exc.wait_seconds
                log.warning("extractor_rate_limited", provider=primary.__class__.__name__,
                            label=source_url, wait_s=exc.wait_seconds)
            except ExtractorQuotaError as exc:
                log.warning("extractor_quota_exhausted", provider=primary.__class__.__name__, reason=str(exc))
                self._exhausted[i] = True
        return await self.fallback.extract_persons(text, city, topic, locale, source_url, community_names)

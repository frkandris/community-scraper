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


class ExtractorUnavailableError(Exception):
    """Raised when extraction could not run at all — transient API/network error,
    or every provider exhausted. Callers MUST treat this as "no result" and skip
    caching; caching an empty result would permanently record "0 communities"
    under the current fingerprint and the page would never be retried."""


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

For 'description': write 1-3 sentences summarising what this group does, who it is for, and \
what makes it distinctive. Write in the same language as the source page — if the page is in \
Swedish write in Swedish, if in English write in English, etc. Leave null only if the page gives \
no meaningful information about the group beyond its name.

Extract these additional fields when clearly stated on the page (leave null/empty if not found):
- 'founding_year': integer year founded (e.g. 1987). Only if explicitly stated.
- 'member_count': member count as string (e.g. "80", "200+", "~50 members"). Only if stated.
- 'fee': cost in original currency and language of the page (e.g. "Free", "€10/year", "3000 Ft/év"). \
  Set to the page's word for free only if page explicitly says it is free.
- 'age_range': age requirements if stated (e.g. "18+", "All ages", "55+").
- 'skill_level': skill/experience level in the page's language (e.g. "All levels", "Beginners welcome", "Advanced").
- 'join_process': how to join in the page's language (e.g. "Open to all", "Email required", "Audition required").
- 'leader': name and/or role of the organizer/leader (e.g. "Jane Smith, conductor").
- 'email': primary contact email address (must contain @).
- 'phone': primary phone number.
- 'tags': 1–5 specific subtopic keywords in the page's language \
  (e.g. for running: ["trail", "marathon", "cross-country"]).
- 'language': primary language(s) of the group (e.g. "Swedish", "English", "English/Swedish").
- 'history': 1–3 sentence background story or history of the group if the page describes it, \
  in the page's language. Leave null if not mentioned.
- 'frequency': how often the group meets as a short phrase in the page's language \
  (e.g. "Weekly", "Biweekly", "Monthly", "Twice a week"). \
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

For 'description': write 1-2 sentences describing the venue — what kind of place it is, \
its atmosphere or special features, and why community groups meet there. \
Use information from the page; do not invent details. Omit if nothing useful is available.

Output field values in the original language of the page.
If no venues found, return an empty venues array.
"""

VENUE_USER_PROMPT_TEMPLATE = """\
Extract physical venues that host community groups in {city} from the following page.
Source URL: {source_url}
{topics_hint}
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

# ── Runtime prompt override mechanism ─────────────────────────────────────────
# Callers (app.py) load DB overrides at startup and after edits via set_prompt_override().
# All extractor methods call get_prompt() so they always use the live active version.

_PROMPT_OVERRIDES: dict[str, str] = {}

PROMPT_KEYS = {
    "extraction_system": lambda: SYSTEM_PROMPT,
    "extraction_user":   lambda: USER_PROMPT_TEMPLATE,
    "enrich_system":     lambda: ENRICH_SYSTEM_PROMPT,
    "venue_system":      lambda: VENUE_SYSTEM_PROMPT,
    "venue_user":        lambda: VENUE_USER_PROMPT_TEMPLATE,
    "person_system":     lambda: PERSON_SYSTEM_PROMPT,
    "person_user":       lambda: PERSON_USER_PROMPT_TEMPLATE,
}


def get_prompt(key: str) -> str:
    return _PROMPT_OVERRIDES.get(key) or PROMPT_KEYS[key]()


def get_extract_fingerprint(model: str = "deepseek-chat") -> str:
    return _prompt_hash(get_prompt("extraction_system") + model)


def set_prompt_override(key: str, content: str | None) -> None:
    if content is None:
        _PROMPT_OVERRIDES.pop(key, None)
    else:
        _PROMPT_OVERRIDES[key] = content


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



_API_EXTRACT_SUFFIX = (
    "\n\nRespond ONLY with a valid JSON object: "
    "{\"communities\": [{\"name\": \"...\", \"confidence\": 0.9, \"joinable\": true, ...}]}"
)
_API_ENRICH_SUFFIX = (
    "\n\nRespond ONLY with a valid JSON object with exactly these keys: "
    "website, contact, social_links, email, phone."
)
# Keep old names as aliases for backward compat

_API_RETRY_DEFAULT_WAIT = 60


class _ApiExtractor:
    """Base class for OpenAI-compatible API extractors (DeepSeek, …)."""

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
        return _prompt_hash(get_prompt("extraction_system") + self.model)

    @property
    def venue_fingerprint(self) -> str:
        return _prompt_hash(get_prompt("venue_system") + self.model)

    @property
    def person_fingerprint(self) -> str:
        return _prompt_hash(get_prompt("person_system") + self.model)

    @property
    def enrich_fingerprint(self) -> str:
        return _prompt_hash(get_prompt("enrich_system") + self.model)

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
            raise ExtractorUnavailableError(f"{self.__class__.__name__}: {exc}") from exc
        if resp.status_code == 402:
            raise ExtractorQuotaError(f"{self.__class__.__name__} billing limit (HTTP 402)")
        if resp.status_code == 429:
            # Retry-After may be an HTTP-date (RFC 7231) — never let a parse
            # error escape the typed-error model and abort the run.
            try:
                retry_after = float(resp.headers.get("retry-after", _API_RETRY_DEFAULT_WAIT))
            except (TypeError, ValueError):
                retry_after = float(_API_RETRY_DEFAULT_WAIT)
            raise ExtractorRateLimitError(retry_after)
        if resp.status_code >= 400:
            log.warning("api_request_failed", provider=self.__class__.__name__, label=label,
                        status=resp.status_code, body=resp.text[:200])
            raise ExtractorUnavailableError(
                f"{self.__class__.__name__}: HTTP {resp.status_code}")
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
        user_message = get_prompt("extraction_user").format(
            topic=topic, city=city, source_url=source_url, page_text=truncated,
        )
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": get_prompt("extraction_system") + false_positive_examples + _API_EXTRACT_SUFFIX},
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
        valid_topics: list[str] | None = None,
    ) -> list[VenueRecord]:
        topics_hint = (
            f"\nValid topic slugs for 'welcomed_topics' (use these exact values): "
            f"{', '.join(valid_topics)}\n"
            if valid_topics else ""
        )
        user_message = get_prompt("venue_user").format(
            city=city, source_url=source_url, page_text=text[:self.max_text_chars],
            topics_hint=topics_hint,
        )
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": get_prompt("venue_system") + "\n\nRespond ONLY with valid JSON: {\"venues\": [...]}"},
                {"role": "user",   "content": user_message},
            ],
            "temperature": 0.0,
            "response_format": {"type": "json_object"},
        }
        try:
            data = await self._post(payload, label=source_url)
            raw = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            return _parse_venues(raw, city, locale, source_url)
        except (ExtractorQuotaError, ExtractorRateLimitError, ExtractorUnavailableError):
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
        user_message = get_prompt("person_user").format(
            city=city, topic=topic, source_url=source_url,
            community_names=names_str,
            page_text=text[:self.max_text_chars],
        )
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": get_prompt("person_system") + "\n\nRespond ONLY with valid JSON: {\"persons\": [...]}"},
                {"role": "user",   "content": user_message},
            ],
            "temperature": 0.0,
            "response_format": {"type": "json_object"},
        }
        try:
            data = await self._post(payload, label=source_url)
            raw = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            return _parse_persons(raw, city, topic, locale, source_url)
        except (ExtractorQuotaError, ExtractorRateLimitError, ExtractorUnavailableError):
            raise
        except Exception as exc:
            log.warning("api_extract_persons_failed", provider=self.__class__.__name__,
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
                {"role": "system", "content": get_prompt("enrich_system") + false_positive_examples + _API_ENRICH_SUFFIX},
                {"role": "user",   "content": user_message},
            ],
            "temperature": 0.0,
            "response_format": {"type": "json_object"},
        }
        try:
            data = await self._post(payload, label=record.name)
            raw = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            return _apply_enrich(record, json.loads(raw))
        except (ExtractorQuotaError, ExtractorRateLimitError, ExtractorUnavailableError):
            raise
        except Exception as exc:
            log.debug("api_enrich_failed", provider=self.__class__.__name__,
                      community=record.name, error=str(exc))
        return record

    async def chat(self, user_msg: str, temperature: float = 0.3) -> str:
        """Free-form chat completion — returns raw text."""
        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": user_msg}],
            "temperature": temperature,
        }
        data = await self._post(payload, label="chat")
        return data.get("choices", [{}])[0].get("message", {}).get("content", "").strip()


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


_GROQ_RETRY_DEFAULT_WAIT = _API_RETRY_DEFAULT_WAIT


class FallbackExtractor:
    """Chain of API extractors (currently DeepSeek only; the chain structure stays so a fallback provider can be re-added with one line).

    Tries primaries left-to-right.
    - ExtractorQuotaError  → permanent skip for that provider
    - ExtractorRateLimitError → temporary skip; retried after wait_seconds
    """

    def __init__(self, primaries: list):
        self.primaries = primaries
        self._exhausted = [False] * len(primaries)
        self._blocked_until = [0.0] * len(primaries)

    def _available(self, idx: int) -> bool:
        return not self._exhausted[idx] and time.monotonic() >= self._blocked_until[idx]

    def _first_available(self) -> int | None:
        for i in range(len(self.primaries)):
            if self._available(i):
                return i
        return None

    _RATE_LIMIT_MAX_WAIT = 300.0

    @property
    def exhausted(self) -> bool:
        """True when no provider is configured or every provider is permanently
        exhausted (HTTP 402) for this run."""
        return not self.primaries or all(self._exhausted)

    async def _call(self, method: str, label: str, *args, **kwargs):
        """Run `method` on the first available provider with failover.

        - ExtractorQuotaError  → provider permanently skipped for this run
        - ExtractorRateLimitError → provider blocked until the window passes;
          if every provider is only rate-limited, waits out the shortest window
          (max 5 min) and retries instead of failing the page
        - ExtractorUnavailableError (transient API/network) → retried once

        Raises ExtractorUnavailableError when the call could not run anywhere.
        Callers must treat that as "no result" and skip caching — never as an
        empty result.
        """
        last_error = "no extraction provider configured"
        for round_no in range(2):
            transient_seen = False
            for i, primary in enumerate(self.primaries):
                if not self._available(i):
                    continue
                try:
                    return await getattr(primary, method)(*args, **kwargs)
                except ExtractorRateLimitError as exc:
                    self._blocked_until[i] = time.monotonic() + exc.wait_seconds
                    last_error = f"rate limited ({exc.wait_seconds:.0f}s)"
                    log.warning("extractor_rate_limited", provider=primary.__class__.__name__,
                                label=label, wait_s=exc.wait_seconds)
                except ExtractorQuotaError as exc:
                    self._exhausted[i] = True
                    last_error = str(exc)
                    log.warning("extractor_quota_exhausted",
                                provider=primary.__class__.__name__, reason=str(exc))
                except ExtractorUnavailableError as exc:
                    transient_seen = True
                    last_error = str(exc)
            if round_no == 0 and not self.exhausted:
                if transient_seen:
                    continue  # one immediate retry for transient API/network errors
                waits = [self._blocked_until[i] - time.monotonic()
                         for i in range(len(self.primaries)) if not self._exhausted[i]]
                wait = min(waits) if waits else -1.0
                if 0 < wait <= self._RATE_LIMIT_MAX_WAIT:
                    log.info("extractor_awaiting_rate_limit", wait_s=round(wait, 1), label=label)
                    await asyncio.sleep(wait + 0.1)
                    continue
            break
        raise ExtractorUnavailableError(f"{method} unavailable: {last_error}")

    @property
    def model_fingerprint(self) -> str:
        idx = self._first_available()
        return self.primaries[idx].model_fingerprint if idx is not None else (self.primaries[0].model_fingerprint if self.primaries else "")

    @property
    def canonical_fingerprint(self) -> str:
        """Always returns primaries[0]'s fingerprint as the canonical cache key.

        model_fingerprint shifts to the fallback provider when the primary is
        exhausted, so pages extracted by the fallback end up stored under the
        fallback's fp — which never matches the done-pairs check (always uses
        primaries[0]). Using canonical_fingerprint for all cache read/write
        ensures every extraction, regardless of which provider ran, is stored
        under the same key that done-pairs checks.
        """
        return self.primaries[0].model_fingerprint if self.primaries else ""

    @property
    def venue_fingerprint(self) -> str:
        idx = self._first_available()
        return self.primaries[idx].venue_fingerprint if idx is not None else (self.primaries[0].venue_fingerprint if self.primaries else "")

    @property
    def person_fingerprint(self) -> str:
        idx = self._first_available()
        return self.primaries[idx].person_fingerprint if idx is not None else (self.primaries[0].person_fingerprint if self.primaries else "")

    @property
    def canonical_venue_fingerprint(self) -> str:
        """primaries[0]'s venue fingerprint — same rationale as canonical_fingerprint:
        venue results extracted by the fallback provider must be cached under the
        primary's key, or they get re-extracted once the primary recovers."""
        return self.primaries[0].venue_fingerprint if self.primaries else ""

    @property
    def canonical_person_fingerprint(self) -> str:
        """primaries[0]'s person fingerprint — see canonical_venue_fingerprint."""
        return self.primaries[0].person_fingerprint if self.primaries else ""

    @property
    def enrich_fingerprint(self) -> str:
        idx = self._first_available()
        return self.primaries[idx].enrich_fingerprint if idx is not None else (self.primaries[0].enrich_fingerprint if self.primaries else "")

    @property
    def model(self) -> str:
        idx = self._first_available()
        return self.primaries[idx].model if idx is not None else (self.primaries[0].model if self.primaries else "")

    async def extract(self, text: str, city: str, topic: str, locale: str,
                      source_url: str, false_positive_examples: str = "") -> list[CommunityRecord]:
        return await self._call("extract", source_url, text, city, topic, locale,
                                source_url, false_positive_examples)

    async def enrich(self, record: CommunityRecord, page_text: str,
                     false_positive_examples: str = "") -> CommunityRecord:
        return await self._call("enrich", record.name, record, page_text, false_positive_examples)

    async def extract_venues(self, text: str, city: str, locale: str,
                             source_url: str,
                             valid_topics: list[str] | None = None) -> list[VenueRecord]:
        return await self._call("extract_venues", source_url, text, city, locale,
                                source_url, valid_topics=valid_topics)

    async def extract_persons(self, text: str, city: str, topic: str, locale: str,
                              source_url: str,
                              community_names: list[str] | None = None) -> list[PersonRecord]:
        return await self._call("extract_persons", source_url, text, city, topic,
                                locale, source_url, community_names)

    async def chat(self, user_msg: str, temperature: float = 0.3) -> str:
        """Free-form chat completion with provider fallback."""
        return await self._call("chat", "chat", user_msg, temperature)

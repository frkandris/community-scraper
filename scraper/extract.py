import json
from datetime import datetime, timezone

import httpx
import structlog

from .models import CommunityRecord

log = structlog.get_logger()

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
    },
    "required": ["website", "contact", "social_links"],
}


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
    ) -> list[CommunityRecord]:
        truncated = text[: self.max_text_chars]
        user_message = USER_PROMPT_TEMPLATE.format(
            topic=topic,
            city=city,
            source_url=source_url,
            page_text=truncated,
        )
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
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
        return self._parse(raw, city, topic, locale, source_url)

    async def enrich(self, record: CommunityRecord, page_text: str) -> CommunityRecord:
        """Try to fill in missing website/contact/social_links from an additional page."""
        user_message = (
            f"Community group: '{record.name}' in {record.city}\n\n"
            f"--- PAGE TEXT ---\n{page_text[:self.max_text_chars]}"
        )
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": ENRICH_SYSTEM_PROMPT},
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

            updates: dict = {}
            if not record.website and enrichment.get("website"):
                updates["website"] = enrichment["website"]
            if not record.contact and enrichment.get("contact"):
                updates["contact"] = enrichment["contact"]
            if not record.social_links and enrichment.get("social_links"):
                updates["social_links"] = enrichment["social_links"]

            if updates:
                log.debug("enrich_merged", community=record.name, fields=list(updates))
                return record.model_copy(update=updates)
        except Exception as exc:
            log.debug("enrich_failed", community=record.name, error=str(exc))
        return record

    def _parse(
        self,
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
            log.warning("ollama_json_parse_failed", source_url=source_url,
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
                )
                records.append(record)
            except Exception as exc:
                log.warning("record_validation_failed", item=item, error=str(exc))
        return records

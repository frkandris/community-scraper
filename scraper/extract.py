import json
import re
from datetime import datetime, timezone

import httpx
import structlog

from .models import CommunityRecord

log = structlog.get_logger()

SYSTEM_PROMPT = """\
You are a data extraction assistant. Your only job is to find community groups and clubs.

Respond ONLY with a valid JSON array. No explanation, no markdown, no code fences.
If you find no community groups, respond with an empty array: []

Each object must follow this schema (omit fields you cannot find, use null not empty strings):
{
  "name": "name of the group",
  "description": "brief description or null",
  "meeting_schedule": "when they meet or null",
  "location": "venue or address or null",
  "contact": "email, phone, or contact URL or null",
  "website": "main website URL or null",
  "social_links": ["list of social media URLs"],
  "confidence": 0.0
}

Only include groups clearly related to the requested topic and city.
The page may be in any language – always output field values in the original language of the page.
"""

USER_PROMPT_TEMPLATE = """\
Extract all {topic} community groups located in or near {city} from the following web page text.
The page was found at: {source_url}

--- PAGE TEXT START ---
{page_text}
--- PAGE TEXT END ---
"""

_CODE_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)```", re.DOTALL)


def _repair_json(raw: str) -> str:
    raw = raw.strip()
    match = _CODE_FENCE_RE.search(raw)
    if match:
        raw = match.group(1).strip()
    # Find first '[' and last ']' to extract the array
    start = raw.find("[")
    end = raw.rfind("]")
    if start != -1 and end != -1 and end > start:
        raw = raw[start : end + 1]
    return raw


class OllamaExtractor:
    def __init__(
        self,
        base_url: str,
        model: str = "llama3.2:3b",
        temperature: float = 0.1,
        timeout_seconds: int = 120,
        max_text_chars: int = 3000,
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

        raw_content = data.get("message", {}).get("content", "")
        return self._parse_response(raw_content, city, topic, locale, source_url)

    def _parse_response(
        self,
        raw: str,
        city: str,
        topic: str,
        locale: str,
        source_url: str,
    ) -> list[CommunityRecord]:
        try:
            cleaned = _repair_json(raw)
            items = json.loads(cleaned)
            if not isinstance(items, list):
                return []
        except json.JSONDecodeError as exc:
            log.warning("ollama_json_parse_failed", source_url=source_url, error=str(exc), raw=raw[:200])
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
                    description=item.get("description"),
                    meeting_schedule=item.get("meeting_schedule"),
                    location=item.get("location"),
                    contact=item.get("contact"),
                    website=item.get("website"),
                    social_links=item.get("social_links") or [],
                    source_url=source_url,
                    extracted_at=extracted_at,
                    confidence=item.get("confidence"),
                )
                records.append(record)
            except Exception as exc:
                log.warning("record_validation_failed", item=item, error=str(exc))
        return records

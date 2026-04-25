import json
from typing import Union

from ..models import CommunityRecord

_TOPIC_TYPE: dict[str, str] = {
    "running": "SportsClub",
    "cycling": "SportsClub",
    "hiking": "SportsClub",
    "swimming": "SportsClub",
    "yoga": "SportsClub",
    "martial_arts": "SportsClub",
    "fitness": "SportsClub",
    "choir": "MusicGroup",
    "music": "MusicGroup",
    "dance": "DanceGroup",
    "theater": "PerformingGroup",
}

_DEFAULT_TYPE = "Organization"


def community_to_schema(record: Union["CommunityRecord", dict]) -> dict:
    if isinstance(record, dict):
        try:
            record = CommunityRecord.model_validate(record)
        except Exception:
            return {}

    schema_type = _TOPIC_TYPE.get(record.topic, _DEFAULT_TYPE)
    obj: dict = {"@type": schema_type, "name": record.name}

    if record.description:
        obj["description"] = record.description
    if record.website:
        obj["url"] = record.website

    loc: dict = {"@type": "Place", "addressLocality": record.city}
    if record.location:
        loc["name"] = record.location
    obj["location"] = loc

    if record.contact:
        obj["contactPoint"] = {
            "@type": "ContactPoint",
            "contactType": "general",
            "description": record.contact,
        }

    if record.social_links:
        obj["sameAs"] = record.social_links

    if record.meeting_schedule:
        obj["openingHoursSpecification"] = {
            "@type": "OpeningHoursSpecification",
            "description": record.meeting_schedule,
        }

    return obj


def records_to_jsonld(records: list) -> str:
    """Convert CommunityRecord objects or dicts to a JSON-LD string."""
    items = [item for r in records if (item := community_to_schema(r))]
    if not items:
        return ""
    ld = {
        "@context": "https://schema.org",
        "@graph": items,
    }
    return json.dumps(ld, ensure_ascii=False, indent=2)

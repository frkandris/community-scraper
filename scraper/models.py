from pydantic import BaseModel, Field


class SearchResult(BaseModel):
    url: str
    title: str
    snippet: str = ""


class CommunityRecord(BaseModel):
    name: str
    topic: str
    city: str
    locale: str
    description: str | None = None
    meeting_schedule: str | None = None
    location: str | None = None
    contact: str | None = None
    website: str | None = None
    social_links: list[str] = Field(default_factory=list)
    source_url: str
    extracted_at: str
    confidence: float | None = None


class RunMetadata(BaseModel):
    last_run: str
    records_by_city_topic: dict[str, dict[str, int]] = Field(default_factory=dict)
    total_records: int = 0

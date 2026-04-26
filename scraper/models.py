import hashlib

from pydantic import BaseModel, Field, model_validator


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
    source_urls: list[str] = Field(default_factory=list)
    extracted_at: str
    confidence: float | None = None
    joinable: bool = True  # open, recurring group a person can join
    community_id: str = ""
    # Extended profile fields
    founding_year: int | None = None
    member_count: str | None = None
    fee: str | None = None
    age_range: str | None = None
    skill_level: str | None = None
    join_process: str | None = None
    leader: str | None = None
    email: str | None = None
    phone: str | None = None
    tags: list[str] = Field(default_factory=list)
    language: str | None = None

    @model_validator(mode="after")
    def _clean_and_generate_id(self) -> "CommunityRecord":
        # Normalize website: add https:// if no scheme present
        if self.website:
            w = self.website.strip()
            if w and not w.startswith(("http://", "https://")):
                w = "https://" + w
            self.website = w or None

        # Keep only actual URLs in social_links
        self.social_links = [
            lnk for lnk in self.social_links
            if isinstance(lnk, str) and lnk.strip().startswith(("http://", "https://"))
        ]

        # Email must contain @
        if self.email and "@" not in self.email:
            self.email = None

        # Tags: strip, deduplicate, cap at 8
        if self.tags:
            self.tags = list(dict.fromkeys(t.strip() for t in self.tags if t.strip()))[:8]

        # Ensure source_url is always in source_urls
        if self.source_url and self.source_url not in self.source_urls:
            self.source_urls = [self.source_url] + self.source_urls

        if not self.community_id:
            key = f"{self.name.lower()}|{self.city.lower()}"
            self.community_id = hashlib.sha256(key.encode()).hexdigest()[:12]
        return self


class RunMetadata(BaseModel):
    last_run: str
    records_by_city_topic: dict[str, dict[str, int]] = Field(default_factory=dict)
    total_records: int = 0

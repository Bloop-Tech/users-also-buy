from __future__ import annotations

import re
import unicodedata
from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, Field, field_validator


ThemeStatus = Literal["active", "insufficient_products", "deprecated", "pending"]


def slugify_title(title_en: str) -> str:
    normalized = unicodedata.normalize("NFKD", title_en)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", ascii_text.lower()).strip("-")
    return slug or "theme"


class ExplorationTheme(BaseModel):
    id: str
    title_en: str
    title_pt: str
    title_es: str
    description: str
    tags: list[str] = Field(default_factory=list)
    category: str
    audience: str | None = None
    season: str | None = None
    search_queries: list[str] = Field(default_factory=list)
    product_ids: list[str] = Field(default_factory=list)
    status: ThemeStatus = "pending"
    version: int = 0
    updated_at: datetime | None = None

    @field_validator("tags", mode="before")
    @classmethod
    def normalize_tags(cls, value: object) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [value]
        if isinstance(value, list):
            return [str(item) for item in value]
        return [str(value)]

    @classmethod
    def from_generated(
        cls,
        *,
        title_en: str,
        title_pt: str,
        title_es: str,
        description: str,
        tags: list[str],
        category: str,
        audience: str | None,
        season: str | None,
    ) -> ExplorationTheme:
        return cls(
            id=slugify_title(title_en),
            title_en=title_en,
            title_pt=title_pt,
            title_es=title_es,
            description=description,
            tags=tags,
            category=category,
            audience=audience,
            season=season,
            status="pending",
            version=0,
        )

    def embedding_text(self) -> str:
        tags = ", ".join(self.tags)
        return f"{self.title_en}. {self.description}. Tags: {tags}"

    def to_typesense_document(self, embedding: list[float]) -> dict:
        updated_at = self.updated_at or datetime.now(UTC)
        return {
            "id": self.id,
            "title_en": self.title_en,
            "title_pt": self.title_pt,
            "title_es": self.title_es,
            "description": self.description,
            "tags": self.tags,
            "category": self.category,
            "audience": self.audience or "",
            "season": self.season or "",
            "search_queries": self.search_queries,
            "product_ids": self.product_ids,
            "product_count": len(self.product_ids),
            "status": self.status,
            "version": self.version,
            "updated_at": int(updated_at.timestamp()),
            "embedding": embedding,
        }


class ThemesFile(BaseModel):
    themes: list[ExplorationTheme] = Field(default_factory=list)


class GeneratedTheme(BaseModel):
    title_en: str
    title_pt: str
    title_es: str
    description: str
    tags: list[str]
    category: str
    audience: str | None = None
    season: str | None = None


class GeneratedThemesBatch(BaseModel):
    themes: list[GeneratedTheme] = Field(..., min_length=1)
    reasoning: str


class ThemeQueries(BaseModel):
    queries: list[str] = Field(..., min_length=2, max_length=4)
    reasoning: str


class ProductCandidate(BaseModel):
    product_id: str
    name: str
    brand: str | None = None
    categories: str | None = None
    description: str | None = None


class ThemeProductSelection(BaseModel):
    product_ids: list[str] = Field(..., min_length=1, max_length=15)
    reasoning: str


class ThemesPipelineRunConfig(BaseModel):
    mode: Literal["refresh", "expand"]
    theme_ids: list[str] | None = None
    expand_count: int = 20
    dry_run: bool = False
    regenerate_metadata: bool = False
    target_version: int | None = None

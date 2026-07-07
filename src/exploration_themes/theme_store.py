from __future__ import annotations

import math
from pathlib import Path

import yaml

from src.common.embeddings import EmbeddingsClient
from src.exploration_themes.data_models import (
    ExplorationTheme,
    GeneratedTheme,
    ThemesFile,
    slugify_title,
)
from src.exploration_themes.runtime_config import get_runtime_config

DEDUP_SIMILARITY_THRESHOLD = 0.85


class ThemeStore:
    def __init__(self, path: Path | None = None) -> None:
        self.path = path or get_runtime_config().themes_file

    def load(self) -> ThemesFile:
        if not self.path.exists():
            return ThemesFile()
        with self.path.open(encoding="utf-8") as handle:
            raw = yaml.safe_load(handle) or {}
        return ThemesFile.model_validate(raw)

    def save(self, themes_file: ThemesFile) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = themes_file.model_dump(mode="json", exclude_none=True)
        with self.path.open("w", encoding="utf-8") as handle:
            yaml.safe_dump(payload, handle, sort_keys=False, allow_unicode=True)

    def get_theme(self, theme_id: str) -> ExplorationTheme | None:
        themes_file = self.load()
        for theme in themes_file.themes:
            if theme.id == theme_id:
                return theme
        return None

    def upsert_theme(self, theme: ExplorationTheme) -> None:
        themes_file = self.load()
        for index, existing in enumerate(themes_file.themes):
            if existing.id == theme.id:
                themes_file.themes[index] = theme
                self.save(themes_file)
                return
        themes_file.themes.append(theme)
        self.save(themes_file)

    def append_generated_themes(
        self,
        generated: list[GeneratedTheme],
        embeddings_client: EmbeddingsClient,
    ) -> list[ExplorationTheme]:
        themes_file = self.load()
        existing_embeddings = self._existing_title_embeddings(
            themes_file.themes, embeddings_client
        )
        added: list[ExplorationTheme] = []
        existing_ids = {theme.id for theme in themes_file.themes}

        for item in generated:
            theme = ExplorationTheme.from_generated(
                title_en=item.title_en,
                title_pt=item.title_pt,
                title_es=item.title_es,
                description_en=item.description_en,
                description_pt=item.description_pt,
                description_es=item.description_es,
                tags=item.tags,
                category=item.category,
                audience=item.audience,
                seasons=item.seasons,
            )
            if theme.id in existing_ids:
                theme.id = f"{theme.id}-{len(existing_ids)}"
            if self._is_duplicate(theme, existing_embeddings, embeddings_client):
                continue
            themes_file.themes.append(theme)
            existing_ids.add(theme.id)
            added.append(theme)
            existing_embeddings.append(
                embeddings_client.embed(system_prompt="", query=theme.title_en)
            )

        if added:
            self.save(themes_file)
        return added

    def list_themes(
        self,
        *,
        theme_ids: list[str] | None = None,
        statuses: list[str] | None = None,
    ) -> list[ExplorationTheme]:
        themes = self.load().themes
        if theme_ids:
            allowed = set(theme_ids)
            themes = [theme for theme in themes if theme.id in allowed]
        if statuses:
            allowed_statuses = set(statuses)
            themes = [theme for theme in themes if theme.status in allowed_statuses]
        return themes

    def existing_title_en_values(self) -> list[str]:
        return [theme.title_en for theme in self.load().themes]

    @staticmethod
    def _existing_title_embeddings(
        themes: list[ExplorationTheme],
        embeddings_client: EmbeddingsClient,
    ) -> list[list[float]]:
        if not themes:
            return []
        return embeddings_client.embed_batch([theme.title_en for theme in themes])

    @staticmethod
    def _cosine_similarity(left: list[float], right: list[float]) -> float:
        dot = sum(a * b for a, b in zip(left, right, strict=True))
        left_norm = math.sqrt(sum(value * value for value in left))
        right_norm = math.sqrt(sum(value * value for value in right))
        if left_norm == 0 or right_norm == 0:
            return 0.0
        return dot / (left_norm * right_norm)

    def _is_duplicate(
        self,
        theme: ExplorationTheme,
        existing_embeddings: list[list[float]],
        embeddings_client: EmbeddingsClient,
    ) -> bool:
        if not existing_embeddings:
            return False
        candidate = embeddings_client.embed(system_prompt="", query=theme.title_en)
        for existing in existing_embeddings:
            if (
                self._cosine_similarity(candidate, existing)
                >= DEDUP_SIMILARITY_THRESHOLD
            ):
                return True
        return False


def ensure_unique_id(title_en: str, existing_ids: set[str]) -> str:
    base = slugify_title(title_en)
    candidate = base
    suffix = 2
    while candidate in existing_ids:
        candidate = f"{base}-{suffix}"
        suffix += 1
    return candidate

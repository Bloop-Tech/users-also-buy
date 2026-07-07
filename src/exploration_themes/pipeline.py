from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime

from src.common.search import SearchService
from src.exploration_themes.agents import (
    get_theme_generator_agent,
    get_theme_query_agent,
    get_theme_selector_agent,
)
from src.exploration_themes.data_models import (
    ExplorationTheme,
    ThemesPipelineRunConfig,
)
from src.exploration_themes.quality_gates import apply_quality_gates
from src.exploration_themes.theme_store import ThemeStore
from src.exploration_themes.typesense_writer import ExplorationThemesWriter

logger = logging.getLogger(__name__)

CANDIDATE_LIMIT = 50
SEARCH_PER_PAGE = 30
SEARCH_RESULT_LIMIT = 15


class ExplorationThemesPipeline:
    def __init__(
        self,
        *,
        theme_store: ThemeStore | None = None,
        search_service: SearchService | None = None,
        typesense_writer: ExplorationThemesWriter | None = None,
        llm_semaphore: asyncio.Semaphore | None = None,
    ) -> None:
        self.theme_store = theme_store or ThemeStore()
        self.search_service = search_service or SearchService.build(
            per_page=SEARCH_PER_PAGE,
            result_limit=SEARCH_RESULT_LIMIT,
        )
        self.typesense_writer = typesense_writer or ExplorationThemesWriter()
        self.theme_generator = get_theme_generator_agent()
        self.query_agent = get_theme_query_agent()
        self.selector_agent = get_theme_selector_agent()
        self.llm_semaphore = llm_semaphore or asyncio.Semaphore(8)

    async def run(self, config: ThemesPipelineRunConfig) -> None:
        if config.mode == "expand":
            await self._expand(config)
        else:
            await self._refresh(config)

    async def _expand(self, config: ThemesPipelineRunConfig) -> None:
        existing_titles = self.theme_store.existing_title_en_values()
        prompt = (
            f"Generate {config.expand_count} new unique themes.\n"
            f"Existing themes to avoid duplicating:\n"
            f"{json.dumps(existing_titles, ensure_ascii=False, indent=2)}"
        )
        async with self.llm_semaphore:
            result = await self.theme_generator.run(prompt)
        if config.dry_run:
            logger.info("Dry run — generated themes: %s", result.output.themes)
            return

        added = self.theme_store.append_generated_themes(
            result.output.themes,
            self.typesense_writer.embeddings_client,
        )
        logger.info("Added %d new themes to %s", len(added), self.theme_store.path)
        for theme in added:
            await self.process_theme(theme, dry_run=config.dry_run)

    async def _refresh(self, config: ThemesPipelineRunConfig) -> None:
        themes = self.theme_store.list_themes(theme_ids=config.theme_ids)
        if not themes:
            logger.warning("No themes found to refresh.")
            return
        for theme in themes:
            if (
                config.target_version is not None
                and theme.version >= config.target_version
            ):
                logger.info(
                    "Skipping %s — already at version %s", theme.id, theme.version
                )
                continue
            await self.process_theme(theme, dry_run=config.dry_run)

    async def process_theme(
        self,
        theme: ExplorationTheme,
        *,
        dry_run: bool = False,
    ) -> ExplorationTheme:
        logger.info("Processing theme %s (%s)", theme.id, theme.title_en)

        query_prompt = (
            "Suggest search queries for this theme:\n"
            f"{json.dumps(theme.model_dump(exclude={'product_ids', 'search_queries', 'embedding'}), indent=2, ensure_ascii=False)}"
        )
        async with self.llm_semaphore:
            query_result = await self.query_agent.run(query_prompt)
        queries = [q.strip() for q in query_result.output.queries if q.strip()]
        theme.search_queries = queries

        candidates = await self._search_candidates(queries)
        if not candidates:
            theme.status = "insufficient_products"
            theme.updated_at = datetime.now(UTC)
            if not dry_run:
                self.theme_store.upsert_theme(theme)
            logger.warning("No candidates found for theme %s", theme.id)
            return theme

        selector_prompt = self._build_selector_prompt(theme, candidates)
        async with self.llm_semaphore:
            selection = await self.selector_agent.run(selector_prompt)

        candidate_ids = {
            str(item["product_id"]) for item in candidates if item.get("product_id")
        }
        candidate_brands = {
            str(item["product_id"]): item.get("product_brand")
            for item in candidates
            if item.get("product_id")
        }
        theme = apply_quality_gates(
            theme,
            selected_product_ids=selection.output.product_ids,
            candidate_ids=candidate_ids,
            candidate_brands=candidate_brands,
        )
        theme.updated_at = datetime.now(UTC)

        if dry_run:
            logger.info(
                "Dry run — theme %s status=%s products=%d",
                theme.id,
                theme.status,
                len(theme.product_ids),
            )
            return theme

        self.theme_store.upsert_theme(theme)
        self.typesense_writer.upsert_theme(theme)
        logger.info(
            "Theme %s done — status=%s products=%d version=%d",
            theme.id,
            theme.status,
            len(theme.product_ids),
            theme.version,
        )
        return theme

    async def _search_candidates(self, queries: list[str]) -> list[dict]:
        seen: set[str] = set()
        merged: list[dict] = []
        for query in queries:
            hits = await asyncio.to_thread(
                self.search_service.compute_search_results, query
            )
            for hit in hits:
                product_id = hit.get("product_id")
                if not product_id or product_id in seen:
                    continue
                seen.add(str(product_id))
                merged.append(hit)
                if len(merged) >= CANDIDATE_LIMIT:
                    return merged
        return merged

    @staticmethod
    def _build_selector_prompt(theme: ExplorationTheme, candidates: list[dict]) -> str:
        simplified = [
            {
                "product_id": hit.get("product_id"),
                "name": hit.get("product_name"),
                "brand": hit.get("product_brand"),
                "categories": hit.get("product_categories"),
                "description": (hit.get("product_description") or "")[:300],
            }
            for hit in candidates
        ]
        return (
            "Select products for this theme:\n"
            f"{json.dumps(theme.model_dump(exclude={'product_ids', 'search_queries'}), indent=2, ensure_ascii=False)}\n\n"
            "Candidate products:\n"
            f"{json.dumps(simplified, indent=2, ensure_ascii=False)}"
        )

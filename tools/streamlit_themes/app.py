from __future__ import annotations

import asyncio
import os
from typing import Any

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from src.common.search import SearchService
from src.exploration_themes.data_models import ExplorationTheme
from src.exploration_themes.pipeline import ExplorationThemesPipeline
from src.exploration_themes.theme_store import ThemeStore
from src.exploration_themes.typesense_writer import ExplorationThemesWriter

load_dotenv()

THEME_STATE_KEY = "theme_refresh_result"
SEARCH_ENV_VARS = [
    "EMBEDDINGS_SERVICE_URL",
    "TYPESENSE_NODE_HOST",
    "TYPESENSE_API_KEY",
    "TYPESENSE_PORT",
]


def get_missing_env(vars_list: list[str]) -> list[str]:
    return [name for name in vars_list if not os.getenv(name)]


@st.cache_resource(show_spinner=False)
def _search_service_factory() -> SearchService:
    return SearchService.build(per_page=30, result_limit=15)


def load_search_service() -> tuple[SearchService | None, str | None]:
    missing = get_missing_env(SEARCH_ENV_VARS)
    if missing:
        return None, f"Missing search environment variables: {', '.join(missing)}"
    try:
        return _search_service_factory(), None
    except Exception as exc:  # pragma: no cover - networking
        return None, f"Unable to initialise search service: {exc}"


@st.cache_resource(show_spinner=False)
def _theme_store_factory() -> ThemeStore:
    return ThemeStore()


def format_theme_option(theme: ExplorationTheme) -> str:
    return f"{theme.title_en} [{theme.status}] — {len(theme.product_ids)} products"


def load_typesense_theme(theme_id: str) -> dict[str, Any] | None:
    missing = get_missing_env(SEARCH_ENV_VARS)
    if missing:
        return None
    try:
        writer = ExplorationThemesWriter()
        return writer.get_theme_document(theme_id)
    except Exception:
        return None


def render_theme_details(theme: ExplorationTheme) -> None:
    st.subheader(theme.title_en)
    st.caption(f"PT: {theme.title_pt} · ES: {theme.title_es}")
    st.write(theme.description)

    cols = st.columns(4)
    cols[0].metric("Status", theme.status)
    cols[1].metric("Products", len(theme.product_ids))
    cols[2].metric("Version", theme.version)
    cols[3].metric("Queries", len(theme.search_queries))

    st.markdown("**Tags**")
    st.write(", ".join(theme.tags) if theme.tags else "—")

    if theme.search_queries:
        st.markdown("**Search queries**")
        for query in theme.search_queries:
            st.code(query)

    typesense_doc = load_typesense_theme(theme.id)
    if typesense_doc:
        st.success("Theme found in Typesense.")
        with st.expander("Typesense document"):
            st.json(
                {
                    key: value
                    for key, value in typesense_doc.items()
                    if key != "embedding"
                }
            )
    else:
        st.warning("Theme not found in Typesense (or Typesense unavailable).")

    search_service, search_error = load_search_service()
    if search_error:
        st.warning(search_error)
        return

    if theme.search_queries:
        st.markdown("**Search preview (first query)**")
        try:
            hits = search_service.compute_search_results(theme.search_queries[0])
            if hits:
                st.dataframe(pd.DataFrame(hits), use_container_width=True)
            else:
                st.info("No search hits for first query.")
        except Exception as exc:  # pragma: no cover - external service
            st.error(f"Search failed: {exc}")


def main() -> None:
    st.set_page_config(
        page_title="Exploration Themes QA",
        layout="wide",
        page_icon="🎨",
    )
    st.title("Exploration themes QA")
    st.caption("Review themes from data/themes.yaml and preview search results.")

    theme_store = _theme_store_factory()
    themes_file = theme_store.load()
    themes = themes_file.themes

    if not themes:
        st.info("No themes in data/themes.yaml yet. Run expand mode first.")
        return

    status_filter = st.multiselect(
        "Filter by status",
        options=sorted({theme.status for theme in themes}),
        default=sorted({theme.status for theme in themes}),
    )
    filtered = [theme for theme in themes if theme.status in status_filter]

    selected = st.selectbox(
        "Choose a theme",
        options=filtered,
        format_func=format_theme_option,
    )

    render_theme_details(selected)

    st.divider()
    st.subheader("Refresh this theme")
    if st.button("Run refresh (dry-run)", type="primary"):
        with st.spinner("Running theme pipeline..."):
            pipeline = ExplorationThemesPipeline(theme_store=theme_store)
            result = asyncio.run(
                pipeline.process_theme(selected.model_copy(deep=True), dry_run=True)
            )
            st.session_state[THEME_STATE_KEY] = result

    result = st.session_state.get(THEME_STATE_KEY)
    if result:
        st.markdown("**Dry-run result**")
        st.json(
            result.model_dump(
                mode="json",
                exclude_none=True,
            )
        )


if __name__ == "__main__":
    main()

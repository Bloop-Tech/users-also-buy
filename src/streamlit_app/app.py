from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from streamlit.delta_generator import DeltaGenerator

from src.agent import get_agent
from src.search import SearchService

load_dotenv()

DATA_PATH = Path(__file__).resolve().parents[2] / "marketplacer-export.csv"
TITLE_COL = "*Title Description"
BRAND_COL = "*Brand"
CATEGORY_COL = "*Category"
PRODUCT_ID_COL = "Ad ID"
RESULT_STATE_KEY = "agent_comparison"
AGENT_ENV_VARS = [
    "AZURE_OPENAI_ENDPOINT",
    "AZURE_OPENAI_API_VERSION",
    "AZURE_OPENAI_API_KEY",
]
SEARCH_ENV_VARS = [
    "EMBEDDINGS_SERVICE_URL",
    "TYPESENSE_NODE_HOST",
    "TYPESENSE_API_KEY",
    "TYPESENSE_PORT",
]
VARIANTS = (
    ("Generic (generic_variant=True)", True),
    ("Specific (generic_variant=False)", False),
)


@st.cache_data(show_spinner=False)
def load_products(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    return df.reset_index(drop=True)


def readable_value(value: Any, fallback: str = "Unknown") -> str:
    if pd.isna(value):
        return fallback
    text = str(value).strip()
    return text or fallback


def format_product_option(row: pd.Series) -> str:
    title = readable_value(row.get(TITLE_COL), "Untitled")
    brand = readable_value(row.get(BRAND_COL), "No brand")
    category = readable_value(row.get(CATEGORY_COL), "No category")
    product_id = readable_value(row.get(PRODUCT_ID_COL), "N/A")
    return f"{title} — {brand} ({category}) · #{product_id}"


def build_product_metadata(row: pd.Series) -> dict[str, str]:
    metadata_map = {
        "category_lvl_1": row.get("*Category"),
        "category_lvl_2": row.get("Category 2"),
        "category_lvl_3": row.get("Category 3"),
        "category_lvl_4": row.get("Category 4"),
        "brand": row.get("*Brand"),
        "title": row.get(TITLE_COL),
        "description": row.get("*Main Description"),
    }
    metadata: dict[str, str] = {}
    for key, value in metadata_map.items():
        if pd.isna(value):
            continue
        value_str = str(value).strip()
        if value_str:
            metadata[key] = value_str
    return metadata


def get_missing_env(vars_list: list[str]) -> list[str]:
    return [name for name in vars_list if not os.getenv(name)]


@st.cache_resource(show_spinner=False)
def _search_service_factory() -> SearchService:
    return SearchService.build()


def load_search_service() -> tuple[SearchService | None, str | None]:
    missing = get_missing_env(SEARCH_ENV_VARS)
    if missing:
        return None, f"Missing search environment variables: {', '.join(missing)}"
    try:
        return _search_service_factory(), None
    except Exception as exc:  # pragma: no cover - networking
        return None, f"Unable to initialise search service: {exc}"


def run_agent_variant(
    *,
    metadata: dict[str, str],
    generic_variant: bool,
    search_service: SearchService | None,
    search_error: str | None,
) -> dict[str, Any]:
    missing = get_missing_env(AGENT_ENV_VARS)
    if missing:
        return {"error": f"Missing agent environment variables: {', '.join(missing)}"}

    agent = get_agent(generic_variant=generic_variant)
    prompt = (
        "Suggest also-buy queries for this product:\n"
        f"{json.dumps(metadata, indent=2, ensure_ascii=False)}"
    )
    try:
        result = agent.run_sync(prompt)
    except Exception as exc:  # pragma: no cover - external service
        return {"error": f"Agent call failed: {exc}"}

    queries = [query.strip() for query in result.output.queries if query.strip()]
    per_query: dict[str, dict[str, Any]] = {}
    for query in queries:
        per_query[query] = {}
        if search_service is None:
            per_query[query]["error"] = search_error or "Search service unavailable."
            continue
        try:
            hits = search_service.compute_search_results(query)
            per_query[query]["results"] = hits
        except Exception as exc:  # pragma: no cover - external service
            per_query[query]["error"] = f"Search failed: {exc}"

    usage = None
    try:
        usage_data = result.usage()
        usage = {
            "input_tokens": getattr(usage_data, "input_tokens", None),
            "output_tokens": getattr(usage_data, "output_tokens", None),
        }
    except Exception:
        usage = None

    return {
        "reasoning": result.output.reasoninig,
        "queries": queries,
        "per_query": per_query,
        "usage": usage,
    }


def render_variant_column(
    column: DeltaGenerator,
    *,
    title: str,
    payload: dict[str, Any],
) -> None:
    column.subheader(title)
    if not payload:
        column.info("No data to show yet.")
        return
    if payload.get("error"):
        column.error(payload["error"])
        return

    if payload.get("usage"):
        usage = payload["usage"]
        tokens = []
        if usage.get("input_tokens") is not None:
            tokens.append(f"in: {usage['input_tokens']}")
        if usage.get("output_tokens") is not None:
            tokens.append(f"out: {usage['output_tokens']}")
        if tokens:
            column.caption(f"Token usage: {', '.join(tokens)}")

    column.markdown(f"**Reasoning**: {payload.get('reasoning', 'n/a')}")

    per_query = payload.get("per_query", {})
    if not per_query:
        column.warning("Agent did not return any queries.")
        return

    for query, results in per_query.items():
        column.markdown(f"##### Query: `{query}`")
        if "error" in results:
            column.error(results["error"])
            continue
        hits = results.get("results") or []
        if not hits:
            column.info("No search results found.")
            continue
        result_df = pd.DataFrame(hits)
        column.dataframe(result_df, use_container_width=True)


def generate_comparison(row: pd.Series) -> dict[str, Any]:
    metadata = build_product_metadata(row)
    search_service, search_error = load_search_service()
    results: dict[str, dict[str, Any]] = {}
    for label, flag in VARIANTS:
        results[label] = run_agent_variant(
            metadata=metadata,
            generic_variant=flag,
            search_service=search_service,
            search_error=search_error,
        )
    return {
        "product_id": readable_value(row.get(PRODUCT_ID_COL)),
        "metadata": metadata,
        "results": results,
        "search_service_error": search_error,
    }


def main() -> None:
    st.set_page_config(
        page_title="Users Also Buy Playground",
        layout="wide",
        page_icon="🛒",
    )
    st.title("Users-also-buy comparison")
    st.caption(
        "Explore how the agent behaves when `generic_variant` is toggled and compare the downstream search hits."
    )

    if not DATA_PATH.exists():
        st.error(f"Could not find CSV at {DATA_PATH}")
        st.stop()

    products_df = load_products(str(DATA_PATH))
    if products_df.empty:
        st.warning("The CSV is empty. Add products and reload the app.")
        st.stop()

    selected_index = st.selectbox(
        "Choose a product",
        options=products_df.index.tolist(),
        format_func=lambda idx: format_product_option(products_df.loc[idx]),
    )
    selected_row = products_df.loc[selected_index]
    metadata = build_product_metadata(selected_row)

    st.write(
        f"**Selected product:** {readable_value(selected_row.get(TITLE_COL))} — "
        f"{readable_value(selected_row.get(BRAND_COL))}"
    )
    if not metadata:
        st.warning("This product has insufficient metadata for the agent.")

    with st.expander("Metadata sent to the agent"):
        st.json(metadata or {"info": "No metadata available"})

    with st.expander("Raw Marketplacer row"):
        st.dataframe(selected_row.to_frame(name="value"))

    if RESULT_STATE_KEY not in st.session_state:
        st.session_state[RESULT_STATE_KEY] = None

    current_product_id = readable_value(selected_row.get(PRODUCT_ID_COL))
    previous = st.session_state.get(RESULT_STATE_KEY)
    if previous and previous.get("product_id") != current_product_id:
        st.session_state[RESULT_STATE_KEY] = None

    if st.button(
        "Generate suggestions",
        type="primary",
        disabled=not metadata,
    ):
        with st.spinner("Running agents and searching..."):
            st.session_state[RESULT_STATE_KEY] = generate_comparison(selected_row)

    comparison = st.session_state.get(RESULT_STATE_KEY)
    if not comparison:
        st.info("Pick a product and click **Generate suggestions** to see results.")
        return

    if comparison.get("search_service_error"):
        st.warning(comparison["search_service_error"])

    columns = st.columns(2)
    for column, (label, _) in zip(columns, VARIANTS, strict=True):
        render_variant_column(
            column,
            title=label,
            payload=comparison["results"].get(label, {}),
        )


if __name__ == "__main__":
    main()

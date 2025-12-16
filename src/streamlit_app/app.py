from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from streamlit.delta_generator import DeltaGenerator

from src.agent import get_agent
from src.data_models import Product
from src.products_fetcher import ProductsFetcher
from src.search import SearchService

load_dotenv()

RESULT_STATE_KEY = "agent_comparison"
PRODUCTS_STATE_KEY = "fetched_products"
DEFAULT_LIMIT = 20
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


def readable_value(value: Any, fallback: str = "Unknown") -> str:
    if value is None:
        return fallback
    text = str(value).strip()
    return text or fallback


def format_product_option(product: Product) -> str:
    brand = readable_value(product.brand, "No brand")
    category = readable_value(product.category_lvl_1, "No category")
    created_at = product.created_date.strftime("%Y-%m-%d")
    return f"{product.title} — {brand} ({category}) · {created_at}"


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


@st.cache_resource(show_spinner=False)
def _products_fetcher_factory() -> ProductsFetcher:
    return ProductsFetcher()


@st.cache_data(show_spinner=False)
def fetch_products_for_range(
    start_date: date, end_date: date, limit: int
) -> tuple[list[Product], str | None]:
    if start_date > end_date:
        return [], "Start date must be before or equal to end date."

    if not os.getenv("MARKETPLACER_URL"):
        return [], "Missing MARKETPLACER_URL environment variable."

    try:
        fetcher = _products_fetcher_factory()
    except Exception as exc:  # pragma: no cover - networking
        return [], f"Unable to initialise product fetcher: {exc}"

    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date, datetime.max.time())

    products: list["Product"] = []
    try:
        for batch in fetcher.fetch_products(
            min_date=start_dt,
            max_date=end_dt,
            limit=limit,
        ):
            products.extend(batch)
    except Exception as exc:  # pragma: no cover - networking
        return [], f"Error fetching products: {exc}"

    return products, None


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


def build_product_metadata(product: Product) -> dict[str, str]:
    return product.metadata


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


def generate_comparison(product: Product) -> dict[str, Any]:
    metadata = build_product_metadata(product)
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
        "product_id": product.id,
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

    if RESULT_STATE_KEY not in st.session_state:
        st.session_state[RESULT_STATE_KEY] = None
    if PRODUCTS_STATE_KEY not in st.session_state:
        st.session_state[PRODUCTS_STATE_KEY] = {"products": [], "error": None}

    today = date.today()
    default_start = today - timedelta(days=7)

    col_start, col_end, col_limit, col_action = st.columns([1, 1, 1, 0.8])
    start_date = col_start.date_input("Start date", value=default_start)
    end_date = col_end.date_input("End date", value=today)
    limit = col_limit.number_input(
        "Limit",
        min_value=1,
        max_value=500,
        value=DEFAULT_LIMIT,
        step=5,
        help="Maximum number of products to fetch.",
    )
    load_clicked = col_action.button("Load products", type="secondary")

    if load_clicked:
        with st.spinner("Fetching products..."):
            products, error = fetch_products_for_range(
                start_date=start_date,
                end_date=end_date,
                limit=int(limit),
            )
        st.session_state[PRODUCTS_STATE_KEY] = {"products": products, "error": error}
        st.session_state[RESULT_STATE_KEY] = None

    products_state = st.session_state[PRODUCTS_STATE_KEY]
    if products_state["error"]:
        st.error(products_state["error"])
    elif not products_state["products"]:
        st.info("Set a date range and limit, then click **Load products**.")
        return

    products: list[Product] = products_state["products"]
    selected_product = st.selectbox(
        "Choose a product",
        options=products,
        format_func=format_product_option,
    )

    metadata = build_product_metadata(selected_product)

    st.write(
        f"**Selected product:** {readable_value(selected_product.title)} — "
        f"{readable_value(selected_product.brand)}"
    )
    if not metadata:
        st.warning("This product has insufficient metadata for the agent.")

    with st.expander("Metadata sent to the agent"):
        st.json(metadata or {"info": "No metadata available"})

    with st.expander("Raw product"):
        st.json(selected_product.model_dump())

    current_product_id = selected_product.id
    previous = st.session_state.get(RESULT_STATE_KEY)
    if previous and previous.get("product_id") != current_product_id:
        st.session_state[RESULT_STATE_KEY] = None

    if st.button(
        "Generate suggestions",
        type="primary",
        disabled=not metadata,
    ):
        with st.spinner("Running agents and searching..."):
            st.session_state[RESULT_STATE_KEY] = generate_comparison(selected_product)

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

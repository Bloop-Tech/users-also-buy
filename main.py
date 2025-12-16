from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from pandas.core.internals.blocks import F

from src.agent import get_agent
from src.data_models import AlsoBuyQueries
from src.search import SearchService

DATA_PATH = Path("marketplacer-export.csv")


def main() -> None:
    load_dotenv()
    df = pd.read_csv(DATA_PATH)
    1 / 0
    agent = get_agent()
    results: dict[str, AlsoBuyQueries] = {}
    for _, raw_product in df.sample(6).iterrows():
        product_id = str(raw_product["Ad ID"])
        data = {
            "category_lvl_1": raw_product["*Category"],
            "category_lvl_2": raw_product.get("Category 2"),
            "category_lvl_3": raw_product.get("Category 3"),
            "category_lvl_4": raw_product.get("Category 4"),
            "brand": raw_product["*Brand"],
            "title": raw_product["*Title Description"],
            "description": raw_product["*Main Description"],
        }
        product_metadata = {
            column: value.strip()
            for column, value in ((c, str(v)) for c, v in data.items() if pd.notna(v))
            if value.strip()
        }
        if not product_metadata:
            continue

        result = agent.run_sync(
            f"""Suggest also-buy queries for this product:
                {json.dumps(product_metadata, indent=2)}
                """,
        )
        print(
            f"Product Title: {product_metadata['title']}, Suggested Queries: {result.output.queries}, Reasoning: {result.output.reasoninig}, Tokens: {result.usage().input_tokens}/{result.usage().output_tokens}"
        )
        results[product_id] = result.output
        print()


if __name__ == "__main__":
    main()

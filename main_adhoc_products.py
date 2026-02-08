from __future__ import annotations

import asyncio
import json
from typing import List, Tuple

from dotenv import load_dotenv

from src.agent import get_agent
from src.data_models import Product
from src.marketplacer_gateway import MarketplacerGateway

PRODUCT_IDS = [
    "R29sZGVuUHJvZHVjdC0zNjEyNDM=",
    "R29sZGVuUHJvZHVjdC00Mjc2NTg=",
    "R29sZGVuUHJvZHVjdC00NDI2ODY=",
    "R29sZGVuUHJvZHVjdC00MzIxNzA=",
    "R29sZGVuUHJvZHVjdC0yMTYxNTU="
]


async def _generate_queries_for_product(
    agent,
    product: Product,
    semaphore: asyncio.Semaphore,
) -> Tuple[Product, List[str]]:
    prompt = f"""Suggest also-buy queries for this product:
                    {json.dumps(product.metadata, indent=2)}
                    """
    async with semaphore:
        result = await agent.run(prompt)
    return product, result.output.queries


async def main() -> None:
    load_dotenv()
    marketplacer_gateway = MarketplacerGateway(page_size=15)
    agent = get_agent(generic_variant=False)
    semaphore = asyncio.Semaphore(5)

    products: list[Product] = []
    for product_id in PRODUCT_IDS:
        print(f"Fetching product {product_id}")
        product = marketplacer_gateway.fetch_product_by_id(product_id)
        if product is None:
            print(f"Product {product_id} not found, skipping.")
            continue
        products.append(product)

    if not products:
        print("No products to process.")
        return

    print(f"Processing products {[(x.id, x.title) for x in products]}")
    batch_results: list[tuple[Product, list[str]]] = await asyncio.gather(
        *[
            _generate_queries_for_product(agent, product, semaphore)
            for product in products
        ]
    )

    print(f"Saving batch of {len(batch_results)} to marketplacer:")
    for product, queries in batch_results:
        marketplacer_gateway.update_product_with_complementary_queries(
            product, queries
        )


if __name__ == "__main__":
    asyncio.run(main())

from __future__ import annotations

import asyncio
import json
import logging
from typing import List, Tuple

import httpx
from dotenv import load_dotenv

from src.agent import get_agent
from src.data_models import Product
from src.logging_config import setup_logging
from src.marketplacer_gateway import MarketplacerGateway

logger = logging.getLogger(__name__)

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
) -> Tuple[Product, List[str]] | None:
    prompt = f"""Suggest also-buy queries for this product:
                    {json.dumps(product.metadata, indent=2)}
                    """
    try:
        async with semaphore:
            result = await agent.run(prompt)
        return product, result.output.queries
    except Exception as e:
        if "content_filter" in str(e) or "content management policy" in str(e):
            logger.warning(f"Skipping product {product.id} ({product.title}) due to content filter: {e}")
            return None
        raise


async def _update_product_in_marketplacer(
    marketplacer_gateway: MarketplacerGateway,
    product: Product,
    queries: list[str],
    http_client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
) -> None:
    async with semaphore:
        await marketplacer_gateway.async_update_product_with_complementary_queries(
            product, queries, http_client
        )


async def main() -> None:
    load_dotenv()
    marketplacer_gateway = MarketplacerGateway(page_size=15)
    agent = get_agent(generic_variant=False)
    semaphore = asyncio.Semaphore(5)

    products: list[Product] = []
    for product_id in PRODUCT_IDS:
        logger.info("Fetching product %s", product_id)
        product = marketplacer_gateway.fetch_product_by_id(product_id)
        if product is None:
            logger.warning("Product %s not found, skipping.", product_id)
            continue
        products.append(product)

    if not products:
        logger.warning("No products to process.")
        return

    logger.info("Processing products %s", [(x.id, x.title) for x in products])
    raw_results = await asyncio.gather(
        *[
            _generate_queries_for_product(agent, product, semaphore)
            for product in products
        ]
    )
    batch_results: list[tuple[Product, list[str]]] = [r for r in raw_results if r is not None]

    logger.info("Saving batch of %d to marketplacer (skipped %d due to content filter)", len(batch_results), len(raw_results) - len(batch_results))
    marketplacer_semaphore = asyncio.Semaphore(6)
    async with httpx.AsyncClient() as http_client:
        await asyncio.gather(*[
            _update_product_in_marketplacer(
                marketplacer_gateway, product, queries, http_client, marketplacer_semaphore
            )
            for product, queries in batch_results
        ])


if __name__ == "__main__":
    setup_logging()
    asyncio.run(main())

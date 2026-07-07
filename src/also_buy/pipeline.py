from __future__ import annotations

import asyncio
import json
import logging
from typing import List, Tuple

import httpx
from tenacity import (
    before_sleep_log,
    retry,
    stop_after_attempt,
    wait_fixed,
)

from src.also_buy.data_models import Product
from src.also_buy.marketplacer_gateway import MarketplacerGateway

logger = logging.getLogger(__name__)


@retry(
    stop=stop_after_attempt(2),
    wait=wait_fixed(2),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
async def generate_queries_for_product(
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
            logger.warning(
                "Skipping product %s (%s) due to content filter: %s",
                product.id,
                product.title,
                e,
            )
            return None
        raise


async def update_product_in_marketplacer(
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

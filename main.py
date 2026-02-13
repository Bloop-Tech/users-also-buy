from __future__ import annotations

import asyncio
import datetime
import json
import time
from datetime import timedelta
from typing import List, Tuple

import httpx
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_fixed, before_sleep_log

import logging

from src.agent import get_agent
from src.azure_blob_client import AzureBlobClient
from src.data_models import PipelineBlobStatus, Product
from src.logging_config import setup_logging
from src.marketplacer_gateway import MarketplacerGateway


logger = logging.getLogger(__name__)


@retry(stop=stop_after_attempt(2), wait=wait_fixed(2), before_sleep=before_sleep_log(logger, logging.WARNING), reraise=True)
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
    pipeline_trigger_datetime = datetime.datetime.now(datetime.UTC)
    marketplacer_gateway = MarketplacerGateway(page_size=200)
    agent = get_agent(generic_variant=False)
    azure_blob_client = AzureBlobClient()
    product_status_file_name = 'product_status_live'
    last_pipeline_status: PipelineBlobStatus = azure_blob_client.read_json(
        product_status_file_name
    )
    logger.info("Last pipeline status: %s", last_pipeline_status)
    if last_pipeline_status is None:
        min_start_date = datetime.datetime(2023, 1, 1)
    else:
        min_start_date = (
            last_pipeline_status.latest_product_datetime_updated + timedelta(seconds=1)
        )
    semaphore = asyncio.Semaphore(10)
    marketplacer_semaphore = asyncio.Semaphore(6)
    for batch_products in marketplacer_gateway.fetch_products(
        min_start_date,
        datetime.datetime.now(datetime.UTC),
        # limit=30
    ):
        time_before = time.time()
        logger.info(
            "Processing batch of products whose dates range is: %s and %s",
            batch_products[0].created_date.isoformat(),
            batch_products[-1].created_date.isoformat(),
        )
        raw_results = await asyncio.gather(
            *[
                _generate_queries_for_product(agent, product, semaphore)
                for product in batch_products
            ]
        )
        batch_results: list[tuple[Product, list[str]]] = [r for r in raw_results if r is not None]
        logger.info(f"Took {time.time() - time_before} seconds to get openai results")
        logger.info("Saving batch of %d to marketplacer (skipped %d due to content filter)", len(batch_results), len(raw_results) - len(batch_results))
        time_before = time.time()
        async with httpx.AsyncClient() as http_client:
            await asyncio.gather(*[
                _update_product_in_marketplacer(
                    marketplacer_gateway, product, queries, http_client, marketplacer_semaphore
                )
                for product, queries in batch_results
            ])
        logger.info(f"Took {time.time() - time_before} seconds to save to marketplacer")
        time_before = time.time()
        # store in blob the latest status
        pipeline_status = PipelineBlobStatus(
            latest_product_datetime_updated=batch_products[-1].created_date,
            latest_datetime_trigger=pipeline_trigger_datetime,
        )
        azure_blob_client.write_pipeline_status(
            blob_name=product_status_file_name, pipeline_status=pipeline_status
        )
        logger.info(f"Took {time.time() - time_before} seconds to save to blob storage")


if __name__ == "__main__":
    setup_logging()
    asyncio.run(main())

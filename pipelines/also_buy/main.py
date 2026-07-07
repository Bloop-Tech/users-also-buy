from __future__ import annotations

import asyncio
import datetime
import logging
import time
from datetime import timedelta

import httpx
from dotenv import load_dotenv

from src.also_buy.agent import get_agent
from src.also_buy.data_models import PipelineBlobStatus
from src.also_buy.marketplacer_gateway import MarketplacerGateway
from src.also_buy.pipeline import (
    generate_queries_for_product,
    update_product_in_marketplacer,
)
from src.common.azure_blob_client import AzureBlobClient

logger = logging.getLogger(__name__)


async def main() -> None:
    load_dotenv()
    pipeline_trigger_datetime = datetime.datetime.now(datetime.UTC)
    marketplacer_gateway = MarketplacerGateway(page_size=200)
    agent = get_agent(generic_variant=False)
    azure_blob_client = AzureBlobClient()
    product_status_file_name = "product_status_live"
    last_pipeline_status = azure_blob_client.read_pipeline_status(
        product_status_file_name
    )
    logger.info("Last pipeline status: %s", last_pipeline_status)
    if last_pipeline_status is None:
        min_start_date = datetime.datetime(2023, 1, 1)
    else:
        min_start_date = (
            last_pipeline_status.latest_product_datetime_updated + timedelta(seconds=1)
        )
    semaphore = asyncio.Semaphore(12)
    marketplacer_semaphore = asyncio.Semaphore(8)
    for batch_products in marketplacer_gateway.fetch_products(
        min_start_date,
        datetime.datetime.now(datetime.UTC),
    ):
        time_before = time.time()
        logger.info(
            "Processing batch of products whose dates range is: %s and %s",
            batch_products[0].created_date.isoformat(),
            batch_products[-1].created_date.isoformat(),
        )
        raw_results = await asyncio.gather(
            *[
                generate_queries_for_product(agent, product, semaphore)
                for product in batch_products
            ]
        )
        batch_results = [r for r in raw_results if r is not None]
        logger.info("Took %s seconds to get openai results", time.time() - time_before)
        logger.info(
            "Saving batch of %d to marketplacer (skipped %d due to content filter)",
            len(batch_results),
            len(raw_results) - len(batch_results),
        )
        time_before = time.time()
        async with httpx.AsyncClient() as http_client:
            await asyncio.gather(
                *[
                    update_product_in_marketplacer(
                        marketplacer_gateway,
                        product,
                        queries,
                        http_client,
                        marketplacer_semaphore,
                    )
                    for product, queries in batch_results
                ]
            )
        logger.info(
            "Took %s seconds to save to marketplacer", time.time() - time_before
        )
        pipeline_status = PipelineBlobStatus(
            latest_product_datetime_updated=batch_products[-1].created_date,
            latest_datetime_trigger=pipeline_trigger_datetime,
        )
        azure_blob_client.write_pipeline_status(
            blob_name=product_status_file_name, pipeline_status=pipeline_status
        )

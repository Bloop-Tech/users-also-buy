from __future__ import annotations

import asyncio
import datetime
import json
from datetime import timedelta
from typing import List, Tuple

from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_fixed, before_sleep_log

import logging

from src.agent import get_agent
from src.azure_blob_client import AzureBlobClient
from src.data_models import PipelineBlobStatus, Product
from src.marketplacer_gateway import MarketplacerGateway


logger = logging.getLogger(__name__)


@retry(stop=stop_after_attempt(2), wait=wait_fixed(2), before_sleep=before_sleep_log(logger, logging.WARNING), reraise=True)
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
    pipeline_trigger_datetime = datetime.datetime.now(datetime.UTC)
    marketplacer_gateway = MarketplacerGateway(page_size=200)
    agent = get_agent(generic_variant=False)
    azure_blob_client = AzureBlobClient()
    product_status_file_name = 'product_status_live'
    last_pipeline_status: PipelineBlobStatus = azure_blob_client.read_json(
        product_status_file_name
    )
    print(last_pipeline_status)
    if last_pipeline_status is None:
        min_start_date = datetime.datetime(2023, 1, 1)
    else:
        min_start_date = (
            last_pipeline_status.latest_product_datetime_updated + timedelta(seconds=1)
        )
    semaphore = asyncio.Semaphore(5)
    for batch_products in marketplacer_gateway.fetch_products(
        min_start_date,
        datetime.datetime.now(datetime.UTC),
        # limit=30
    ):
        print(
            f"Processing batch of products whose dates range is: {batch_products[0].created_date.isoformat()} and {batch_products[-1].created_date.isoformat()}"
        )
        batch_results: list[tuple[Product, list[str]]] = await asyncio.gather(
            *[
                _generate_queries_for_product(agent, product, semaphore)
                for product in batch_products
            ]
        )

        print(f"Saving batch of {len(batch_results)} to marketplacer:")
        for product, queries in batch_results:
            marketplacer_gateway.update_product_with_complementary_queries(
                product, queries
            )

        # store in blob the latest status
        pipeline_status = PipelineBlobStatus(
            latest_product_datetime_updated=batch_products[-1].created_date,
            latest_datetime_trigger=pipeline_trigger_datetime,
        )
        azure_blob_client.write_pipeline_status(
            blob_name=product_status_file_name, pipeline_status=pipeline_status
        )


if __name__ == "__main__":
    asyncio.run(main())

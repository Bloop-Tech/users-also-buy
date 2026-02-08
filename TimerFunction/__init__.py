import asyncio
import logging

import azure.functions as func

from main import main as run_pipeline


async def main(mytimer: func.TimerRequest) -> None:
    if mytimer.past_due:
        logging.warning("Timer trigger is running later than scheduled.")
    await run_pipeline()

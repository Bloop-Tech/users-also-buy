from __future__ import annotations

import argparse
import asyncio
import logging

from dotenv import load_dotenv

from src.common.logging_config import setup_logging
from src.exploration_themes.data_models import ThemesPipelineRunConfig
from src.exploration_themes.pipeline import ExplorationThemesPipeline

logger = logging.getLogger(__name__)


def parse_args() -> ThemesPipelineRunConfig:
    parser = argparse.ArgumentParser(description="Exploration themes pipeline")
    parser.add_argument(
        "--mode",
        choices=["refresh", "expand"],
        required=True,
        help="refresh re-curates existing themes; expand generates new ones",
    )
    parser.add_argument(
        "--theme-ids",
        nargs="*",
        default=None,
        help="Optional subset of theme ids to process",
    )
    parser.add_argument(
        "--expand-count",
        type=int,
        default=20,
        help="Number of themes to generate in expand mode",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run without writing to yaml or Typesense",
    )
    parser.add_argument(
        "--regenerate-metadata",
        action="store_true",
        help="Reserved for future metadata regeneration",
    )
    parser.add_argument(
        "--target-version",
        type=int,
        default=None,
        help="Skip themes already at or above this version",
    )
    args = parser.parse_args()
    return ThemesPipelineRunConfig(
        mode=args.mode,
        theme_ids=args.theme_ids,
        expand_count=args.expand_count,
        dry_run=args.dry_run,
        regenerate_metadata=args.regenerate_metadata,
        target_version=args.target_version,
    )


async def main() -> None:
    load_dotenv()
    config = parse_args()
    pipeline = ExplorationThemesPipeline()
    await pipeline.run(config)


if __name__ == "__main__":
    setup_logging()
    asyncio.run(main())

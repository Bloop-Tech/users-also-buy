from __future__ import annotations

import argparse
import asyncio
import logging

from dotenv import load_dotenv
from src.exploration_themes.runtime_config import get_runtime_config

from src.common.logging_config import setup_logging
from src.exploration_themes.data_models import ThemesPipelineRunConfig
from src.exploration_themes.pipeline import ExplorationThemesPipeline

logger = logging.getLogger(__name__)

PRODUCTION_WARNING_BANNER = "!" * 80


def log_runtime_config() -> None:
    runtime_config = get_runtime_config()
    logger.info("Exploration themes runtime config:")
    logger.info("  APP_ENV=%s", runtime_config.app_env)
    logger.info("  PRODUCTS_COLLECTION=%s", runtime_config.products_collection)
    logger.info(
        "  EXPLORATION_THEMES_COLLECTION=%s",
        runtime_config.exploration_themes_collection,
    )
    logger.info("  THEMES_FILE=%s", runtime_config.themes_file)

    if runtime_config.app_env.lower() == "production":
        logger.warning(PRODUCTION_WARNING_BANNER)
        logger.warning("RUNNING EXPLORATION THEMES PIPELINE IN PRODUCTION")
        logger.warning(
            "This run will use the production products collection, themes collection, and YAML file."
        )
        logger.warning(PRODUCTION_WARNING_BANNER)


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
    log_runtime_config()
    config = parse_args()
    pipeline = ExplorationThemesPipeline()
    await pipeline.run(config)


if __name__ == "__main__":
    setup_logging()
    asyncio.run(main())

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

DEFAULT_APP_ENV = "development"
DEFAULT_PRODUCTS_COLLECTION = "products_v2"
DEFAULT_EXPLORATION_THEMES_COLLECTION = "exploration_themes"
DEFAULT_THEMES_FILE = Path("data/themes.yaml")


@dataclass(frozen=True)
class ExplorationThemesRuntimeConfig:
    app_env: str
    products_collection: str
    exploration_themes_collection: str
    themes_file: Path


def get_runtime_config() -> ExplorationThemesRuntimeConfig:
    return ExplorationThemesRuntimeConfig(
        app_env=os.getenv("APP_ENV", DEFAULT_APP_ENV),
        products_collection=os.getenv(
            "PRODUCTS_COLLECTION", DEFAULT_PRODUCTS_COLLECTION
        ),
        exploration_themes_collection=os.getenv(
            "EXPLORATION_THEMES_COLLECTION",
            DEFAULT_EXPLORATION_THEMES_COLLECTION,
        ),
        themes_file=Path(os.getenv("THEMES_FILE", str(DEFAULT_THEMES_FILE))),
    )

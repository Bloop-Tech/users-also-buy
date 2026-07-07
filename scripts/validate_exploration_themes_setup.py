#!/usr/bin/env python3
"""Validate exploration themes local setup without calling external services."""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.exploration_themes.data_models import slugify_title
from src.exploration_themes.theme_store import ThemeStore
from src.exploration_themes.typesense_writer import EXPLORATION_THEMES_SCHEMA


def main() -> None:
    store = ThemeStore()
    themes_file = store.load()
    themes = themes_file.themes

    print(f"Loaded {len(themes)} themes from {store.path}")
    for theme in themes:
        expected_id = slugify_title(theme.title_en)
        if theme.id != expected_id and not theme.id.startswith(expected_id):
            print(
                f"WARNING: theme id '{theme.id}' does not match slug of '{theme.title_en}'",
                file=sys.stderr,
            )
        print(
            f"  - {theme.id}: {theme.title_en} [{theme.status}] "
            f"products={len(theme.product_ids)} version={theme.version}"
        )

    required_env = [
        "AZURE_OPENAI_ENDPOINT",
        "AZURE_OPENAI_API_VERSION",
        "AZURE_OPENAI_API_KEY",
        "TYPESENSE_NODE_HOST",
        "TYPESENSE_API_KEY",
        "TYPESENSE_PORT",
        "EMBEDDINGS_SERVICE_URL",
    ]
    missing = [name for name in required_env if not os.getenv(name)]
    if missing:
        print("\nMissing env vars for full pipeline run:")
        for name in missing:
            print(f"  - {name}")
        print("\nLocal validation passed. Configure env vars to run expand/refresh.")
    else:
        print("\nAll required env vars are set.")

    print(f"\nTypesense collection schema fields: {len(EXPLORATION_THEMES_SCHEMA['fields'])}")
    print("Create collection with:")
    print("  uv run python scripts/create_exploration_themes_collection.py")


if __name__ == "__main__":
    main()

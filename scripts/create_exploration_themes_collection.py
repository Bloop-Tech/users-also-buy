#!/usr/bin/env python3
"""Create the exploration_themes Typesense collection if it does not exist."""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import typesense
from dotenv import load_dotenv

from src.exploration_themes.typesense_writer import EXPLORATION_THEMES_SCHEMA


def main() -> None:
    load_dotenv()
    host = os.getenv("TYPESENSE_NODE_HOST")
    port = os.getenv("TYPESENSE_PORT")
    api_key = os.getenv("TYPESENSE_API_KEY")
    collection_name = os.getenv(
        "EXPLORATION_THEMES_COLLECTION", EXPLORATION_THEMES_SCHEMA["name"]
    )

    if not all([host, port, api_key]):
        print("Missing TYPESENSE_* environment variables.", file=sys.stderr)
        sys.exit(1)

    client = typesense.Client(
        {
            "nodes": [{"host": host, "port": int(port), "protocol": "https"}],
            "api_key": api_key,
            "connection_timeout_seconds": 5,
        }
    )

    schema = dict(EXPLORATION_THEMES_SCHEMA)
    schema["name"] = collection_name

    try:
        client.collections[collection_name].retrieve()
        print(f"Collection '{collection_name}' already exists.")
    except typesense.exceptions.ObjectNotFound:
        client.collections.create(schema)
        print(f"Created collection '{collection_name}'.")


if __name__ == "__main__":
    main()

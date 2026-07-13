from __future__ import annotations

import os

from src.common.embeddings import EmbeddingsClient
from src.common.typesense_connector import BaseTypesense
from src.exploration_themes.data_models import ExplorationTheme
from src.exploration_themes.quality_gates import should_publish_to_typesense
from src.exploration_themes.runtime_config import get_runtime_config

EXPLORATION_THEMES_SCHEMA = {
    "name": "exploration_themes",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "title_en", "type": "string"},
        {"name": "title_pt", "type": "string"},
        {"name": "title_es", "type": "string"},
        {"name": "description_en", "type": "string"},
        {"name": "description_pt", "type": "string"},
        {"name": "description_es", "type": "string"},
        {"name": "tags", "type": "string[]", "facet": True},
        {"name": "category", "type": "string", "facet": True},
        {"name": "audience", "type": "string", "facet": True},
        {"name": "seasons", "type": "string[]", "facet": True},
        {"name": "search_queries", "type": "string[]"},
        {"name": "product_ids", "type": "string[]"},
        {"name": "product_count", "type": "int32"},
        {"name": "status", "type": "string", "facet": True},
        {"name": "version", "type": "int32"},
        {"name": "updated_at", "type": "int64"},
        {
            "name": "embedding",
            "type": "float[]",
            "num_dim": 768,
            "hnsw_params": {"M": 16, "ef_construction": 200},
        },
    ],
}


class ExplorationThemesWriter:
    def __init__(
        self,
        *,
        typesense: BaseTypesense | None = None,
        embeddings_client: EmbeddingsClient | None = None,
        collection_name: str | None = None,
    ) -> None:
        runtime_config = get_runtime_config()
        self.collection_name = (
            collection_name or runtime_config.exploration_themes_collection
        )
        self.typesense = typesense or BaseTypesense(
            host=str(os.getenv("TYPESENSE_NODE_HOST")),
            key=str(os.getenv("TYPESENSE_API_KEY")),
            port=str(os.getenv("TYPESENSE_PORT")),
        )
        self.embeddings_client = embeddings_client or EmbeddingsClient()

    def upsert_theme(self, theme: ExplorationTheme) -> None:
        if not should_publish_to_typesense(theme):
            return
        embedding = self.embeddings_client.embed(
            system_prompt="passage: ",
            query=theme.embedding_text(),
        )
        document = theme.to_typesense_document(embedding)
        self.typesense.upsert_document(self.collection_name, document)

    def get_theme_document(self, theme_id: str) -> dict | None:
        try:
            return self.typesense.get_document(self.collection_name, theme_id)
        except Exception:
            return None

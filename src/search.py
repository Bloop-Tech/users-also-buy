from __future__ import annotations

import os
from abc import ABC, abstractmethod
from typing import Any, Self

from src.embeddings import EmbeddingsClient
from src.typesense_connector import BaseTypesense


class SearchService(ABC):
    def __init__(
        self,
        *,
        embeddings_client: EmbeddingsClient,
        typesense_connector: BaseTypesense,
        collection_name: str = "products_v2",
        per_page: int = 30,
        result_limit: int = 10,
    ) -> None:
        self.embeddings_client = embeddings_client
        self.typesense_connector = typesense_connector
        self.collection_name = collection_name
        self.per_page = per_page
        self.result_limit = result_limit

    @classmethod
    def build(
        cls,
        *,
        collection_name: str = "products_v2",
        per_page: int = 30,
        result_limit: int = 10,
    ) -> Self:
        embeddings_client = EmbeddingsClient(
            base_url=os.getenv("EMBEDDINGS_SERVICE_URL"),
        )
        typesense_connector = BaseTypesense(
            host=str(os.getenv("TYPESENSE_NODE_HOST")),
            key=str(os.getenv("TYPESENSE_API_KEY")),
            port=str(os.getenv("TYPESENSE_PORT")),
        )
        return cls(
            embeddings_client=embeddings_client,
            typesense_connector=typesense_connector,
            collection_name=collection_name,
            per_page=per_page,
            result_limit=result_limit,
        )

    def build_search_parameters(
        self,
        *,
        csv_embeddings: str,
        search_query: str,
    ) -> dict[str, Any]:
        return {
            "query_by": "name,categories,brandName,sellerNames,navigationCategories_en,navigationCategories_pt,product_attributes",
            "exclude_fields": "embeddings,product_attributes",
            "group_by": "productId",
            "group_limit": 1,
            "prefix": False,
            "highlight_full_fields": "none",
            "highlight_fields": "none",
            "num_typos": 1,
            "enable_typos_for_numerical_tokens": False,
            "prioritize_num_matching_fields": True,
            "prioritize_exact_match": True,
            "min_len_1typo": 15,
            "facet_sample_threshold": 1000,
            "facet_sample_percent": 20,
            "enable_analytics": False,
            "vector_query": (
                f"embeddings:([{csv_embeddings}], "
                "k:100, alpha:0.8, distance_threshold:0.7)"
            ),
            "q": search_query,
            "per_page": self.per_page,
        }

    def compute_search_results(self, search_query: str) -> list[dict[str, Any]]:
        embeddings = self.embeddings_client.embed(
            system_prompt="query: ",
            query=search_query,
        )
        csv_embeddings = ",".join(f"{value:.6f}" for value in embeddings)
        search_parameters = self.build_search_parameters(
            csv_embeddings=csv_embeddings,
            search_query=search_query,
        )

        results = self.typesense_connector.get_search_results_parsed_with_groupby(
            collection_name=self.collection_name,
            search_parameters=search_parameters,
        )
        return results[: self.result_limit]

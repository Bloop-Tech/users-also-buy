from typing import Any, cast

import typesense
from typesense.types.document import SearchResponse


class BaseTypesense:
    def __init__(
        self,
        host: str,
        key: str,
        port: str,
        protocol: str = "https",
        connection_timeout_seconds: int = 2,
    ):
        self.client = typesense.Client(
            {
                "nodes": [
                    {
                        "host": host,
                        "port": int(port),
                        "protocol": protocol,
                    }
                ],
                "api_key": key,
                "connection_timeout_seconds": connection_timeout_seconds,
            }
        )

    def get_search_results(
        self, collection_name: str, search_parameters: dict[str, Any]
    ) -> SearchResponse:
        return self.client.collections[collection_name].documents.search(
            search_parameters=search_parameters  # ty:ignore[invalid-argument-type]
        )

    def upsert_document(
        self, collection_name: str, document: dict[str, Any]
    ) -> dict[str, Any]:
        return self.client.collections[collection_name].documents.upsert(document)  # ty:ignore[invalid-argument-type, invalid-return-type]

    def import_documents(
        self, collection_name: str, documents: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        return self.client.collections[collection_name].documents.import_(  # ty:ignore[invalid-argument-type, no-matching-overload, invalid-return-type]
            documents, {"action": "upsert"}
        )

    def get_document(self, collection_name: str, document_id: str) -> dict[str, Any]:
        return cast(
            dict[str, Any],
            self.client.collections[collection_name].documents[document_id].retrieve(),  # ty:ignore[invalid-argument-type]
        )

    def _parse_hit(
        self, document: dict[str, Any], hit: dict[str, Any]
    ) -> dict[str, Any]:
        return {
            "product_id": document.get("productId") or document.get("id"),
            "product_name": document.get("name"),
            "product_brand": document.get("brandName"),
            "product_categories": document.get("navigationCategories_pt")
            or document.get("categories_pt"),
            "product_description": document.get("description_pt"),
            "product_text_match": hit.get("text_match"),
            "product_vector_distance": hit.get("vector_distance"),
        }

    def get_search_results_parsed(
        self, collection_name: str, search_parameters: dict[str, Any]
    ) -> list[dict[str, Any]]:
        search_results = self.client.collections[collection_name].documents.search(
            search_parameters=search_parameters  # ty:ignore[invalid-argument-type]
        )
        cleaned_search_result = []
        for value in search_results["hits"]:
            cleaned_search_result.append(
                self._parse_hit(value["document"], value)  # ty:ignore[invalid-argument-type]
            )
        return cleaned_search_result

    def get_search_results_parsed_with_groupby(
        self, collection_name: str, search_parameters: dict[str, Any]
    ) -> list[dict[str, Any]]:
        multi_body = {
            "searches": [{"collection": collection_name, **search_parameters}]
        }
        multi_response = self.client.multi_search.perform(multi_body)  # ty:ignore[invalid-argument-type]

        if (
            not multi_response
            or "results" not in multi_response
            or not multi_response["results"]
        ):
            raise RuntimeError("Typesense multi_search returned no results payload")

        search_results = multi_response["results"][0]
        error_message = search_results.get("error")
        success_flag = search_results.get("success")
        status_code = search_results.get("code")

        if (
            error_message is not None
            or success_flag is False
            or (isinstance(status_code, int) and status_code != 200)
        ):
            raise RuntimeError(
                f"Typesense multi_search failed: code={status_code}, error={error_message}"
            )

        cleaned_search_result = []
        for group in search_results.get("grouped_hits", []):
            for hit in group.get("hits", []):
                document = hit.get("document", {})
                cleaned_search_result.append(self._parse_hit(document, hit))

        return cleaned_search_result

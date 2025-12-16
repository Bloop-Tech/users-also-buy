from typing import Any

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
                        "port": port,
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
            search_parameters=search_parameters
        )

    def get_search_results_parsed(
        self, collection_name: str, search_parameters: dict[str, Any]
    ) -> list[dict[str, Any]]:
        search_results = self.client.collections[collection_name].documents.search(
            search_parameters=search_parameters
        )
        cleaned_search_result = list()
        for value in search_results["hits"]:
            cleaned_search_result.append(
                dict(
                    product_name=value["document"]["name"],
                    product_brand=value["document"]["brandName"],
                    product_categories=value["document"]["categories_pt"],
                    product_description=value["document"]["description_pt"],
                    product_text_match=value["text_match"],
                    product_vector_distance=value["vector_distance"],
                )
            )

        return cleaned_search_result

    def get_search_results_parsed_with_groupby(
        self, collection_name: str, search_parameters: dict[str, Any]
    ) -> list[dict[str, Any]]:
        # Use POST /multi_search to avoid exceeding URL length limits with long vector queries
        multi_body = {
            "searches": [{"collection": collection_name, **search_parameters}]
        }
        multi_response = self.client.multi_search.perform(multi_body)

        # Ensure successful per-search response before parsing results
        if (
            not multi_response
            or "results" not in multi_response
            or not multi_response["results"]
        ):
            raise RuntimeError("Typesense multi_search returned no results payload")

        search_results = multi_response["results"][0]

        # Some Typesense servers embed errors per-search inside each result item
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
        print(search_results)
        for group in search_results.get("grouped_hits", []):
            for hit in group.get("hits", []):
                document = hit.get("document", {})
                cleaned_search_result.append(
                    {
                        "product_name": document.get("name"),
                        "product_brand": document.get("brandName"),
                        "product_categories": document.get("navigationCategories_pt"),
                        "product_description": document.get("description_pt"),
                        "product_text_match": hit.get("text_match"),
                        "product_vector_distance": hit.get("vector_distance"),
                    }
                )
        print([(x["product_name"], x["product_brand"]) for x in cleaned_search_result])
        return cleaned_search_result

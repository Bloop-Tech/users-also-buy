from __future__ import annotations

import os
from datetime import date, datetime
from typing import Any, Iterator

from dotenv import load_dotenv
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport

from src.data_models import Product

GOLDEN_PRODUCTS_QUERY = """
query goldenProducts($after: String, $first: Int!, $createdSince: ISO8601DateTime, $createdUntil: ISO8601DateTime) {
  goldenProducts(first: $first, after: $after, sort: [ {
        field: CREATED_AT, ordering: ASCENDING
    }], filters: {
     createdSince: $createdSince
     createdUntil: $createdUntil
  }) {
    nodes {
      active
      id
      legacyId
      title
      createdAt
      description
      brand {
        id
        name
      }
      taxon {
        id
        treeName
      }
    }
    pageInfo {
      hasNextPage
      hasPreviousPage
      endCursor
      startCursor
    }
    totalCount
  }
}
""".strip()


class ProductsFetcher:
    def __init__(
        self,
        token: str | None = None,
        page_size: int = 100,
        timeout_seconds: int = 15,
    ) -> None:
        self.endpoint = os.getenv("MARKETPLACER_URL")
        if not self.endpoint:
            raise ValueError(
                "Missing GraphQL endpoint. Set GOLDEN_PRODUCTS_ENDPOINT or pass endpoint explicitly."
            )

        self.token = os.getenv("MARKETPLACER_TOKEN")
        self.page_size = page_size
        self.timeout_seconds = timeout_seconds
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        transport = RequestsHTTPTransport(
            url=self.endpoint,
            headers=headers,
            timeout=timeout_seconds,
        )
        self._client = Client(
            transport=transport,
            fetch_schema_from_transport=False,
        )

    def fetch_products(
        self,
        min_date: datetime,
        max_date: datetime | None = None,
        limit: int | None = None,
    ) -> Iterator[list[Product]]:
        """Yield products between the given dates in batches."""
        created_since = self._normalise_date(min_date)
        created_until = self._normalise_date(max_date)
        print(created_since, created_until)
        if limit is not None and limit <= 0:
            return

        cursor: str | None = None
        remaining = limit

        while True:
            current_batch_size = (
                self.page_size if remaining is None else min(self.page_size, remaining)
            )
            payload = self._run_query(
                after=cursor,
                first=current_batch_size,
                created_since=min_date.isoformat(),
                created_until=max_date.isoformat(),
            )
            nodes = payload.get("nodes") or []
            page_info = payload.get("pageInfo") or {}

            mapped = [self._map_product(node) for node in nodes]
            if mapped:
                yield mapped

            if remaining is not None:
                remaining -= len(mapped)
                if remaining <= 0:
                    break

            if not page_info.get("hasNextPage"):
                break

            cursor = page_info.get("endCursor")
            if not cursor:
                break

    def _run_query(
        self,
        after: str | None,
        first: int,
        created_since: str,
        created_until: str,
    ) -> dict[str, Any]:
        variables = {
            "after": after,
            "first": first,
            "createdSince": created_since,
            "createdUntil": created_until,
        }
        query = gql(GOLDEN_PRODUCTS_QUERY)
        query.variable_values = variables
        result = self._client.execute(query)  # , variable_values=variables)
        products_payload = (result or {}).get("goldenProducts")
        if not isinstance(products_payload, dict):
            raise RuntimeError("GraphQL response missing 'goldenProducts' payload")
        return products_payload

    def _map_product(self, node: dict[str, Any]) -> Product:
        taxon = node.get("taxon") or {}
        tree_name = taxon.get("treeName") or ""
        brand_payload = node.get("brand") or {}
        brand_name = brand_payload.get("name") or "Unknown"

        categories = self._split_categories(tree_name)
        if categories[0] is None:
            raise ValueError("Product missing category information")
        title = node.get("title")
        if title is None:
            raise ValueError("Product missing title information")
        return Product(
            id=node.get("id"),
            created_date=node.get("createdAt"),
            category_lvl_1=categories[0],
            category_lvl_2=categories[1],
            category_lvl_3=categories[2],
            category_lvl_4=categories[3],
            brand=brand_name,
            title=title,
            description=node.get("description") or "",
        )

    @staticmethod
    def _split_categories(tree_name: str) -> list[str | None]:
        if not tree_name:
            return [None, None, None, None]

        separators = [" > ", " / ", "/", ">"]
        parts: list[str] = []
        for sep in separators:
            if sep in tree_name:
                parts = [part.strip() for part in tree_name.split(sep) if part.strip()]
                break
        if not parts:
            parts = [tree_name.strip()] if tree_name.strip() else []

        parts = parts[:4]
        while len(parts) < 4:
            parts.append(None)
        return parts

    @staticmethod
    def _normalise_date(value: str | date) -> str:
        return value.isoformat() if isinstance(value, date) else value


if __name__ == "__main__":
    load_dotenv()
    fetcher = ProductsFetcher()

    for batch in fetcher.fetch_products(
        "2025-11-01", "2025-11-05", limit=10, batch_size=5
    ):
        for product in batch:
            print(product)

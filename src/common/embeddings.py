import os
from typing import Any, List, cast

import httpx
from pydantic import BaseModel, Field

MAX_EMBED_BATCH_SIZE = 32


class EmbedRequest(BaseModel):
    input: List[str] | str = Field(..., description="Texts to embed (OpenAI-style)")


class EmbedResponse(BaseModel):
    embeddings: List[List[float]]


class EmbeddingsClient:
    """Synchronous client for embeddings (/v1/embeddings-compatible)."""

    def __init__(self, base_url: str | None = None, timeout_seconds: float = 15.0):
        self.base_url = str(os.getenv("EMBEDDINGS_SERVICE_URL"))
        self.timeout_seconds = timeout_seconds
        self._client = httpx.Client(
            base_url=self.base_url, timeout=self.timeout_seconds
        )

    def close(self) -> None:
        self._client.close()

    def embed(self, system_prompt: str, query: str) -> list[float]:
        combined = f"{system_prompt}{query}" if system_prompt else query
        payload = EmbedRequest(input=[combined]).model_dump(exclude_none=True)
        response = self._client.post("/v1/embeddings", json=payload)
        if response.is_error:
            raise ValueError(
                f"Embeddings request failed with status "
                f"{response.status_code}: {response.text}"
            )
        data = response.json()
        if isinstance(data, dict):
            if "data" in data and data["data"]:
                items = cast(list[dict[str, Any]], data["data"])
                return cast(list[float], items[0]["embedding"])
            if "embeddings" in data:
                return cast(list[list[float]], data["embeddings"])[0]
        return cast(list[list[float]], data)[0]

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []

        embeddings: list[list[float]] = []
        for start in range(0, len(texts), MAX_EMBED_BATCH_SIZE):
            chunk = texts[start : start + MAX_EMBED_BATCH_SIZE]
            payload = EmbedRequest(input=chunk).model_dump(exclude_none=True)
            response = self._client.post("/v1/embeddings", json=payload)
            if response.is_error:
                raise ValueError(
                    f"Embeddings request failed with status "
                    f"{response.status_code}: {response.text}"
                )
            data = response.json()
            if isinstance(data, dict) and "data" in data:
                items = cast(list[dict[str, Any]], data["data"])
                embeddings.extend(
                    cast(list[float], item["embedding"]) for item in items
                )
                continue
            if isinstance(data, dict) and "embeddings" in data:
                embeddings.extend(cast(list[list[float]], data["embeddings"]))
                continue
            embeddings.extend(cast(list[list[float]], data))
        return embeddings

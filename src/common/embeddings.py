import os
from typing import List

import httpx
from pydantic import BaseModel, Field


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
                return data["data"][0]["embedding"]
            if "embeddings" in data:
                return EmbedResponse(embeddings=data["embeddings"]).embeddings[0]
        return EmbedResponse(embeddings=data).embeddings[0]

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        payload = EmbedRequest(input=texts).model_dump(exclude_none=True)
        response = self._client.post("/v1/embeddings", json=payload)
        if response.is_error:
            raise ValueError(
                f"Embeddings request failed with status "
                f"{response.status_code}: {response.text}"
            )
        data = response.json()
        if isinstance(data, dict) and "data" in data:
            return [item["embedding"] for item in data["data"]]
        if isinstance(data, dict) and "embeddings" in data:
            return data["embeddings"]
        return EmbedResponse(embeddings=data).embeddings

import os
from typing import List

import httpx
from pydantic import BaseModel, Field


class EmbedRequest(BaseModel):
    input: List[str] | str = Field(..., description="Texts to embed (OpenAI-style)")


class EmbedResponse(BaseModel):
    embeddings: List[List[float]]


class EmbeddingsClient:
    """Synchronous client for embeddings (/v1/embeddings-compatible).

    Combines a system prompt and user query into a single input string and
    posts to `/v1/embeddings`.
    """

    def __init__(self, base_url: str | None = None, timeout_seconds: float = 15.0):
        self.base_url = str(os.getenv("EMBEDDINGS_SERVICE_URL"))
        self.timeout_seconds = timeout_seconds
        self._client = httpx.Client(
            base_url=self.base_url, timeout=self.timeout_seconds
        )

    def close(self) -> None:
        self._client.close()

    def embed(self, system_prompt: str, query: str) -> list[float]:
        """Return a single embedding for the combined system prompt + query."""
        combined = f"{system_prompt}{query}" if system_prompt else query
        payload = EmbedRequest(input=[combined]).model_dump(exclude_none=True)
        response = self._client.post("/v1/embeddings", json=payload)
        # Surface more context on common 4xx/5xx
        if response.is_error:
            try:
                detail = response.json()
            except Exception:
                detail = response.text

            print(detail)
            response.raise_for_status()
        data = response.json()
        if isinstance(data, dict):
            if "data" in data and data["data"]:
                return data["data"][0]["embedding"]
            if "embeddings" in data:
                return EmbedResponse(embeddings=data["embeddings"]).embeddings[0]
        return EmbedResponse(embeddings=data).embeddings[0]

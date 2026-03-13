"""Voyage AI embedding client using httpx (no SDK needed)."""

import logging

import httpx

logger = logging.getLogger(__name__)

VOYAGE_API_URL = "https://api.voyageai.com/v1/embeddings"
VOYAGE_MODEL = "voyage-3"
VOYAGE_DIMENSIONS = 1024
_MAX_BATCH = 128


async def embed_texts(
    texts: list[str],
    api_key: str,
    *,
    input_type: str = "document",
) -> list[list[float]]:
    """Embed a list of texts via Voyage AI, batching as needed.

    Args:
        texts: Strings to embed.
        api_key: Voyage AI API key.
        input_type: ``"document"`` for indexing, ``"query"`` for retrieval.

    Returns:
        List of embedding vectors (each 1024 floats).
    """
    if not texts:
        return []

    all_embeddings: list[list[float]] = []
    async with httpx.AsyncClient(timeout=60) as client:
        for start in range(0, len(texts), _MAX_BATCH):
            batch = texts[start : start + _MAX_BATCH]
            resp = await client.post(
                VOYAGE_API_URL,
                json={
                    "model": VOYAGE_MODEL,
                    "input": batch,
                    "input_type": input_type,
                },
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
            )
            resp.raise_for_status()
            data = resp.json()
            batch_embeddings = [item["embedding"] for item in data["data"]]
            all_embeddings.extend(batch_embeddings)
            logger.debug(
                "Embedded %d texts (batch %d-%d), tokens used: %s",
                len(batch),
                start,
                start + len(batch),
                data.get("usage", {}).get("total_tokens", "?"),
            )

    return all_embeddings


async def embed_query(text: str, api_key: str) -> list[float]:
    """Embed a single query for retrieval."""
    result = await embed_texts([text], api_key, input_type="query")
    return result[0]

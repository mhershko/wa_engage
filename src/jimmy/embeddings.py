"""Voyage AI embedding client using httpx (no SDK needed)."""

import asyncio
import logging

import httpx

logger = logging.getLogger(__name__)

VOYAGE_API_URL = "https://api.voyageai.com/v1/embeddings"
VOYAGE_MODEL = "voyage-3"
VOYAGE_DIMENSIONS = 1024
_MAX_BATCH = 20
_MAX_RETRIES = 5
_RETRY_BASE_DELAY = 2.0


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
            batch_embeddings = await _embed_batch_with_retry(
                client, batch, api_key, input_type
            )
            all_embeddings.extend(batch_embeddings)
            logger.info(
                "Embedded %d texts (batch %d-%d)",
                len(batch),
                start,
                start + len(batch),
            )

    return all_embeddings


async def _embed_batch_with_retry(
    client: httpx.AsyncClient,
    batch: list[str],
    api_key: str,
    input_type: str,
) -> list[list[float]]:
    """Call Voyage API with exponential backoff on 429/5xx."""
    for attempt in range(_MAX_RETRIES):
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
        if resp.status_code == 429 or resp.status_code >= 500:
            delay = _RETRY_BASE_DELAY * (2 ** attempt)
            retry_after = resp.headers.get("retry-after")
            if retry_after:
                try:
                    delay = max(delay, float(retry_after))
                except ValueError:
                    pass
            logger.warning(
                "Voyage API %d, retrying in %.1fs (attempt %d/%d)",
                resp.status_code,
                delay,
                attempt + 1,
                _MAX_RETRIES,
            )
            await asyncio.sleep(delay)
            continue
        resp.raise_for_status()
        data = resp.json()
        return [item["embedding"] for item in data["data"]]

    resp.raise_for_status()
    return []


async def embed_query(text: str, api_key: str) -> list[float]:
    """Embed a single query for retrieval."""
    result = await embed_texts([text], api_key, input_type="query")
    return result[0]

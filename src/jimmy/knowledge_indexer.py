"""Knowledge indexer: chunks Notion guide pages and stores embeddings in pgvector."""

import logging
import re
import time
from datetime import datetime, timezone
from typing import Any, Callable, Awaitable

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from jimmy.embeddings import embed_texts
from models.knowledge_chunk import KnowledgeChunk

logger = logging.getLogger(__name__)

_SENTENCE_SPLIT = re.compile(r"(?<=[.!?\n])\s+")


def chunk_text(
    text: str, max_chars: int = 500, overlap: int = 50
) -> list[str]:
    """Split text into chunks of roughly *max_chars* with *overlap*."""
    if not text.strip():
        return []
    if len(text) <= max_chars:
        return [text.strip()]

    sentences = _SENTENCE_SPLIT.split(text)
    chunks: list[str] = []
    current = ""

    for sentence in sentences:
        sentence = sentence.strip()
        if not sentence:
            continue
        if current and len(current) + len(sentence) + 1 > max_chars:
            chunks.append(current.strip())
            # Keep overlap from end of previous chunk
            if overlap > 0 and len(current) > overlap:
                current = current[-overlap:] + " " + sentence
            else:
                current = sentence
        else:
            current = f"{current} {sentence}".strip() if current else sentence

    if current.strip():
        chunks.append(current.strip())

    return chunks


async def index_pages(
    docs: list[dict[str, str]],
    session_factory: async_sessionmaker[AsyncSession],
    voyage_api_key: str,
) -> int:
    """Chunk, embed, and store all guide pages. Returns total chunk count."""
    if not docs or not voyage_api_key:
        return 0

    started = time.perf_counter()
    all_chunks: list[dict[str, Any]] = []

    for doc in docs:
        page_id = doc.get("page_id", "")
        title = doc.get("title", "")
        content = doc.get("content", "")
        if not content.strip():
            continue

        prefixed_content = f"{title}\n{content}" if title else content
        text_chunks = chunk_text(prefixed_content)
        for i, chunk in enumerate(text_chunks):
            all_chunks.append({
                "notion_page_id": page_id,
                "page_title": title,
                "chunk_index": i,
                "chunk_text": chunk,
            })

    if not all_chunks:
        return 0

    texts = [c["chunk_text"] for c in all_chunks]
    logger.info("Embedding %d chunks from %d pages...", len(texts), len(docs))
    embeddings = await embed_texts(texts, voyage_api_key)

    now = datetime.now(timezone.utc)
    async with session_factory() as session:
        page_ids = {c["notion_page_id"] for c in all_chunks}
        await session.execute(
            delete(KnowledgeChunk).where(
                KnowledgeChunk.notion_page_id.in_(page_ids)  # type: ignore[union-attr]
            )
        )

        for chunk_meta, embedding in zip(all_chunks, embeddings, strict=True):
            session.add(KnowledgeChunk(
                notion_page_id=chunk_meta["notion_page_id"],
                page_title=chunk_meta["page_title"],
                chunk_index=chunk_meta["chunk_index"],
                chunk_text=chunk_meta["chunk_text"],
                embedding=embedding,
                updated_at=now,
            ))

        await session.commit()

    elapsed_ms = (time.perf_counter() - started) * 1000
    logger.info(
        "Indexed %d chunks from %d pages in %.0fms",
        len(all_chunks),
        len(docs),
        elapsed_ms,
    )
    return len(all_chunks)


async def reindex_if_needed(
    session_factory: async_sessionmaker[AsyncSession],
    fetch_docs: Callable[[], Awaitable[list[dict[str, str]]]],
    voyage_api_key: str,
) -> int:
    """Index all pages if the knowledge_chunk table is empty."""
    async with session_factory() as session:
        result = await session.execute(
            select(func.count()).select_from(KnowledgeChunk)
        )
        count = result.scalar() or 0

    if count > 0:
        logger.info("Knowledge chunks already indexed (%d rows), skipping.", count)
        return 0

    logger.info("Knowledge chunk table is empty, triggering full index...")
    docs = await fetch_docs()
    return await index_pages(docs, session_factory, voyage_api_key)

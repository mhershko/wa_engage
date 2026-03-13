"""Background scheduler that re-indexes knowledge chunks once per day."""

import asyncio
import logging
from datetime import datetime

import zoneinfo
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession

from config import Settings
from jimmy.knowledge_indexer import index_pages
from jimmy.notion_client import NotionClient

logger = logging.getLogger(__name__)

TZ_JERUSALEM = zoneinfo.ZoneInfo("Asia/Jerusalem")
REINDEX_HOUR = 3  # 3:00 AM Israel time


class KnowledgeIndexScheduler:
    def __init__(
        self,
        settings: Settings,
        notion: NotionClient,
        session_factory: async_sessionmaker[AsyncSession],
    ):
        self._settings = settings
        self._notion = notion
        self._session_factory = session_factory
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        if not self._settings.voyage_api_key or not self._settings.notion_guides_db_id:
            logger.info("KnowledgeIndexScheduler not started: missing voyage_api_key or guides_db_id")
            return
        self._task = asyncio.create_task(self._loop())
        logger.info("KnowledgeIndexScheduler started (daily at %d:00 Israel time)", REINDEX_HOUR)

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("KnowledgeIndexScheduler stopped")

    async def _loop(self) -> None:
        while True:
            try:
                await self._sleep_until_next_run()
                await self.run_index()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Knowledge index scheduler iteration failed")
                await asyncio.sleep(3600)

    async def _sleep_until_next_run(self) -> None:
        now = datetime.now(TZ_JERUSALEM)
        target = now.replace(hour=REINDEX_HOUR, minute=0, second=0, microsecond=0)
        if now >= target:
            target = target.replace(day=target.day + 1)
        delta = (target - now).total_seconds()
        logger.info("Next knowledge re-index in %.0f seconds", delta)
        await asyncio.sleep(delta)

    async def run_index(self) -> int:
        """Run a full re-index. Called by scheduler or manually via admin command."""
        logger.info("Starting scheduled knowledge re-index...")
        try:
            docs = await self._notion.get_all_guide_documents_with_images(
                self._settings.notion_guides_db_id
            )
            count = await index_pages(
                docs, self._session_factory, self._settings.voyage_api_key
            )
            logger.info("Scheduled knowledge re-index complete: %d chunks", count)
            return count
        except Exception:
            logger.exception("Scheduled knowledge re-index failed")
            return 0

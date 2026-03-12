import asyncio
import logging
from typing import Any

from cachetools import TTLCache
from sqlmodel.ext.asyncio.session import AsyncSession

from config import Settings
from jimmy.brain import JimmyBrain
from jimmy.handler import JimmyHandler
from jimmy.notion_client import NotionClient
from models import (
    WhatsAppWebhookPayload,
)
from whatsapp import WhatsAppClient
from .base_handler import BaseHandler
from models import Message

logger = logging.getLogger(__name__)

# In-memory processing guard: 4 minutes TTL to prevent duplicate handling
_processing_cache = TTLCache(maxsize=1000, ttl=4 * 60)
_processing_lock = asyncio.Lock()


class MessageHandler(BaseHandler):
    def __init__(
        self,
        session: AsyncSession,
        whatsapp: WhatsAppClient,
        settings: Settings,
        notion: NotionClient | None = None,
        session_factory: Any | None = None,
    ):
        self.settings = settings
        self._notion = notion
        self._session_factory = session_factory
        super().__init__(session, whatsapp)

    async def __call__(self, payload: WhatsAppWebhookPayload):
        message = await self.store_message(payload)

        if not message or not message.text:
            return

        my_jid = await self.whatsapp.get_my_jid()
        if message.sender_jid == my_jid.normalize_str():
            return

        # In-memory dedupe (must run before any handling)
        if message.message_id:
            async with _processing_lock:
                if message.message_id in _processing_cache:
                    return
                _processing_cache[message.message_id] = True

        # --- Jimmy bot: handle DMs ---
        if message and not message.group:
            if self._notion:
                await self._handle_jimmy_dm(message)
            return

        # --- Jimmy bot: handle group messages (admin commands) ---
        if self._notion and message and message.group and message.text:
            jimmy = self._build_jimmy()
            try:
                await jimmy.handle_group_message(
                    message.chat_jid, message.sender_jid, message.text
                )
            except Exception:
                logger.exception("Jimmy group handler error")

    # ------------------------------------------------------------------
    # Jimmy helpers
    # ------------------------------------------------------------------

    def _build_jimmy(self) -> JimmyHandler:
        assert self._notion is not None
        brain = JimmyBrain(
            self.settings,
            self._notion,
            session_factory=self._session_factory,
        )
        return JimmyHandler(
            session=self.session,
            whatsapp=self.whatsapp,
            settings=self.settings,
            notion=self._notion,
            brain=brain,
        )

    async def _handle_jimmy_dm(self, message: Message) -> None:
        jimmy = self._build_jimmy()
        try:
            await jimmy.handle_dm(message.sender_jid, message.text or "")
        except Exception:
            logger.exception("Jimmy DM handler error for %s", message.sender_jid)

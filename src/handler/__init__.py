import asyncio
import logging

from cachetools import TTLCache
from sqlmodel.ext.asyncio.session import AsyncSession
from voyageai.client_async import AsyncClient

from config import Settings
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
        embedding_client: AsyncClient,
        settings: Settings,
    ):
        self.settings = settings
        super().__init__(session, whatsapp, embedding_client)

    async def __call__(self, payload: WhatsAppWebhookPayload):
        message = await self.store_message(payload)

        # ignore messages that don't exist or don't have text
        if not message or not message.text:
            return

        # Ignore messages sent by the bot itself
        my_jid = await self.whatsapp.get_my_jid()
        if message.sender_jid == my_jid.normalize_str():
            return

        if message.sender_jid.endswith("@lid"):
            logging.info(
                f"Received message from {message.sender_jid}: {payload.model_dump_json()}"
            )

        # direct message - simple autoreply if enabled
        if message and not message.group:
            if self.settings.dm_autoreply_enabled:
                await self.send_message(
                    message.sender_jid,
                    self.settings.dm_autoreply_message,
                    message.message_id,
                )
            return

        # In-memory dedupe: if this message is already being processed/recently processed, skip
        if message and message.message_id:
            async with _processing_lock:
                if message.message_id in _processing_cache:
                    logging.info(
                        f"Message {message.message_id} already in processing cache; skipping."
                    )
                    return
                _processing_cache[message.message_id] = True

        # ignore messages from unmanaged groups
        if message and message.group and not message.group.managed:
            return

        # Bot is mentioned - simple response
        mentioned = message.has_mentioned(my_jid)
        if mentioned:
            await self.send_message(
                message.chat_jid,
                "I'm tracking messages and reactions for monthly activity reports. No action needed! 📊",
            )
            return

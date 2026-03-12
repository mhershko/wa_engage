"""Webhook endpoint for Jimmy-specific group events (e.g. bot added to group).

The WhatsApp bridge may emit group-change events separately from normal
message webhooks. This endpoint handles those events.
"""

from typing import Annotated, Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from api.deps import get_db_async_session, get_whatsapp, get_notion, get_session_factory
from config import Settings, get_settings
from jimmy.brain import JimmyBrain
from jimmy.handler import JimmyHandler
from jimmy.notion_client import NotionClient
from sqlmodel.ext.asyncio.session import AsyncSession
from whatsapp import WhatsAppClient

router = APIRouter(prefix="/jimmy", tags=["jimmy"])


class GroupAddEvent(BaseModel):
    group_jid: str = Field(..., alias="group_jid")
    group_name: str = Field(default="", alias="group_name")
    adder_jid: Optional[str] = Field(default=None, alias="adder_jid")


@router.post("/group-add")
async def group_add(
    event: GroupAddEvent,
    session: Annotated[AsyncSession, Depends(get_db_async_session)],
    whatsapp: Annotated[WhatsAppClient, Depends(get_whatsapp)],
    settings: Annotated[Settings, Depends(get_settings)],
    notion: Annotated[NotionClient | None, Depends(get_notion)],
    session_factory: Annotated[object, Depends(get_session_factory)] = None,
) -> str:
    """Handle the bot being added to a new WhatsApp group."""
    if not notion:
        return "notion client not initialized"

    brain = JimmyBrain(settings, notion, session_factory=session_factory)
    jimmy = JimmyHandler(
        session=session,
        whatsapp=whatsapp,
        settings=settings,
        notion=notion,
        brain=brain,
    )
    await jimmy.handle_group_add(
        group_jid=event.group_jid,
        group_name=event.group_name,
        adder_jid=event.adder_jid,
    )
    return "ok"

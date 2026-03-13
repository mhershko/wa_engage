from typing import Annotated, AsyncGenerator

from fastapi import Depends, Request
from sqlmodel.ext.asyncio.session import AsyncSession

from handler import MessageHandler
from jimmy.notion_client import NotionClient
from whatsapp import WhatsAppClient
from config import Settings, get_settings


async def get_db_async_session(request: Request) -> AsyncGenerator[AsyncSession, None]:
    assert request.app.state.async_session, "AsyncSession generator not initialized"
    async with request.app.state.async_session() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


def get_whatsapp(request: Request) -> WhatsAppClient:
    assert request.app.state.whatsapp, "WhatsApp client not initialized"
    return request.app.state.whatsapp


def get_notion(request: Request) -> NotionClient | None:
    return getattr(request.app.state, "notion_client", None)


def get_session_factory(request: Request):
    return getattr(request.app.state, "async_session", None)


def get_knowledge_scheduler(request: Request):
    return getattr(request.app.state, "knowledge_scheduler", None)


async def get_handler(
    session: Annotated[AsyncSession, Depends(get_db_async_session)],
    whatsapp: Annotated[WhatsAppClient, Depends(get_whatsapp)],
    settings: Annotated[Settings, Depends(get_settings)],
    notion: Annotated[NotionClient | None, Depends(get_notion)],
    session_factory: Annotated[object, Depends(get_session_factory)] = None,
    knowledge_scheduler: Annotated[object, Depends(get_knowledge_scheduler)] = None,
) -> MessageHandler:
    return MessageHandler(
        session,
        whatsapp,
        settings,
        notion,
        session_factory=session_factory,
        knowledge_scheduler=knowledge_scheduler,
    )

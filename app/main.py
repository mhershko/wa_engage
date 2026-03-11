import asyncio
from contextlib import asynccontextmanager
from warnings import warn

from fastapi import FastAPI
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlmodel.ext.asyncio.session import AsyncSession
import logging
import logfire

from api import status, webhook, jimmy_webhook
import models  # noqa
from config import get_settings
from jimmy.notion_client import NotionClient
from jimmy.reminders import ReminderScheduler
from whatsapp import WhatsAppClient
from whatsapp.init_groups import gather_groups


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=settings.log_level,
    )

    app.state.settings = settings

    app.state.whatsapp = WhatsAppClient(
        settings.whatsapp_host,
        settings.whatsapp_basic_auth_user,
        settings.whatsapp_basic_auth_password,
    )

    if settings.db_uri.startswith("postgresql://"):
        warn("use 'postgresql+asyncpg://' instead of 'postgresql://' in db_uri")
    engine = create_async_engine(
        settings.db_uri,
        pool_size=20,
        max_overflow=40,
        pool_timeout=30,
        pool_pre_ping=True,
        pool_recycle=600,
        future=True,
    )
    if settings.logfire_token:
        logfire.instrument_sqlalchemy(engine)
    async_session = async_sessionmaker(
        engine, expire_on_commit=False, class_=AsyncSession
    )

    asyncio.create_task(gather_groups(engine, app.state.whatsapp))

    app.state.db_engine = engine
    app.state.async_session = async_session

    # --- Jimmy bot initialization ---
    reminder_scheduler: ReminderScheduler | None = None
    notion = NotionClient(api_key=settings.notion_api_key)
    app.state.notion_client = notion

    reminder_scheduler = ReminderScheduler(
        settings=settings,
        notion=notion,
        whatsapp=app.state.whatsapp,
        session_factory=async_session,
    )
    reminder_scheduler.start()

    try:
        yield
    finally:
        if reminder_scheduler:
            await reminder_scheduler.stop()
        if app.state.notion_client:
            await app.state.notion_client.close()
        await engine.dispose()


# Initialize FastAPI app
app = FastAPI(title="Jimmy Bot API", lifespan=lifespan)

if get_settings().logfire_token:
    logfire.configure()
    logfire.instrument_pydantic_ai()
    logfire.instrument_fastapi(app)
    logfire.instrument_httpx(capture_all=True)
    logfire.instrument_system_metrics()

app.include_router(webhook.router)
app.include_router(status.router)
app.include_router(jimmy_webhook.router)

if __name__ == "__main__":
    import uvicorn

    settings = get_settings()
    print(f"Running on {settings.host}:{settings.port}")

    uvicorn.run("main:app", host=settings.host, port=settings.port, reload=True)

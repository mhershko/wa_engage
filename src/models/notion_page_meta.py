from datetime import datetime, timezone
from typing import Optional

from sqlmodel import Column, DateTime, Field, SQLModel


class NotionPageMeta(SQLModel, table=True):
    __tablename__ = "notion_page_meta"

    notion_page_id: str = Field(primary_key=True, max_length=64)
    title: str = Field(default="", max_length=500)
    purpose: str = Field(default="")
    source_type: str = Field(default="other", max_length=32)
    is_auto_generated: bool = Field(default=True)
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )

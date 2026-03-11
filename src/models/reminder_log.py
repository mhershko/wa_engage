from datetime import datetime, timezone
from typing import Optional

from sqlmodel import Column, DateTime, Field, SQLModel


class ReminderLog(SQLModel, table=True):
    __tablename__ = "reminder_log"

    id: Optional[int] = Field(default=None, primary_key=True)
    leader_phone: str = Field(max_length=64, index=True)
    reminder_type: str = Field(max_length=64)
    message_sent: str
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )

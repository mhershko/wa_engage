from datetime import datetime, timezone
from typing import Optional

from sqlmodel import Column, DateTime, Field, SQLModel


class EscalatedQuestion(SQLModel, table=True):
    __tablename__ = "escalated_question"

    id: Optional[int] = Field(default=None, primary_key=True)
    leader_phone: str = Field(max_length=64, index=True)
    leader_name: Optional[str] = Field(default=None, max_length=255)
    leader_group: Optional[str] = Field(default=None, max_length=255)
    question_text: str
    intent_type: str = Field(max_length=64)
    answered: bool = Field(default=False)
    answer_text: Optional[str] = Field(default=None)
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )

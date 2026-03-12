from datetime import datetime, timezone
from typing import Optional

from sqlmodel import Column, DateTime, Field, SQLModel


class AnswerReview(SQLModel, table=True):
    __tablename__ = "answer_review"

    id: Optional[int] = Field(default=None, primary_key=True)
    review_id: str = Field(max_length=32, unique=True, index=True)
    leader_phone: str = Field(max_length=64, index=True)
    leader_jid: str = Field(max_length=255, index=True)
    leader_name: Optional[str] = Field(default=None, max_length=255)
    leader_group: Optional[str] = Field(default=None, max_length=255)
    question_text: str
    bot_answer: str
    intent_type: str = Field(max_length=64)
    status: str = Field(default="pending", max_length=32, index=True)
    corrected_answer: Optional[str] = Field(default=None)
    reviewer_jid: Optional[str] = Field(default=None, max_length=255)
    notion_writeback_ok: Optional[bool] = Field(default=None)
    notion_writeback_error: Optional[str] = Field(default=None)
    reviewed_at: Optional[datetime] = Field(
        default=None,
        sa_column=Column(DateTime(timezone=True), nullable=True),
    )
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )

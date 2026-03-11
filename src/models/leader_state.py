import enum
from datetime import datetime, timezone
from typing import Optional

from sqlmodel import Column, DateTime, Enum, Field, SQLModel


class OnboardingStage(str, enum.Enum):
    NEW = "new"
    ONBOARDED = "onboarded"


class LeaderState(SQLModel, table=True):
    __tablename__ = "leader_state"

    id: Optional[int] = Field(default=None, primary_key=True)
    leader_phone: str = Field(max_length=64, unique=True, index=True)
    onboarding_stage: OnboardingStage = Field(
        default=OnboardingStage.NEW,
        sa_column=Column(
            Enum(OnboardingStage, values_callable=lambda e: [m.value for m in e], create_type=False),
            nullable=False,
            default="new",
        ),
    )
    group_id: Optional[str] = Field(default=None, max_length=255)
    group_name: Optional[str] = Field(default=None, max_length=255)
    group_approved: bool = Field(default=False)
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )

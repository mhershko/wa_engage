"""add jimmy tables (leader_state, escalated_question, reminder_log)

Revision ID: d3e4f5g6h7i8
Revises: c1d2e3f4g5h6
Create Date: 2026-02-21

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "d3e4f5g6h7i8"
down_revision: Union[str, None] = "c1d2e3f4g5h6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE TYPE onboardingstage AS ENUM ('new', 'onboarded')")

    op.create_table(
        "leader_state",
        sa.Column("id", sa.Integer(), autoincrement=True, primary_key=True),
        sa.Column("leader_phone", sa.String(64), nullable=False),
        sa.Column(
            "onboarding_stage",
            postgresql.ENUM("new", "onboarded", name="onboardingstage", create_type=False),
            nullable=False,
            server_default="new",
        ),
        sa.Column("group_id", sa.String(255), nullable=True),
        sa.Column("group_name", sa.String(255), nullable=True),
        sa.Column("group_approved", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index("ix_leader_state_leader_phone", "leader_state", ["leader_phone"], unique=True)

    op.create_table(
        "escalated_question",
        sa.Column("id", sa.Integer(), autoincrement=True, primary_key=True),
        sa.Column("leader_phone", sa.String(64), nullable=False),
        sa.Column("leader_name", sa.String(255), nullable=True),
        sa.Column("leader_group", sa.String(255), nullable=True),
        sa.Column("question_text", sa.Text(), nullable=False),
        sa.Column("intent_type", sa.String(64), nullable=False),
        sa.Column("answered", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("answer_text", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index("ix_escalated_question_leader_phone", "escalated_question", ["leader_phone"])

    op.create_table(
        "reminder_log",
        sa.Column("id", sa.Integer(), autoincrement=True, primary_key=True),
        sa.Column("leader_phone", sa.String(64), nullable=False),
        sa.Column("reminder_type", sa.String(64), nullable=False),
        sa.Column("message_sent", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index("ix_reminder_log_leader_phone", "reminder_log", ["leader_phone"])


def downgrade() -> None:
    op.drop_table("reminder_log")
    op.drop_table("escalated_question")
    op.drop_table("leader_state")
    sa.Enum(name="onboardingstage").drop(op.get_bind(), checkfirst=True)

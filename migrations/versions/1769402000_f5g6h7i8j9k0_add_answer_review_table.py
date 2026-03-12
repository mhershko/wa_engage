"""add answer_review table

Revision ID: f5g6h7i8j9k0
Revises: e4f5g6h7i8j9
Create Date: 2026-03-12

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "f5g6h7i8j9k0"
down_revision: Union[str, None] = "e4f5g6h7i8j9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "answer_review",
        sa.Column("id", sa.Integer(), autoincrement=True, primary_key=True),
        sa.Column("review_id", sa.String(length=32), nullable=False),
        sa.Column("leader_phone", sa.String(length=64), nullable=False),
        sa.Column("leader_jid", sa.String(length=255), nullable=False),
        sa.Column("leader_name", sa.String(length=255), nullable=True),
        sa.Column("leader_group", sa.String(length=255), nullable=True),
        sa.Column("question_text", sa.Text(), nullable=False),
        sa.Column("bot_answer", sa.Text(), nullable=False),
        sa.Column("intent_type", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="pending"),
        sa.Column("corrected_answer", sa.Text(), nullable=True),
        sa.Column("reviewer_jid", sa.String(length=255), nullable=True),
        sa.Column("notion_writeback_ok", sa.Boolean(), nullable=True),
        sa.Column("notion_writeback_error", sa.Text(), nullable=True),
        sa.Column("reviewed_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
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
    op.create_index("ix_answer_review_review_id", "answer_review", ["review_id"], unique=True)
    op.create_index("ix_answer_review_leader_phone", "answer_review", ["leader_phone"])
    op.create_index("ix_answer_review_leader_jid", "answer_review", ["leader_jid"])
    op.create_index("ix_answer_review_status", "answer_review", ["status"])


def downgrade() -> None:
    op.drop_index("ix_answer_review_status", table_name="answer_review")
    op.drop_index("ix_answer_review_leader_jid", table_name="answer_review")
    op.drop_index("ix_answer_review_leader_phone", table_name="answer_review")
    op.drop_index("ix_answer_review_review_id", table_name="answer_review")
    op.drop_table("answer_review")

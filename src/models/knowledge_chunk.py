from datetime import datetime, timezone

from pgvector.sqlalchemy import Vector
from sqlalchemy import Column, DateTime, Index
from sqlmodel import Field, SQLModel


class KnowledgeChunk(SQLModel, table=True):
    __tablename__ = "knowledge_chunk"

    id: int | None = Field(default=None, primary_key=True)
    notion_page_id: str = Field(max_length=64, index=True)
    page_title: str = Field(default="", max_length=500)
    chunk_index: int = Field(default=0)
    chunk_text: str = Field(default="")
    embedding: list[float] | None = Field(
        default=None,
        sa_column=Column(Vector(1024)),
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        sa_column=Column(DateTime(timezone=True), nullable=False),
    )

    __table_args__ = (
        Index(
            "ix_knowledge_chunk_embedding",
            Column("embedding", Vector(1024)),
            postgresql_using="hnsw",
            postgresql_with={"m": 16, "ef_construction": 64},
            postgresql_ops={"embedding": "vector_cosine_ops"},
        ),
    )

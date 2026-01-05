import logging
import os
import re

from langchain_postgres import PGVector
from langchain_core.embeddings import Embeddings
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

from src.storage.vectorstore.base import ContentType
from src.storage.models import EmbeddingRecord

logger = logging.getLogger(__name__)


class VectorStoreManager:
    """Manages LangChain PGVectorStore instances per embedding model."""

    def __init__(
        self,
        embeddings: Embeddings,
        model_name: str,
        dimensions: int,
        session: Session,
    ):
        self.embeddings = embeddings
        self.model_name = model_name
        self.dimensions = dimensions
        self.session = session

        # Get database URL and convert to psycopg format
        db_url = os.getenv("DATABASE_URL")
        if db_url.startswith("postgresql://"):
            db_url = db_url.replace("postgresql://", "postgresql+psycopg://", 1)
        self.db_url = db_url

        # LangChain PGVector
        self.table_name = self._model_to_table_name(model_name, dimensions)
        self.vector_store = PGVector(
            embeddings=embeddings,
            collection_name=self.table_name,
            connection=db_url,
            use_jsonb=True,
        )

        logger.info(f"Initialized VectorStoreManager with table: {self.table_name}")

    def _model_to_table_name(self, model_name: str, dimensions: int) -> str:
        """Convert model name + dimensions to a valid table name."""
        sanitized = re.sub(r"[^a-z0-9]+", "_", model_name.lower()).strip("_")
        return f"embeddings_{sanitized}_{dimensions}"

    def _get_existing_ids(self, content_ids: list[str]) -> set[str]:
        """Get IDs that are already embedded in this collection."""
        if not content_ids:
            return set()

        existing = self.session.query(EmbeddingRecord.content_id).filter(
            EmbeddingRecord.content_id.in_(content_ids),
            EmbeddingRecord.collection_name == self.table_name,
        ).all()

        return {r.content_id for r in existing}

    def _record_embeddings(
        self,
        content_ids: list[str],
        content_types: list[ContentType],
        username: str,
        subreddit: str = "",
    ) -> None:
        """Record that content has been embedded using bulk upsert."""
        if not content_ids:
            return

        records = [
            {
                "content_id": cid,
                "collection_name": self.table_name,
                "content_type": ctype.value,
                "username": username,
                "subreddit": subreddit,
            }
            for cid, ctype in zip(content_ids, content_types)
        ]

        stmt = insert(EmbeddingRecord).values(records)
        stmt = stmt.on_conflict_do_nothing(
            index_elements=["content_id", "collection_name"]
        )
        self.session.execute(stmt)
        self.session.commit()

    def add(
        self,
        content_ids: list[str],
        content_types: list[ContentType],
        texts: list[str],
        timestamps: list[int | None],
        username: str | None = None,
        subreddit: str | None = None,
    ) -> dict:
        """Add documents to the vector store with deduplication.

        Args:
            content_ids: Unique IDs for each document
            content_types: Type of content (submission/comment)
            texts: Document texts to embed
            timestamps: Unix timestamps (created_utc) for each document
            username: Username who authored the content
            subreddit: Subreddit the content belongs to
        """
        if not texts:
            return {"num_added": 0, "num_skipped": 0}

        # Find which IDs are already embedded
        existing_ids = self._get_existing_ids(content_ids)

        # Filter to only new content
        new_items = [
            (cid, ctype, text, ts)
            for cid, ctype, text, ts in zip(content_ids, content_types, texts, timestamps)
            if cid not in existing_ids
        ]

        num_skipped = len(content_ids) - len(new_items)

        if not new_items:
            logger.info(f"All {num_skipped} documents already embedded, skipping")
            return {"num_added": 0, "num_skipped": num_skipped}

        # Prepare data for PGVector
        new_ids = [item[0] for item in new_items]
        new_types = [item[1] for item in new_items]
        new_texts = [item[2] for item in new_items]
        new_timestamps = [item[3] for item in new_items]
        metadatas = [
            {
                "content_id": cid,
                "content_type": ctype.value,
                "username": username or "",
                "subreddit": subreddit or "",
                "created_utc": ts or 0,
            }
            for cid, ctype, ts in zip(new_ids, new_types, new_timestamps)
        ]

        # Add to PGVector directly
        self.vector_store.add_texts(
            texts=new_texts,
            metadatas=metadatas,
            ids=new_ids,
        )

        # Record successful embeddings
        self._record_embeddings(new_ids, new_types, username or "", subreddit or "")

        logger.info(
            f"Embedding complete: added={len(new_ids)}, skipped={num_skipped}"
        )
        return {"num_added": len(new_ids), "num_skipped": num_skipped}

    def as_retriever(self, **kwargs):
        """Get a LangChain retriever for dense search."""
        return self.vector_store.as_retriever(**kwargs)

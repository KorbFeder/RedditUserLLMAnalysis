import logging
import os
import re

from langchain_postgres import PGVector
from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_classic.indexes import SQLRecordManager, index

from src.storage.vectorstore.base import ContentType

logger = logging.getLogger(__name__)


class VectorStoreManager:
    """Manages LangChain PGVectorStore instances per embedding model."""

    def __init__(
        self,
        embeddings: Embeddings,
        model_name: str,
        dimensions: int,
    ):
        self.embeddings = embeddings
        self.model_name = model_name
        self.dimensions = dimensions

        # Get database URL and convert to psycopg format
        db_url = os.getenv("DATABASE_URL")
        if db_url.startswith("postgresql://"):
            db_url = db_url.replace("postgresql://", "postgresql+psycopg://", 1)

        # LangChain PGVector
        self.table_name = self._model_to_table_name(model_name, dimensions)
        self.vector_store = PGVector(
            embeddings=embeddings,
            collection_name=self.table_name,
            connection=db_url,
            use_jsonb=True,
        )

        # Record manager for deduplication (uses same database)
        self.record_manager = SQLRecordManager(
            namespace=f"pgvector/{self.table_name}",
            db_url=db_url,
        )
        self.record_manager.create_schema()

        logger.info(f"Initialized VectorStoreManager with table: {self.table_name}")

    def _model_to_table_name(self, model_name: str, dimensions: int) -> str:
        """Convert model name + dimensions to a valid table name."""
        sanitized = re.sub(r"[^a-z0-9]+", "_", model_name.lower()).strip("_")
        return f"embeddings_{sanitized}_{dimensions}"

    def add(
        self,
        content_ids: list[str],
        content_types: list[ContentType],
        texts: list[str],
        username: str | None = None,
    ) -> dict:
        """Add documents to the vector store with automatic deduplication."""
        if not texts:
            return {"num_added": 0, "num_skipped": 0, "num_updated": 0, "num_deleted": 0}

        documents = [
            Document(
                page_content=text,
                metadata={
                    "content_id": cid,
                    "content_type": ctype.value,
                    "username": username or "",
                },
            )
            for cid, ctype, text in zip(content_ids, content_types, texts)
        ]

        # Use indexing API for automatic deduplication
        result = index(
            documents,
            self.record_manager,
            self.vector_store,
            cleanup=None,  # Don't delete, just dedupe
            source_id_key="content_id",
        )

        logger.info(
            f"Indexing complete: added={result['num_added']}, "
            f"skipped={result['num_skipped']}, updated={result['num_updated']}"
        )
        return result

    def as_retriever(self, **kwargs):
        """Get a LangChain retriever for dense search."""
        return self.vector_store.as_retriever(**kwargs)

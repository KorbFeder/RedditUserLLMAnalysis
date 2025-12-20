import logging
import os
import re

from langchain_postgres import PGVector
from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings

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
        table_name = self._model_to_table_name(model_name, dimensions)
        self.vector_store = PGVector(
            embeddings=embeddings,
            collection_name=table_name,
            connection=db_url,
            use_jsonb=True,
        )
        logger.info(f"Initialized VectorStoreManager with table: {table_name}")

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
    ) -> int:
        """Add documents to the vector store."""
        if not texts:
            return 0

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

        self.vector_store.add_documents(documents)
        logger.info(f"Added {len(texts)} embeddings to vector store")
        return len(texts)

    def as_retriever(self, **kwargs):
        """Get a LangChain retriever for dense search."""
        return self.vector_store.as_retriever(**kwargs)

    def get_existing_ids(
        self, content_ids: list[str], content_type: ContentType
    ) -> set[str]:
        """Return content_ids that already have embeddings for the given type."""
        if not content_ids:
            return set()

        # Search for documents with these content_ids
        # This is a limitation - we do a similarity search with empty query
        # and filter by metadata. For better performance, direct SQL would be needed.
        existing = set()
        for cid in content_ids:
            results = self.vector_store.similarity_search(
                "",
                k=1,
                filter={"content_id": cid, "content_type": content_type.value},
            )
            if results:
                existing.add(cid)

        logger.debug(
            f"Found {len(existing)}/{len(content_ids)} existing {content_type.value} embeddings"
        )
        return existing

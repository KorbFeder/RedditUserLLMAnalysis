import logging
import os
from sqlalchemy import create_engine, select, text, func, literal_column
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from fastembed import TextEmbedding

from src.storage.models import Embedding, Submission, Comment
from src.storage.vectorstore.base import SearchResult, ContentType

logger = logging.getLogger(__name__)


class PgVectorStore:
    def __init__(self: "PgVectorStore", config: dict, session=None):
        if session:
            self.session = session
            self._owns_session = False
        else:
            engine = create_engine(os.getenv('DATABASE_URL'))
            Session = sessionmaker(bind=engine)
            self.session = Session()
            self._owns_session = True
        self.model = TextEmbedding(config["embedding"]["model_name"])
        self.document_prefix = config["embedding"]["document_prefix"]
        self.query_prefix = config["embedding"]["query_prefix"]
        self.encode_batch_size = config["embedding"].get("encode_batch_size", 32)
        search_config = config.get("search", {})
        self.dense_limit = search_config.get("dense", {}).get("limit", 10)
        self.sparse_limit = search_config.get("sparse", {}).get("limit", 10)

    def _embed(self: "PgVectorStore", texts: list[str], prefix: str = "") -> list[list[float]]:
        prefixed = [f"{prefix}{t}" for t in texts] if prefix else texts
        embeddings = list(self.model.embed(prefixed))
        return [emb.tolist() for emb in embeddings]

    def add(self: "PgVectorStore", content_ids: list[str], content_types: list[ContentType], texts: list[str]) -> int:
        if not texts:
            return 0

        embeddings = self._embed(texts, prefix=self.document_prefix)

        values = [
            {
                "content_id": cid,
                "content_type": ctype.value,
                "embedding": emb
            }
            for cid, ctype, emb in zip(content_ids, content_types, embeddings)
        ]

        stmt = insert(Embedding).values(values).on_conflict_do_update(
            index_elements=['content_id', 'content_type'],
            set_={'embedding': insert(Embedding).excluded.embedding}
        )

        self.session.execute(stmt)
        self.session.commit()
        logger.info(f"Added {len(texts)} embeddings")
        return len(texts)

    def dense_search(self, query_text: str, username: str | None = None, limit: int | None = None) -> list[SearchResult]:
        limit = limit or self.dense_limit
        query_embedding = self._embed([query_text], prefix=self.query_prefix)[0]

        if username:
            # JOIN to filter by author
            submission_query = (
                select(
                    Embedding.content_id,
                    Embedding.content_type,
                    Embedding.embedding.cosine_distance(query_embedding).label('distance')
                )
                .join(Submission, Embedding.content_id == Submission.id)
                .where(Embedding.content_type == ContentType.SUBMISSION.value)
                .where(Submission.author == username)
            )
            comment_query = (
                select(
                    Embedding.content_id,
                    Embedding.content_type,
                    Embedding.embedding.cosine_distance(query_embedding).label('distance')
                )
                .join(Comment, Embedding.content_id == Comment.id)
                .where(Embedding.content_type == ContentType.COMMENT.value)
                .where(Comment.author == username)
            )
            query = submission_query.union_all(comment_query).order_by(text('distance')).limit(limit)
        else:
            query = (
                select(Embedding.content_id, Embedding.content_type)
                .order_by(Embedding.embedding.cosine_distance(query_embedding))
                .limit(limit)
            )

        results = self.session.execute(query).fetchall()

        return [
            SearchResult(
                content_id=row.content_id,
                content_type=ContentType(row.content_type),
                rank=rank
            )
            for rank, row in enumerate(results, start=1)
        ]

    def sparse_search(self, query_text: str, username: str | None = None, limit: int | None = None) -> list[SearchResult]:
        limit = limit or self.sparse_limit
        ts_query = func.websearch_to_tsquery('english', query_text)

        submission_query = (
            select(
                Submission.id.label('content_id'),
                literal_column("'submission'").label('content_type'),
                func.ts_rank(Submission.search_vector, ts_query).label('score')
            )
            .where(Submission.search_vector.op('@@')(ts_query))
        )

        comment_query = (
            select(
                Comment.id.label('content_id'),
                literal_column("'comment'").label('content_type'),
                func.ts_rank(Comment.search_vector, ts_query).label('score')
            )
            .where(Comment.search_vector.op('@@')(ts_query))
        )

        if username:
            submission_query = submission_query.where(Submission.author == username)
            comment_query = comment_query.where(Comment.author == username)

        combined = submission_query.union_all(comment_query).order_by(text('score DESC')).limit(limit)
        results = self.session.execute(combined).fetchall()

        return [
            SearchResult(
                content_id=row.content_id,
                content_type=ContentType(row.content_type),
                rank=rank
            )
            for rank, row in enumerate(results, start=1)
        ]

    def get_existing_ids(self, content_ids: list[str], content_type: ContentType) -> set[str]:
        """Return content_ids that already have embeddings for the given type."""
        if not content_ids:
            return set()

        query = (
            select(Embedding.content_id)
            .where(Embedding.content_id.in_(content_ids))
            .where(Embedding.content_type == content_type.value)
        )
        results = self.session.execute(query).scalars().all()
        logger.debug(f"Found {len(results)}/{len(content_ids)} existing {content_type.value} embeddings")
        return set(results)

    def close(self):
        if self._owns_session:
            self.session.close()
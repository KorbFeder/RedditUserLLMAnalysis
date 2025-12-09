import logging
from sqlalchemy import create_engine, select, text, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from sentence_transformers import SentenceTransformer

from src.storage.models import Embedding, Submission, Comment
from src.storage.vectorstore.base import SearchResult, ContentType

logger = logging.getLogger(__name__)


class PgVectorStore:
    def __init__(
        self,
        connection_string: str,
        model_name: str,
        document_prefix: str = "",
        query_prefix: str = ""
    ):
        engine = create_engine(connection_string)
        Session = sessionmaker(bind=engine)
        self.session = Session()
        self.model = SentenceTransformer(model_name, trust_remote_code=True)
        self.document_prefix = document_prefix
        self.query_prefix = query_prefix

    def _embed(self, texts: list[str], prefix: str = "") -> list[list[float]]:
        prefixed = [f"{prefix}{t}" for t in texts]
        embeddings = self.model.encode(prefixed)
        return embeddings.tolist()

    def add(self, content_ids: list[str], content_types: list[ContentType], texts: list[str]) -> None:
        if not texts:
            return

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

    def dense_search(self, query_text: str, limit: int = 10) -> list[SearchResult]:
        query_embedding = self._embed([query_text], prefix=self.query_prefix)[0]

        query = (
            select(
                Embedding.content_id,
                Embedding.content_type,
                Embedding.embedding.cosine_distance(query_embedding).label('score')
            )
            .order_by('score')
            .limit(limit)
        )

        results = self.session.execute(query).fetchall()

        return [
            SearchResult(
                content_id=row.content_id,
                content_type=ContentType(row.content_type),
                score=row.score
            )
            for row in results
        ]

    def sparse_search(self, query_text: str, limit: int = 10) -> list[SearchResult] | None:
        ts_query = func.websearch_to_tsquery('english', query_text)

        submission_query = (
            select(
                Submission.id.label('content_id'),
                text("'submission'").label('content_type'),
                func.ts_rank(Submission.search_vector, ts_query).label('score')
            )
            .where(Submission.search_vector.op('@@')(ts_query))
        )

        comment_query = (
            select(
                Comment.id.label('content_id'),
                text("'comment'").label('content_type'),
                func.ts_rank(Comment.search_vector, ts_query).label('score')
            )
            .where(Comment.search_vector.op('@@')(ts_query))
        )

        combined = submission_query.union_all(comment_query).order_by(text('score DESC')).limit(limit)
        results = self.session.execute(combined).fetchall()

        return [
            SearchResult(
                content_id=row.content_id,
                content_type=ContentType(row.content_type),
                score=row.score
            )
            for row in results
        ]

    def close(self):
        self.session.close()
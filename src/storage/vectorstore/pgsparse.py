from typing import Any

from langchain_core.callbacks import CallbackManagerForRetrieverRun
from langchain_core.retrievers import BaseRetriever
from langchain_core.documents import Document
from pydantic import ConfigDict
from sqlalchemy import select, text, func, literal_column

from src.storage.models import Submission, Comment


class PgSparseRetriever(BaseRetriever):
    """PostgreSQL full-text search retriever using tsvector/tsquery."""

    session: Any = None
    username: str | None = None
    k: int = 20
    start_time: int | None = None
    end_time: int | None = None

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def _get_relevant_documents(
        self, query: str, *, run_manager: CallbackManagerForRetrieverRun
    ) -> list[Document]:
        ts_query = func.websearch_to_tsquery("english", query)

        submission_query = (
            select(
                Submission.id.label("content_id"),
                literal_column("'submission'").label("content_type"),
                func.ts_rank(Submission.search_vector, ts_query).label("score"),
                Submission.title,
                Submission.selftext,
            ).where(Submission.search_vector.op("@@")(ts_query))
        )

        comment_query = (
            select(
                Comment.id.label("content_id"),
                literal_column("'comment'").label("content_type"),
                func.ts_rank(Comment.search_vector, ts_query).label("score"),
                literal_column("NULL").label("title"),
                Comment.body.label("selftext"),
            ).where(Comment.search_vector.op("@@")(ts_query))
        )

        if self.username:
            submission_query = submission_query.where(Submission.author == self.username)
            comment_query = comment_query.where(Comment.author == self.username)

        # Time filtering
        if self.start_time:
            submission_query = submission_query.where(Submission.created_utc >= self.start_time)
            comment_query = comment_query.where(Comment.created_utc >= self.start_time)
        if self.end_time:
            submission_query = submission_query.where(Submission.created_utc <= self.end_time)
            comment_query = comment_query.where(Comment.created_utc <= self.end_time)

        combined = (
            submission_query.union_all(comment_query)
            .order_by(text("score DESC"))
            .limit(self.k)
        )
        results = self.session.execute(combined).fetchall()

        return [
            Document(
                page_content=row.selftext or row.title or "",
                metadata={
                    "content_id": row.content_id,
                    "content_type": row.content_type,
                },
            )
            for row in results
        ]

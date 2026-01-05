from datetime import datetime

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, MappedAsDataclass, relationship
from sqlalchemy.dialects.postgresql import JSONB, TSVECTOR
from sqlalchemy import func, ForeignKey, Computed
class Base(MappedAsDataclass, DeclarativeBase):
    pass

class Submission(Base):
    __tablename__ = 'submissions'
    id: Mapped[str] = mapped_column(primary_key=True)
    raw_json: Mapped[dict] = mapped_column(JSONB)

    author: Mapped[str | None] = mapped_column(default=None)
    subreddit: Mapped[str | None] = mapped_column(default=None)
    title: Mapped[str | None] = mapped_column(default=None)
    selftext: Mapped[str | None] = mapped_column(default=None)
    url: Mapped[str | None] = mapped_column(default=None)
    score: Mapped[int | None] = mapped_column(default=None)
    ups: Mapped[int | None] = mapped_column(default=None)
    upvote_ratio: Mapped[float | None] = mapped_column(default=None)
    num_comments: Mapped[int | None] = mapped_column(default=None)
    gilded: Mapped[int | None] = mapped_column(default=None)
    all_awardings: Mapped[list | None] = mapped_column(JSONB, default=None)
    created_utc: Mapped[int | None] = mapped_column(default=None)

    # Source tracking: is_deleted=True means Reddit doesn't have it (deleted)
    # is_archived=False means PullPush doesn't have it yet (too new)
    is_deleted: Mapped[bool] = mapped_column(default=False)
    is_archived: Mapped[bool] = mapped_column(default=True)

    fetched_at: Mapped[datetime] = mapped_column(default=func.now(), init=False)

    # Full-text search vector (generated column in DB - excluded from INSERT/UPDATE)
    search_vector = mapped_column(
        TSVECTOR,
        Computed("to_tsvector('english', coalesce(title, '') || ' ' || coalesce(selftext, ''))"),
        init=False
    )

class Comment(Base):
    __tablename__ = 'comments'
    id: Mapped[str] = mapped_column(primary_key=True)
    raw_json: Mapped[dict] = mapped_column(JSONB)

    submission_id: Mapped[str | None] = mapped_column(default=None)
    parent_id: Mapped[str | None] = mapped_column(default=None)
    author: Mapped[str | None] = mapped_column(default=None)
    subreddit: Mapped[str | None] = mapped_column(default=None)
    body: Mapped[str | None] = mapped_column(default=None)
    score: Mapped[int | None] = mapped_column(default=None)
    ups: Mapped[int | None] = mapped_column(default=None)
    gilded: Mapped[int | None] = mapped_column(default=None)
    all_awardings: Mapped[list | None] = mapped_column(JSONB, default=None)
    created_utc: Mapped[int | None] = mapped_column(default=None)

    # Source tracking: is_deleted=True means Reddit doesn't have it (deleted)
    # is_archived=False means PullPush doesn't have it yet (too new)
    is_deleted: Mapped[bool] = mapped_column(default=False)
    is_archived: Mapped[bool] = mapped_column(default=True)

    fetched_at: Mapped[datetime] = mapped_column(default=func.now(), init=False)

    # Full-text search vector (generated column in DB - excluded from INSERT/UPDATE)
    search_vector = mapped_column(
        TSVECTOR,
        Computed("to_tsvector('english', coalesce(body, ''))"),
        init=False
    )

class Job(Base):
    __tablename__ = 'jobs'

    id: Mapped[str] = mapped_column(primary_key=True)
    job_type: Mapped[str]
    status: Mapped[str] = mapped_column(default="pending")
    result: Mapped[str | None] = mapped_column(default=None)
    error: Mapped[str | None] = mapped_column(default=None)

    updated_at: Mapped[datetime] = mapped_column(default=func.now(), onupdate=func.now(), init=False)
    created_utc: Mapped[datetime] = mapped_column(default=func.now(), init=False)

    user_sentiment_job: Mapped["UserSentimentJob | None"] = relationship(
        default=None, init=False, lazy="joined"
    )

class UserSentimentJob(Base):
    __tablename__ = 'user_sentiment_jobs'

    job_id: Mapped[str] = mapped_column(ForeignKey('jobs.id'), primary_key=True)
    username: Mapped[str]
    question: Mapped[str]
    service: Mapped[str] = mapped_column(default="pending")

class UserContributionCacheStatus(Base):
    __tablename__ = 'user_contribution_cache_status'
    username: Mapped[str] = mapped_column(primary_key=True)
    newest_submission_cursor: Mapped[int | None] = mapped_column(default=None)
    newest_comment_cursor: Mapped[int | None] = mapped_column(default=None)


class SubredditContributionCacheStatus(Base):
    __tablename__ = 'subreddit_contribution_cache_status'
    subreddit: Mapped[str] = mapped_column(primary_key=True)
    newest_submission_cursor: Mapped[int | None] = mapped_column(default=None)
    newest_comment_cursor: Mapped[int | None] = mapped_column(default=None)


class EmbeddingRecord(Base):
    """Tracks which content has been embedded in each vector collection."""
    __tablename__ = 'embedding_records'

    content_id: Mapped[str] = mapped_column(primary_key=True)
    collection_name: Mapped[str] = mapped_column(primary_key=True)
    content_type: Mapped[str]  # 'submission' or 'comment'
    username: Mapped[str]
    subreddit: Mapped[str] = mapped_column(default="")
    embedded_at: Mapped[datetime] = mapped_column(default=func.now(), init=False)

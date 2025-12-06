from datetime import datetime

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, MappedAsDataclass
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import func

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

    fetched_at: Mapped[datetime] = mapped_column(default=func.now(), init=False)

class Comment(Base):
    __tablename__ = 'comments'
    id: Mapped[str] = mapped_column(primary_key=True)
    raw_json: Mapped[dict] = mapped_column(JSONB)

    submission_id: Mapped[str | None] = mapped_column(default=None)
    parent_id: Mapped[str | None] = mapped_column(default=None)
    author: Mapped[str | None] = mapped_column(default=None)
    body: Mapped[str | None] = mapped_column(default=None)
    score: Mapped[int | None] = mapped_column(default=None)
    ups: Mapped[int | None] = mapped_column(default=None)
    gilded: Mapped[int | None] = mapped_column(default=None)
    all_awardings: Mapped[list | None] = mapped_column(JSONB, default=None)
    created_utc: Mapped[int | None] = mapped_column(default=None)

    fetched_at: Mapped[datetime] = mapped_column(default=func.now(), init=False) 

class UserContributionCacheStatus(Base):
    __tablename__ = 'user_contribution_cache_status'
    username: Mapped[str] = mapped_column(primary_key=True)
    newest_submission_cursor: Mapped[int | None] = mapped_column(default=None)
    newest_comment_cursor: Mapped[int | None] = mapped_column(default=None)


class ThreadCacheStatus(Base):
    __tablename__ = 'thread_cache_status'
    submission_id: Mapped[str] = mapped_column(primary_key=True)
    newest_item_cursor: Mapped[int | None] = mapped_column(default=None)
    is_history_complete: Mapped[bool] = mapped_column(default=False)




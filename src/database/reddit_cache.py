import os
from sqlalchemy import ForeignKey, func, create_engine, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker, joinedload
from sqlalchemy.dialects.postgresql import JSONB, insert
import logging

from datetime import datetime

logger = logging.getLogger(__name__)

class Base(DeclarativeBase):
    pass

class Submission(Base):
    __tablename__ = 'submissions'
    id: Mapped[str] = mapped_column(primary_key=True)
    author: Mapped[str | None]
    subreddit: Mapped[str | None]
    title: Mapped[str | None]
    selftext: Mapped[str | None]
    url: Mapped[str | None]
    score: Mapped[int | None]
    ups: Mapped[int | None]
    upvote_ratio: Mapped[float | None]
    num_comments: Mapped[int | None]
    gilded: Mapped[int | None]
    all_awardings: Mapped[list | None] = mapped_column(JSONB)
    created_utc: Mapped[int]
    fetched_at: Mapped[datetime] = mapped_column(default=func.now())
    raw_json: Mapped[dict] = mapped_column(JSONB)

    comments: Mapped[list["Comment"]] = relationship(back_populates="submission")

class Comment(Base):
    __tablename__ = 'comments'
    id: Mapped[str] = mapped_column(primary_key=True)
    submission_id: Mapped[str | None] = mapped_column(ForeignKey('submissions.id'))
    parent_id: Mapped[str | None]
    author: Mapped[str | None]
    body: Mapped[str | None]
    score: Mapped[int | None]
    ups: Mapped[int | None]
    gilded: Mapped[int | None]
    all_awardings: Mapped[list | None] = mapped_column(JSONB)
    created_utc: Mapped[int]
    fetched_at: Mapped[datetime] = mapped_column(default=func.now())
    raw_json: Mapped[dict] = mapped_column(JSONB)

    submission: Mapped["Submission"] = relationship(back_populates="comments")



class RedditCache:
    def __init__(self):
        url = os.getenv('DATABASE_URL')
        if not url:
            logger.error("DATABASE_URL environment variable not set")
            raise ValueError("DATABASE_URL environment variable not set")
        engine = create_engine(url)
        Session = sessionmaker(bind=engine)
        self.session = Session()

    def add_submissions(self: "RedditCache", _submissions: list[dict]) -> None:
        if not _submissions:
            return

        fields = {c.name for c in Submission.__table__.columns}
        rows = []
        for submission in _submissions:
            # Ensure all fields are present with None for missing values
            filtered = {field: submission.get(field) for field in fields}
            filtered['raw_json'] = submission
            rows.append(filtered)

        stmt = insert(Submission).values(rows).on_conflict_do_nothing(index_elements=['id'])
        self.session.execute(stmt)
        self.session.commit()

    def add_comments(self: "RedditCache", _comments: list[dict]) -> None:
        if not _comments:
            return

        fields = {c.name for c in Comment.__table__.columns}
        rows = []
        for comment in _comments:
            # Ensure all fields are present with None for missing values
            filtered = {field: comment.get(field) for field in fields}
            filtered['submission_id'] = comment['link_id'].removeprefix('t3_')
            filtered['raw_json'] = comment
            rows.append(filtered)

        stmt = insert(Comment).values(rows).on_conflict_do_nothing(index_elements=['id'])
        self.session.execute(stmt)
        self.session.commit()

    def fetch_user_contributions(self: "RedditCache", username: str) -> tuple[list[Submission], list[Comment]]:
        submission_query = select(Submission).where(Submission.author == username)
        comment_query = select(Comment).where(Comment.author == username)
        submissions = self.session.scalars(submission_query).all()
        comments = self.session.scalars(comment_query).all()
        return submissions, comments

    def get_thread(self: "RedditCache", thread_id: str) -> Submission | None:
        submission_query = (
            select(Submission)
            .where(Submission.id == thread_id)
            .options(joinedload(Submission.comments))
        )
        return self.session.scalars(submission_query).first()


    def threads_exist_check(self: "RedditCache", thread_ids: list[str]) -> list[str]:
        if not thread_ids:
            return []

        query = select(Submission.id).where(Submission.id.in_(thread_ids))
        return self.session.scalars(query).all()

    def get_submissions(self: "RedditCache", ids: list[str]) -> list[Submission]:
        if not ids:
            return []

        query = select(Submission).where(Submission.id.in_(ids))
        return self.session.scalars(query).all()

    def get_comments(self: "RedditCache", ids: list[str]) -> list[Comment]:
        if not ids:
            return []

        query = select(Comment).where(Comment.id.in_(ids))
        return self.session.scalars(query).all()

    def close(self: "RedditCache"):
        self.session.close()
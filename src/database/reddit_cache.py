import os
from sqlalchemy import ForeignKey, func, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker
from sqlalchemy.dialects.postgresql import JSONB

from datetime import datetime

class Base(DeclarativeBase):
    pass

class Submission(Base):
    __tablename__ = 'submissions'
    id: Mapped[str] = mapped_column(primary_key=True)
    author: Mapped[str]
    subreddit: Mapped[str]
    title: Mapped[str]
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
    submission_id: Mapped[str] = mapped_column(ForeignKey('submissions.id'))
    parent_id: Mapped[str | None]
    author: Mapped[str]
    body: Mapped[str]
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
        engine = create_engine(os.getenv('DATABASE_URL'))
        Session = sessionmaker(bind=engine)
        self.session = Session()

    def add_submissions(self: "RedditCache", _submissions: list[dict]) -> None:
        for submission in _submissions:
            fields = {c.name for c in Submission.__table__.columns}
            filtered = {k: v for k, v in submission.items() if k in fields}
            filtered['raw_json'] = submission
            self.session.add(Submission(**filtered))

        self.session.commit()

    def add_comments(self: "RedditCache", _comments: list[dict]) -> None:
        for comment in _comments:
            fields = {c.name for c in Comment.__table__.columns}
            filtered = {k: v for k, v in comment.items() if k in fields}
            filtered['submission_id'] = comment['link_id'].removeprefix('t3_')
            filtered['raw_json'] = comment
            self.session.add(Comment(**filtered))

        self.session.commit()

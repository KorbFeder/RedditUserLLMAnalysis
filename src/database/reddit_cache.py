import os
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, joinedload
from sqlalchemy.dialects.postgresql import insert
import logging

from src.models.reddit import Submission, Comment

logger = logging.getLogger(__name__)

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
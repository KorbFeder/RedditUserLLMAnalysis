import os
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
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

    def add_submissions(self: "RedditCache", submissions: list[Submission]) -> None:
        if not submissions:
            return
        
        self.session.add_all(submissions)
        self.session.commit()

    def add_comments(self: "RedditCache", comments: list[Comment]) -> None:
        if not comments:
            return
            
        self.session.add_all(comments)
        self.session.commit()

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

    def get_submission_comments(self, submission_id: str) -> list[Comment]:
        query = select(Comment).where(Comment.submission_id == submission_id)
        return list(self.session.scalars(query).all())

    def submissions_exist(self, ids: list[str]) -> set[str]:
        if not ids:
            return set()

        query = select(Submission.id).where(Submission.id.in_(ids))
        return set(self.session.scalars(query).all())

    def comments_exist(self, ids: list[str]) -> set[str]:
        if not ids:
            return set()

        query = select(Comment.id).where(Comment.id.in_(ids))
        return set(self.session.scalars(query).all())
   
    def close(self: "RedditCache"):
        self.session.close()
import logging

from sqlalchemy.orm import Session

from src.storage.postgres import PostgresStore
from src.storage.vectorstore.pgvector import PgVectorStore
from src.storage.vectorstore.base import ContentType
from src.storage.models import Submission, Comment
from src.services.vectorizer.rag.chunking import DocumentBuilder

logger = logging.getLogger(__name__)

class Vectorizer:
    def __init__(self: "Vectorizer", config: dict, session: Session):
        self.session = session
        self.config = config

        self.store = PostgresStore(self.session)
        self.vector_store = PgVectorStore(config, self.session)
        self.small_to_large = DocumentBuilder()

    def sync_embeddings(self: "Vectorizer", username: str) -> dict:
        logger.info(f"Starting embedding sync for user: {username}")

        submissions: list[Submission] = self.store.get_users_submissions(username)
        comments: list[Comment] = self.store.get_users_comments(username)
        submission_ids = [submission.id for submission in submissions]
        comment_ids = [comment.id for comment in comments]

        logger.info(f"Found {len(submissions)} submissions, {len(comments)} comments")

        existing_submission_ids = self.vector_store.get_existing_ids(submission_ids, ContentType.SUBMISSION)
        existing_comment_ids = self.vector_store.get_existing_ids(comment_ids, ContentType.COMMENT)

        new_submission_ids = set(submission_ids) - existing_submission_ids
        new_comment_ids = set(comment_ids) - existing_comment_ids

        logger.info(f"Skipping {len(existing_submission_ids)} existing submissions, {len(existing_comment_ids)} existing comments")

        submissions = [submission for submission in submissions if submission.id in new_submission_ids]
        comments = [comment for comment in comments if comment.id in new_comment_ids]

        if not submissions and not comments:
            logger.info("No new content to embed")
            return {"submissions": 0, "comments": 0}

        submission_ids_for_comments = [c.submission_id for c in comments if c.submission_id]
        parent_ids = [c.parent_id for c in comments if c.parent_id]

        submissions_by_id = {s.id: s for s in self.store.get_submissions(submission_ids_for_comments)}
        parents_by_id = {c.id: c for c in self.store.get_comments(parent_ids)}

        docs = []
        content_types = []
        ids = []

        for comment in comments:
            submission = submissions_by_id.get(comment.submission_id)
            if not submission:
                logger.warning(f"Missing submission {comment.submission_id} for comment {comment.id}")
                continue
            parent = parents_by_id.get(comment.parent_id)
            doc = self.small_to_large.comment(submission, comment, parent)

            ids.append(comment.id)
            content_types.append(ContentType.COMMENT)
            docs.append(doc)

        for submission in submissions:
            doc = self.small_to_large.submission(submission)

            ids.append(submission.id)
            content_types.append(ContentType.SUBMISSION)
            docs.append(doc)

        total_items = len(ids)
        batch_size = self.config.get("embedding", {}).get("batch_size", 100)
        total_batches = (total_items + batch_size - 1) // batch_size

        logger.info(f"Embedding {len(submissions)} submissions, {len(comments)} comments in {total_batches} batches")

        for i in range(0, total_items, batch_size):
            batch_num = i // batch_size + 1
            batch_ids = ids[i:i + batch_size]
            batch_types = content_types[i:i + batch_size]
            batch_docs = docs[i:i + batch_size]

            self.vector_store.add(batch_ids, batch_types, batch_docs)
            logger.info(f"Batch {batch_num}/{total_batches} complete ({len(batch_ids)} items)")

        logger.info(f"Embedding sync complete for user: {username}")

        return {"submissions": len(submissions), "comments": len(comments)}

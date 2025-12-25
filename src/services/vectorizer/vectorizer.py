import logging

from sqlalchemy.orm import Session

from src.storage.postgres import PostgresStore
from src.storage.vectorstore.pgvector import VectorStoreManager
from src.storage.vectorstore.base import ContentType
from src.storage.models import Submission, Comment
from src.services.vectorizer.rag.chunking import DocumentBuilder
from src.embedding.embedding_factory import get_embeddings, get_embedding_info

logger = logging.getLogger(__name__)


class Vectorizer:
    def __init__(self: "Vectorizer", config: dict, session: Session):
        self.session = session
        self.config = config

        embeddings = get_embeddings(config)
        model_name, dimensions = get_embedding_info(config)

        self.store = PostgresStore(self.session)
        self.vector_store = VectorStoreManager(
            embeddings=embeddings,
            model_name=model_name,
            dimensions=dimensions,
        )
        self.small_to_large = DocumentBuilder()

    def sync_embeddings(self: "Vectorizer", username: str) -> dict:
        """Sync embeddings for a user. Automatically skips already-indexed content."""
        logger.info(f"Starting embedding sync for user: {username}")

        # Fetch all user content from database
        submissions: list[Submission] = self.store.get_users_submissions(username)
        comments: list[Comment] = self.store.get_users_comments(username)

        logger.info(f"Found {len(submissions)} submissions, {len(comments)} comments")

        if not submissions and not comments:
            logger.info("No content to process")
            return {"num_added": 0, "num_skipped": 0, "num_updated": 0, "num_deleted": 0}

        # Fetch parent context for comments
        submission_ids_for_comments = [c.submission_id for c in comments if c.submission_id]
        parent_ids = [c.parent_id for c in comments if c.parent_id]

        submissions_by_id = {
            s.id: s for s in self.store.get_submissions(submission_ids_for_comments)
        }
        parents_by_id = {c.id: c for c in self.store.get_comments(parent_ids)}

        # Build documents for all content
        docs = []
        content_types = []
        ids = []

        for comment in comments:
            submission = submissions_by_id.get(comment.submission_id)
            if not submission:
                logger.warning(
                    f"Missing submission {comment.submission_id} for comment {comment.id}"
                )
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

        # Index all documents - the indexing API handles deduplication automatically
        total_items = len(ids)
        batch_size = self.config.get("embedding", {}).get("batch_size", 100)
        total_batches = (total_items + batch_size - 1) // batch_size

        logger.info(f"Processing {total_items} documents in {total_batches} batches")

        totals = {"num_added": 0, "num_skipped": 0, "num_updated": 0, "num_deleted": 0}

        for i in range(0, total_items, batch_size):
            batch_num = i // batch_size + 1
            batch_ids = ids[i : i + batch_size]
            batch_types = content_types[i : i + batch_size]
            batch_docs = docs[i : i + batch_size]

            result = self.vector_store.add(batch_ids, batch_types, batch_docs, username=username)

            # Accumulate totals
            for key in totals:
                totals[key] += result.get(key, 0)

            logger.info(
                f"Batch {batch_num}/{total_batches}: "
                f"added={result['num_added']}, skipped={result['num_skipped']}"
            )

        logger.info(
            f"Embedding sync complete for user: {username} - "
            f"added={totals['num_added']}, skipped={totals['num_skipped']}"
        )

        return totals

import logging
from datetime import datetime
from dataclasses import asdict
from tqdm import tqdm

from src.services.repository import Repository
from src.storage.chroma import VectorStore
from src.storage.models import Submission, Comment
from src.rag.chunking import DocumentBuilder, DocumentMetadata, DocumentType

logger = logging.getLogger(__name__)

class Vectorizer:
    def __init__(self: "Vectorizer", config: dict):
        self.reddit_repo = Repository(config)
        self.db = VectorStore()
        self.small_to_large = DocumentBuilder()

    def store_user_data(self: "Vectorizer", username: str):
        submissions, comments = self.reddit_repo.get_user_contributions(username)

        thread_ids = list(dict.fromkeys(
            [submission.id for submission in submissions] +
            [comment.submission_id for comment in comments if comment.submission_id]
        ))

        threads_stored = 0

        for thread_id in thread_ids:
            thread = self.reddit_repo.get_thread(thread_id)

            if thread is None:
                logger.info(f"The thread {thread_id} is None!")
                continue


            logger.info(f"Submission/Post {threads_stored} of {len(thread_ids)} sumissions/posts")
            logger.info(f"storing the full thread with thread id: {thread_id} in the database")
            threads_stored += 1
 
    
    def fill_vector_db(self: "Vectorizer", username: str):
        submissions, comments = self.reddit_repo.get_user_contributions(username)

        # filter out submission already stored in the vector db
        existing_ids = set(self.db.elements_exist_check([s.id for s in submissions]))
        submissions = [submission for submission in submissions if submission.id not in existing_ids]
        logger.info(f"Skipping {len(existing_ids)} existing, inserting {len(submissions)} new submissions")

        id_batch = []
        doc_batch = []
        metadata_batch = []
        logger.info(f"Fetched Submissions and Comment now filling Vector database with {len(submissions)} submissions and {len(comments)} comments")
        for submission in tqdm(submissions):
            doc = self.small_to_large.submission(submission)
            metadata = DocumentMetadata(
                id=submission.id,
                document_type=DocumentType.SUBMISSION.value,
                submission_id=submission.id,
                parent_id="",
                username=username,
                parent_author="",
                subreddit=submission.subreddit or "",
                post_title=submission.title or "",
                created_utc=submission.created_utc or 0,
                score=submission.score or 0,
                is_top_level=False,
                num_comments=submission.num_comments or 0,
                upvote_ratio=submission.upvote_ratio or 0.0
            )
            id_batch.append(submission.id)
            doc_batch.append("\n".join(doc))
            metadata_batch.append(asdict(metadata))

        before = self.db.get_element_count()
        self.db.add_elements(id_batch, doc_batch, metadata_batch)
        after = self.db.get_element_count()

        logger.info("Added all submissions to the vector database moving on to comments")
        logger.info(f"Nr of elements in vectordb before: {before} and now afterwards: {after}")

        # filter comments
        existing_ids = set(self.db.elements_exist_check([c.id for c in comments]))
        comments = [comment for comment in comments if comment.id not in existing_ids]
        logger.info(f"Skipping {len(existing_ids)} existing, inserting {len(comments)} new comments")

        id_batch = []
        doc_batch = []
        metadata_batch = []

        submission_ids = list(set(c.submission_id for c in comments if c.submission_id))
        submissions = {s.id: s for s in self.reddit_repo.cache.get_submissions(submission_ids)}
        parent_ids = [c.parent_id for c in comments if c.parent_id]
        parent_comments = {c.id: c for c in self.reddit_repo.cache.get_comments(parent_ids)}

        for comment in tqdm(comments):
            parent_comment = parent_comments.get(comment.parent_id)
            submission =  submissions.get(comment.submission_id)
            
            if submission is None:
                logger.warning(f"Could not find submission {comment.submission_id} for comment {comment.id}")
                continue

            # maybe a batch fetch here cause of speed
            doc = self.small_to_large.comment(submission, comment, parent_comment)

            metadata = DocumentMetadata(
                id=comment.id,
                document_type=DocumentType.COMMENT.value,
                submission_id=submission.id,
                parent_id=comment.parent_id or "",
                username=username,
                parent_author=parent_comment.author if parent_comment else "",
                subreddit=submission.subreddit or "",
                post_title=submission.title or "",
                created_utc=comment.created_utc or 0,
                score=comment.score or 0,
                is_top_level=False if parent_comment else True,
                num_comments=0,
                upvote_ratio=0.0
            )           
            id_batch.append(comment.id)
            doc_batch.append("\n".join(doc))
            metadata_batch.append(asdict(metadata))

        self.db.add_elements(id_batch, doc_batch, metadata_batch)
        


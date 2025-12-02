from datetime import datetime
import logging

from src.models.reddit import Comment, Submission
from src.database.rag.order_comments import CommentNode
from src.database.reddit_vectorstore import ThreadMetadata
from src.database.rag.order_comments import order_comments

logger = logging.getLogger(__name__)

class ThreadToText:
    def _add_comment(self: "ThreadToText", node: CommentNode, document: list[str]):
        comment = node.comment
        document.append("---------------------------------------------")
        document.append(f"Poster/Author/Username: {comment.author}")
        document.append(f"The score of the reddit post: {comment.score} and upvotes {comment.ups}")
        document.append(f"Number of rewards: {comment.gilded} with {comment.all_awardings}")
        document.append(f"Created on: {datetime.fromtimestamp(int(comment.created_utc or 0))}")
        document.append(f"Comment: {comment.body}")
        document.append(f"============================================")


    def _iterate_to_leaf(self: "ThreadToText", nodes: list[CommentNode], document: list[str], depth: str):
        for i, node in enumerate(nodes, 1):
            document.append(f"{depth}{str(i)}. Reply")
            self._add_comment(node, document)

            self._iterate_to_leaf(node.replies, document, f"{depth}{i}.")

    
    def _convert_thread_to_document(self: "ThreadToText", submission: Submission, comments: list[Comment]) -> tuple[list[str], ThreadMetadata]:
        comments_tree = order_comments(submission.id, comments)

        document = []
        document.append("POST")
        document.append("---------------------------------------------")
        document.append(f"Reddit Post Title: {submission.title} (Post ID: {submission.id})")
        document.append(f"Subreddit: {submission.subreddit}")
        document.append(f"Poster/Author/Username: {submission.author}")
        document.append(f"Reddit Post URL: {submission.url}")
        document.append(f"The score of the reddit post: {submission.score} (upvote ratio: {submission.upvote_ratio} and upvotes {submission.ups})")
        document.append(f"Number of rewards: {submission.gilded} with {submission.all_awardings}")
        document.append(f"Created on: {datetime.fromtimestamp(int(submission.created_utc or 0))}")
        document.append(f"Text of the Post: {submission.selftext}")

        if len(comments) > 0:
            document.append(f"============================================")
            document.append(f"Comments that were posted under the post:")
            document.append(f"============================================")

        for i, node in enumerate(comments_tree, 1):
            document.append(f"{i}. Reply")
            self._add_comment(node, document)
            self._iterate_to_leaf(node.replies, document, f"{i}.")

        logger.info(f"Converted Post: {submission.title} ({submission.id}), with all the comments, to a single document")

        # Create the Metadata for the RAG DB
        metadata = ThreadMetadata(
            id=submission.id,
            username=submission.author or "",
            created=int(submission.created_utc or 0),
            nr_of_rewards=submission.gilded or 0,
            num_comments=submission.num_comments or 0,
            url=submission.url or "",
            score=submission.score or 0,
            ups=submission.ups or 0,
            upvote_ratio=submission.upvote_ratio or 0.0,
            title=submission.title or "",
        )

        return document, metadata


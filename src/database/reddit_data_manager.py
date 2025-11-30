import logging
from datetime import datetime
from typing import List, Dict, TypedDict, Tuple
from dataclasses import dataclass, field

from src.database.reddit_repository import RedditRepository
from database.reddit_vectorstore import RedditVectorstore, ThreadMetadata
from src.models.reddit import Submission, Comment

logger = logging.getLogger(__name__)

@dataclass
class CommentNode:
    comment: Comment
    replies: list["CommentNode"] = field(default_factory=list)

class DataManager:
    def __init__(self: "DataManager", config: dict):
        self.reddit_repo = RedditRepository(config)
        self.db = RedditVectorstore()

    def store_user_data(self: "DataManager", username: str):
        submissions, comments = self.reddit_repo.get_user_contributions(username)

        thread_ids = list(set(
            [submission.id for submission in submissions] + 
            [comment.submission_id for comment in comments]
        ))

        threads_stored = 0

        for thread_id in thread_ids:
            thread = self.reddit_repo.get_thread(thread_id)

            if thread is None:
                logger.info(f"The thread {thread_id} is None!")
                continue

            submission, comments = thread
            document, metadata = self._convert_thread_to_document(submission, comments)

            logger.info(f"Submission/Post {threads_stored} of {len(thread_ids)} sumissions/posts")
            logger.info(f"storing the full thread with thread id: {thread_id} in the database")
            self.db.add_thread(thread_id, document, metadata)
            threads_stored += 1
 
    
    def search_user_data(self: "DataManager", username: str, search_term):
        pass

    def _add_comment(self: "DataManager", node: CommentNode, document: List[str]):
        comment = node.comment
        document.append("---------------------------------------------")
        document.append(f"Poster/Author/Username: {comment.author}")
        document.append(f"The score of the reddit post: {comment.score} and upvotes {comment.ups}")
        document.append(f"Number of rewards: {comment.gilded} with {comment.all_awardings}")
        document.append(f"Created on: {datetime.fromtimestamp(int(comment.created_utc))}")
        document.append(f"Comment: {comment.body}")
        document.append(f"============================================")


    def _iterate_to_leaf(self: "DataManager", nodes: List[CommentNode], document: List[str], depth: str):
        for i, node in enumerate(nodes, 1):
            document.append(f"{depth}{str(i)}. Reply")
            self._add_comment(node, document)

            self._iterate_to_leaf(node.replies, document, f"{depth}{i}.")

    
    def _convert_thread_to_document(self: "DataManager", submission: Submission, comments: list[Comment]) -> Tuple[List[str], ThreadMetadata]:
        comments_tree = self._order_comments(submission.id, comments)

        document = []
        document.append("POST")
        document.append("---------------------------------------------")
        document.append(f"Reddit Post Title: {submission.title} (Post ID: {submission.id})")
        document.append(f"Subreddit: {submission.subreddit}")
        document.append(f"Poster/Author/Username: {submission.author}")
        document.append(f"Reddit Post URL: {submission.url}")
        document.append(f"The score of the reddit post: {submission.score} (upvote ratio: {submission.upvote_ratio} and upvotes {submission.ups})")
        document.append(f"Number of rewards: {submission.gilded} with {submission.all_awardings}")
        document.append(f"Created on: {datetime.fromtimestamp(int(submission.created_utc))}")
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
            username=submission.author,
            created=int(submission.created_utc),
            nr_of_rewards=submission.gilded,
            num_comments=submission.num_comments,
            url=submission.url,
            score=submission.score,
            ups=submission.ups,
            upvote_ratio=submission.upvote_ratio,
            title=submission.title,
        )
        return document, metadata

    def _order_comments(self: "DataManager", submission_id: str, comments: list[Comment]) -> list[CommentNode]:
        nodes = {c.id: CommentNode(comment=c) for c in comments}
        root = []

        for comment in comments:
            parent_id = comment.parent_id

            # in case the comment does not have a parent id (faulty or deleted comment)
            if not parent_id or isinstance(parent_id, int):
                logger.warning(f"Adding comment {comment.id} to root, cause parent_id field seems corrupted")
                root.append(nodes[comment.id])
                continue

            if parent_id == submission_id:
                root.append(nodes[comment.id])
            elif parent_id in nodes:
                nodes[parent_id].replies.append(nodes[comment.id])
            else:
                logger.warning(f"The comment {comment.id} parent {parent_id} could not be found, adding it to root")
                root.append(nodes[comment.id])

        return root  

#a = DataManager()
#a.store_user_data('swintec')
#b = a._convert_thread_to_document('1h0n5ql')
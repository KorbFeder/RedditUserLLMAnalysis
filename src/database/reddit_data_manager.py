import logging
from datetime import datetime
from typing import List, Dict, TypedDict, Tuple

from src.data_providers.pushpull import PushPullProvider
from src.database.reddit_store import RedditStore, ThreadMetadata

logger = logging.getLogger(__name__)

class DataManager:
    def __init__(self: "DataManager"):
        self.pushpull = PushPullProvider()
        self.db = RedditStore()

    def store_user_data(self: "DataManager", username: str, cache: bool = True):
        submissions, comments = self.pushpull.fetch_user_contributions(username)

        thread_ids = list(set([submission['id'] for submission in submissions] + [comment['link_id'].removeprefix('t3_') for comment in comments]))

        threads_stored = 0

        # filter existing threads
        if cache:
            logger.info("Checking for already existing threads is set to true so only fetching missing ones")
            logger.info(f"Before filtering for existing threads in the database we have {len(thread_ids)} thread_ids to fetch")
            thread_ids = self._check_if_thread_already_saved(thread_ids)
            logger.info(f"After filtering  for existing threads in the database we have {len(thread_ids)} thread_ids to fetch")

        for thread_id in thread_ids:
            thread = self._convert_thread_to_document(thread_id)
            if thread == None:
                logger.info(f"The thread {thread_id} is None!")
                continue

            document, metadata = thread
            logger.info(f"Submission/Post {threads_stored} of {len(thread_ids)} sumissions/posts")
            logger.info(f"storing the full thread with thread id: {thread_id} in the database")
            self.db.add_thread(thread_id, document, metadata)
            threads_stored += 1
 
    
    def _check_if_thread_already_saved(self: "DataManager", thread_ids: List[str]):
        existing_threads = set(self.db.threads_exist_check(thread_ids))
        logger.info(f"Found {len(existing_threads)} existing threads in the rag database already")
        missing_threads = [thread_id for thread_id in thread_ids if thread_id not in existing_threads]
        return missing_threads

            
    def search_user_data(self: "DataManager", username: str, search_term):
        pass

    def _add_comment(self: "DataManager", comment: Dict, document: List[str]):
        document.append("---------------------------------------------")
        document.append(f"Poster/Author/Username: {comment.get('author', 'Error')}")
        document.append(f"The score of the reddit post: {comment.get('score', 'Error')} and upvotes {comment.get('ups', 'Error')}")
        document.append(f"Number of rewards: {comment.get('gilded', 'Error')} with {comment.get('all_awardings', 'Error')}")
        document.append(f"Created on: {datetime.fromtimestamp(int(comment.get('created_utc', 0)))}")
        document.append(f"Comment: {comment.get('body', 'Error')}")
        document.append(f"============================================")


    def iterate_to_leaf(self: "DataManager", replies: List[Dict], document: List[str], depth: str):
        for i, reply in enumerate(replies):
            document.append(f"{depth}{str(i+1)}. Reply")
            self._add_comment(reply, document)

            self.iterate_to_leaf(reply['replies'], document, depth + str(i+1) + ".")

    
    def _convert_thread_to_document(self: "DataManager", thread_id: str) -> Tuple[List[str], ThreadMetadata]:
        # Try cache first
        thread = self.pushpull.get_thread(thread_id)
        if thread:
            submission, comments = thread
        else:
            return None

        document = []
        document.append("POST")
        document.append("---------------------------------------------")
        document.append(f"Reddit Post Title: {submission.get('title', 'Error')} (Post ID: {submission.get('id', 'Error')})")
        document.append(f"Subreddit: {submission.get('subreddit', 'Error')}")
        document.append(f"Poster/Author/Username: {submission.get('author', 'Error')}")
        document.append(f"Reddit Post URL: {submission.get('url', 'Error')}")
        document.append(f"The score of the reddit post: {submission.get('score', 'Error')} (upvote ratio: {submission.get('upvote_ratio', 'Error')} and upvotes {submission.get('ups', 'Error')})")
        document.append(f"Number of rewards: {submission.get('gilded', 'Error')} with {submission.get('all_awardings', 'Error')}")
        document.append(f"Created on: {datetime.fromtimestamp(int(submission.get('created_utc', 0)))}")
        document.append(f"Text of the Post: {submission.get('selftext', 'Error')}")

        if len(comments) > 0:
            document.append(f"============================================")
            document.append(f"Comments that were posted under the post:")
            document.append(f"============================================")

        root_comment_nr = 1
        for comment in comments:

            document.append(f"{root_comment_nr}. Reply")
            self._add_comment(comment, document)
 
            self.iterate_to_leaf(comment["replies"], document, f"{root_comment_nr}.")
            
            root_comment_nr += 1

        logger.info(f"Converted Post: {submission.get('title', 'Error')} ({thread_id}), with all the comments, to a single document")

        # Create the Metadata for the RAG DB
        metadata = ThreadMetadata(
            id=submission['id'],
            username=submission.get('author', 'error'),
            created=int(submission.get('created_utc', 0)),
            nr_of_rewards=submission.get('gilded', 0),
            num_comments=submission.get('num_comments', 0),
            url=submission.get('url', ''),
            score=submission.get('score', 0),
            ups=submission.get('ups', 0),
            upvote_ratio=submission.get('upvote_ratio', 0.0),
            title=submission.get('title', ''),
        )
        return document, metadata


a = DataManager()
a.store_user_data('swintec')
#b = a._convert_thread_to_document('1h0n5ql')
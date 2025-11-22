
import logging
from datetime import datetime
from typing import List, Dict

from src.data_providers.pushpull import PushPullProvider

logger = logging.getLogger(__name__)

class DataManager:
    def __init__(self: "DataManager"):
        self.pushpull = PushPullProvider()

    def _add_comment(self: "DataManager", comment: Dict, document: List[str]):
        document.append("---------------------------------------------")
        document.append(f"Poster/Author/Username: {comment['author']}")
        document.append(f"The score of the reddit post: {comment['score']} and upvotes {comment['ups']}")
        document.append(f"Number of rewards: {comment.get('gilded', 0)} with {comment.get('all_awardings', '')}")
        document.append(f"Created on: {datetime.fromtimestamp(int(comment['created_utc']))}")
        document.append(f"Comment: {comment['body']}")
        document.append(f"============================================")


    def iterate_to_leaf(self: "DataManager", replies: List[Dict], document: List[str], depth: str):
        for i, reply in enumerate(replies):
            document.append(f"{depth}{str(i+1)}. Reply")
            self._add_comment(reply, document)

            self.iterate_to_leaf(reply['replies'], document, depth + str(i+1) + ".")

    
    def _convert_thread_to_document(self: "DataManager", thread_id: str):
        submission, comments = self.pushpull.get_thread(thread_id)

        document = []
        document.append("POST")
        document.append("---------------------------------------------")
        document.append(f"Reddit Post Title: {submission['title']} (Post ID: {submission['id']})")
        document.append(f"Subreddit: {submission['subreddit']}")
        document.append(f"Poster/Author/Username: {submission['author']}")
        document.append(f"Reddit Post URL: {submission['url']}")
        document.append(f"The score of the reddit post: {submission['score']} (upvote ratio: {submission['upvote_ratio']} and upvotes {submission['ups']})")
        document.append(f"Number of rewards: {submission.get('gilded', 0)} with {submission.get('all_awardings', '')}")
        document.append(f"Created on: {datetime.fromtimestamp(int(submission['created_utc']))}")
        document.append(f"Text of the Post: {submission.get('selftext', '')}")

        document.append(f"============================================")
        document.append(f"Comments that were posted under the post:")
        document.append(f"============================================")

        root_comment_nr = 1
        for comment in comments:

            document.append(f"{root_comment_nr}. Reply")
            self._add_comment(comment, document)
 
            self.iterate_to_leaf(comment["replies"], document, f"{root_comment_nr}.")
            
            root_comment_nr += 1

        logger.info(f"Converted Post: {submission['title']} ({thread_id}), with all the comments, to a single document")
        return document
        

        


#a = DataManager()
#b = a._convert_thread_to_document('1h0n5ql')
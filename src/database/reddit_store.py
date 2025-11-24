import chromadb 
from typing import List, Dict, TypedDict, Tuple, Optional

class ThreadMetadata(TypedDict):
    id: str
    username: str
    created: int
    nr_of_rewards: int
    num_comments: int
    url: str
    score: int
    ups: int
    upvote_ratio: float
    title: str

class RedditStore:
    def __init__(self: "RedditStore"):
        self.db = chromadb.PersistentClient(path="./data/chroma_db")

        self.thread_collection = self.db.get_or_create_collection(
            name="reddit_threads",  # Changed to "threads" since you store full threads
        )

    def get_thread_count(self: "RedditStore"):
        return self.thread_collection.count()

    def add_thread(self: "RedditStore", thread_id: str, document: list[str], metadata: ThreadMetadata):
        document_text = "\n".join(document)

        self.thread_collection.add(
            documents=[document_text],
            metadatas=[metadata],
            ids=[thread_id]
        )

    def threads_exist_check(self: "RedditStore", thread_ids: List[str]) -> List[str]:
        if len(thread_ids) == 0:
            return []

        existing_ids = self.thread_collection.get(ids=thread_ids, include=[])
        return existing_ids['ids']


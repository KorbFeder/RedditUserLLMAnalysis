import chromadb 
from typing import List, Dict, TypedDict, Tuple

class ThreadMetadata(TypedDict):
    id: str
    username: str
    created: int
    nr_of_rewards: int
    num_comments: int
    url: str
    score: int
    ups: int
    upvote_ratio: int
    title: str

class RedditStore:
    def __init__(self: "RedditStore"):
        self.db = chromadb.PersistentClient(path="./data/chroma_db")

        self.thread_collection = self.db.get_or_create_collection(
            name="reddit_threads",  # Changed to "threads" since you store full threads
        )

    def add_thread(self: "RedditStore", thread_id: str, document: list[str], metadata: ThreadMetadata):
        document_text = "\n".join(document)

        self.thread_collection.add(
            documents=[document_text],
            metadatas=[metadata],
            ids=[thread_id]
        )
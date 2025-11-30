import chromadb 
from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction
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

class RedditVectorstore:
    def __init__(self: "RedditVectorstore"):
        self.db = chromadb.PersistentClient(path="./data/chroma_db")

        self.embedding_function = SentenceTransformerEmbeddingFunction(
            model_name="nomic-ai/nomic-embed-text-v1.5",
            trust_remote_code=True
        )

        self.thread_collection = self.db.get_or_create_collection(
            name="reddit_threads",  
            embedding_function=self.embedding_function
        )

    def get_thread_count(self: "RedditVectorstore"):
        return self.thread_collection.count()

    def add_thread(self: "RedditVectorstore", thread_id: str, document: list[str], metadata: ThreadMetadata):
        document_text = "\n".join(document)

        self.thread_collection.add(
            documents=[document_text],
            metadatas=[metadata],
            ids=[thread_id]
        )

    def threads_exist_check(self: "RedditVectorstore", thread_ids: List[str]) -> List[str]:
        if len(thread_ids) == 0:
            return []

        existing_ids = self.thread_collection.get(ids=thread_ids, include=[])
        return existing_ids['ids']


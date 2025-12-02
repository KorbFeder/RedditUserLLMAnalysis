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

    def get_element_count(self: "RedditVectorstore"):
        return self.thread_collection.count()

    def add_elements(self: "RedditVectorstore", ids: list[str], documents: list[str], metadatas: list[dict]):
        if len(ids) > 0:
            self.thread_collection.add(
                documents=documents,
                metadatas=metadatas,
                ids=ids
            )

    def elements_exist_check(self: "RedditVectorstore", ids: list[str]) -> List[str]:
        if len(ids) == 0:
            return []

        existing_ids = self.thread_collection.get(ids=ids, include=[])
        return existing_ids['ids']


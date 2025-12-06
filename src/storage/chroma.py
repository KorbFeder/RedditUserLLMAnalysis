import chromadb 
from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction
from typing import List, Dict, TypedDict, Tuple, Optional
import logging
 
logger = logging.getLogger(__name__)

class VectorStore:
    def __init__(self: "VectorStore"):
        self.db = chromadb.PersistentClient(path="./data/chroma_db")

        self.embedding_function = SentenceTransformerEmbeddingFunction(
            model_name="nomic-ai/nomic-embed-text-v1.5",
            trust_remote_code=True
        )

        self.thread_collection = self.db.get_or_create_collection(
            name="reddit_threads",  
            embedding_function=self.embedding_function
        )

    def query_user_content(self: "VectorStore", query_text: str, username: str, n_results: int = 10) -> dict:
        logger.info(f"Query for Rag: {query_text}")
        response = self.thread_collection.query(query_texts=[query_text], n_results=n_results, where={"username": username})
        logger.info(f"Rag response {response}")
        return response

    def get_element_count(self: "VectorStore"):
        return self.thread_collection.count()

    def add_elements(self: "VectorStore", ids: list[str], documents: list[str], metadatas: list[dict]):
        if len(ids) > 0:
            self.thread_collection.add(
                documents=documents,
                metadatas=metadatas,
                ids=ids
            )

    def elements_exist_check(self: "VectorStore", ids: list[str]) -> List[str]:
        if len(ids) == 0:
            return []

        existing_ids = self.thread_collection.get(ids=ids, include=[])
        return existing_ids['ids']


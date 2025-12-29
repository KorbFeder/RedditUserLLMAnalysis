import logging

from langchain_classic.retrievers import EnsembleRetriever
from langchain_core.documents import Document

from src.storage.vectorstore.pgvector import VectorStoreManager
from src.storage.vectorstore.pgsparse import PgSparseRetriever
from src.storage.postgres import PostgresStore
from src.services.agent.comment_chain import CommentChain
from src.services.agent.retrieval.reranker import Reranker
from src.embedding.embedding_factory import get_embeddings, get_embedding_info

logger = logging.getLogger(__name__)


class Retriever:
    def __init__(self, config: dict, session):
        self.session = session
        self.config = config

        embeddings = get_embeddings(config)
        model_name, dimensions = get_embedding_info(config)

        self.vector_store = VectorStoreManager(
            embeddings=embeddings,
            model_name=model_name,
            dimensions=dimensions,
        )
        self.store = PostgresStore(session)
        self.reranker = Reranker(config)

        search_config = config.get("search", {})
        self.dense_k = search_config.get("dense", {}).get("limit", 50)
        self.sparse_k = search_config.get("sparse", {}).get("limit", 50)
        self.dense_weight = search_config.get("dense", {}).get("weight", 0.5)
        self.sparse_weight = search_config.get("sparse", {}).get("weight", 0.5)
        self.rrf_k = search_config.get("rrf_k", 60)

    def _create_hybrid_retriever(self, username: str) -> EnsembleRetriever:
        """Create a hybrid retriever for the given user."""
        dense = self.vector_store.as_retriever(
            search_kwargs={"k": self.dense_k, "filter": {"username": username}}
        )
        sparse = PgSparseRetriever(
            session=self.session,
            username=username,
            k=self.sparse_k,
        )
        return EnsembleRetriever(
            retrievers=[dense, sparse],
            weights=[self.dense_weight, self.sparse_weight],
            c=self.rrf_k,
        )

    def search(self, query: str, username: str, limit: int | None = None) -> list[CommentChain]:
        """
        Search user's content using hybrid search (dense + sparse) with RRF fusion.

        Args:
            query: Search query text
            username: Filter results to this user's content
            limit: Max results to return

        Returns:
            Fused and ranked search results
        """
        hybrid = self._create_hybrid_retriever(username)
        docs = hybrid.invoke(query)

        logger.info(f"Hybrid retrieval result count: {len(docs)}")
        chains = self.fetch_from_db(docs)
        result = self.reranker.rank(query, chains)
        logger.info(f"Reranker result count: {len(result)}")

        for r in result:
            logger.info(r.to_context_string())

        return result

    def search_with_scores(self, query: str, username: str) -> list[tuple[CommentChain, float]]:
        """
        Search user's content and return results with reranker scores.

        Args:
            query: Search query text
            username: Filter results to this user's content

        Returns:
            List of (CommentChain, score) tuples, sorted by score descending
        """
        hybrid = self._create_hybrid_retriever(username)
        docs = hybrid.invoke(query)

        logger.info(f"Hybrid retrieval result count: {len(docs)}")
        chains = self.fetch_from_db(docs)
        result = self.reranker.rank_with_scores(query, chains)
        logger.info(f"Reranker result count: {len(result)}")

        return result

    def fetch_from_db(self, docs: list[Document]) -> list[CommentChain]:
        """Convert retrieved Documents to CommentChains with full context."""
        chains: list[CommentChain] = []

        # Separate by content type
        submission_ids = [
            doc.metadata["content_id"]
            for doc in docs
            if doc.metadata.get("content_type") == "submission"
        ]
        comment_ids = [
            doc.metadata["content_id"]
            for doc in docs
            if doc.metadata.get("content_type") == "comment"
        ]

        # Batch fetch comment chains (1 query)
        comment_chains_dict = self.store.get_comment_chains(comment_ids)

        # Collect all submission IDs needed
        all_submission_ids = set(submission_ids)
        for chain in comment_chains_dict.values():
            if chain:
                all_submission_ids.add(chain[0].submission_id)

        # Batch fetch submissions (1 query)
        submissions = {s.id: s for s in self.store.get_submissions(list(all_submission_ids))}

        # Build chains for submissions (user authored a post)
        for sid in submission_ids:
            if sid in submissions:
                chains.append(CommentChain(submission=submissions[sid], comments=[]))

        # Build chains for comments
        for chain in comment_chains_dict.values():
            if chain:
                sub = submissions.get(chain[0].submission_id)
                if sub:
                    chains.append(CommentChain(submission=sub, comments=chain))

        return chains

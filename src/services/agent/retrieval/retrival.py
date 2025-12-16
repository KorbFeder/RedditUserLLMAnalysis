import logging

from src.storage.vectorstore.pgvector import PgVectorStore
from src.storage.vectorstore.base import SearchResult, ContentType
from src.services.agent.retrieval.rrf import ReciprocalRankFusion
from src.storage.postgres import PostgresStore
from src.services.agent.comment_chain import CommentChain
from src.services.agent.retrieval.reranker import Reranker

logger = logging.getLogger(__name__)

class Retriever:
    def __init__(self, config: dict, session):
        self.vector_store = PgVectorStore(config, session)
        self.store = PostgresStore(session)
        self.reranker = Reranker(config)

        search_config = config.get("search", {})
        self.rrf = ReciprocalRankFusion(k=search_config.get("rrf_k", 60))
        self.dense_weight = search_config.get("dense", {}).get("weight", 0.5)
        self.sparse_weight = search_config.get("sparse", {}).get("weight", 0.5)

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
        dense_results = self.vector_store.dense_search(query, username=username)
        sparse_results = self.vector_store.sparse_search(query, username=username)

        fused = self.rrf.run(
            [dense_results, sparse_results],
            weights=[self.dense_weight, self.sparse_weight]
        )
        chains = self.fetch_from_db(fused)
        result = self.reranker.rank(query, chains)
        
        logger.info("Finished the search and ranked with reranker:")
        for r in result:
            logger.info(r.to_context_string)

        return result

    def search_custom(
        self,
        query: str,
        username: str,
        weights: list[float],
        limit: int | None = None
    ) -> list[SearchResult]:
        """
        Search with custom weights (override config).

        Args:
            query: Search query text
            username: Filter results to this user's content
            weights: [dense_weight, sparse_weight]
            limit: Max results to return

        Returns:
            Fused and ranked search results
        """
        dense_results = self.vector_store.dense_search(query, username=username)
        sparse_results = self.vector_store.sparse_search(query, username=username)

        fused = self.rrf.run([dense_results, sparse_results], weights=weights)
        return fused[:limit] if limit else fused

    def fetch_from_db(self, search_results: list[SearchResult]) -> list[CommentChain]:
        chains: list[CommentChain] = []

        # Separate by content type
        submission_ids = [s.content_id for s in search_results if s.content_type == ContentType.SUBMISSION]
        comment_ids = [s.content_id for s in search_results if s.content_type == ContentType.COMMENT]

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

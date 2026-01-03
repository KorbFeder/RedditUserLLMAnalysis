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
            session=session,
        )
        self.store = PostgresStore(session)
        self.reranker = Reranker(config)

        search_config = config.get("search", {})
        self.dense_k = search_config.get("dense", {}).get("limit", 50)
        self.sparse_k = search_config.get("sparse", {}).get("limit", 50)
        self.dense_weight = search_config.get("dense", {}).get("weight", 0.5)
        self.sparse_weight = search_config.get("sparse", {}).get("weight", 0.5)
        self.rrf_k = search_config.get("rrf_k", 60)

    def _create_hybrid_retriever(
        self,
        username: str,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> EnsembleRetriever:
        """Create a hybrid retriever for the given user with optional time filtering."""
        # Build filter for dense search
        # langchain_postgres requires $and for multiple conditions on same field
        filter_conditions = [{"username": username}]
        if start_time is not None:
            filter_conditions.append({"created_utc": {"$gte": start_time}})
        if end_time is not None:
            filter_conditions.append({"created_utc": {"$lte": end_time}})

        # Use $and if multiple conditions, otherwise simple filter
        if len(filter_conditions) == 1:
            dense_filter = filter_conditions[0]
        else:
            dense_filter = {"$and": filter_conditions}

        dense = self.vector_store.as_retriever(
            search_kwargs={"k": self.dense_k, "filter": dense_filter}
        )
        sparse = PgSparseRetriever(
            session=self.session,
            username=username,
            k=self.sparse_k,
            start_time=start_time,
            end_time=end_time,
        )
        return EnsembleRetriever(
            retrievers=[dense, sparse],
            weights=[self.dense_weight, self.sparse_weight],
            c=self.rrf_k,
        )

    def search(
        self,
        query: str,
        username: str,
        limit: int | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> list[CommentChain]:
        """
        Search user's content using hybrid search (dense + sparse) with RRF fusion.

        Args:
            query: Search query text
            username: Filter results to this user's content
            limit: Max results to return
            start_time: Only include content created after this Unix timestamp
            end_time: Only include content created before this Unix timestamp

        Returns:
            Fused and ranked search results
        """
        hybrid = self._create_hybrid_retriever(username, start_time, end_time)
        docs = hybrid.invoke(query)

        logger.info(f"Hybrid retrieval result count: {len(docs)}")
        chains = self.fetch_from_db(docs)
        result = self.reranker.rank(query, chains)
        logger.info(f"Reranker result count: {len(result)}")

        for r in result:
            logger.info(r.to_context_string())

        return result

    def search_with_scores(
        self,
        query: str,
        username: str,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> list[tuple[CommentChain, float]]:
        """
        Search user's content and return results with reranker scores.

        Args:
            query: Search query text
            username: Filter results to this user's content
            start_time: Only include content created after this Unix timestamp
            end_time: Only include content created before this Unix timestamp

        Returns:
            List of (CommentChain, score) tuples, sorted by score descending
        """
        hybrid = self._create_hybrid_retriever(username, start_time, end_time)
        docs = hybrid.invoke(query)

        logger.info(f"Hybrid retrieval result count: {len(docs)}")
        chains = self.fetch_from_db(docs)
        result = self.reranker.rank_with_scores(query, chains)
        logger.info(f"Reranker result count: {len(result)}")

        return result

    def fetch_from_db(self, docs: list[Document]) -> list[CommentChain]:
        """Convert retrieved Documents to CommentChains with full context.

        Deduplicates by submission - multiple comments from the same thread
        are merged into a single CommentChain to avoid context duplication.
        """
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

        # Group comments by submission to deduplicate
        comments_by_submission: dict[str, dict[str, any]] = {}

        for chain in comment_chains_dict.values():
            if chain:
                sub_id = chain[0].submission_id
                if sub_id not in comments_by_submission:
                    comments_by_submission[sub_id] = {}
                # Add all comments from this chain (deduped by id)
                for comment in chain:
                    comments_by_submission[sub_id][comment.id] = comment

        # Build deduplicated chains
        chains: list[CommentChain] = []

        # Add submission-only chains (user authored posts, not covered by comments)
        for sid in submission_ids:
            if sid in submissions and sid not in comments_by_submission:
                chains.append(CommentChain(submission=submissions[sid], comments=[]))

        # Add merged comment chains (one per submission)
        for sub_id, comments_dict in comments_by_submission.items():
            sub = submissions.get(sub_id)
            if sub:
                # Sort comments by created_utc for chronological order
                sorted_comments = sorted(
                    comments_dict.values(),
                    key=lambda c: c.created_utc or 0
                )
                chains.append(CommentChain(submission=sub, comments=sorted_comments))

        return chains

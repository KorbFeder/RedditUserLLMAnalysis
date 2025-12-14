from src.storage.vectorstore.base import SearchResult, ContentType


class ReciprocalRankFusion:
    """
    Reciprocal Rank Fusion (RRF) for combining multiple ranked lists.

    RRF score for a document d:
        score(d) = Σ weight_i / (k + rank_i(d))
    """

    def __init__(self, k: int = 60):
        """
        Args:
            k: RRF constant (default 60). Higher k reduces impact of high ranks.
        """
        self.k = k

    def run(self, all_search_results: list[list[SearchResult]], weights: list[float] | None = None) -> list[SearchResult]:
        """
        Fuse multiple ranked lists using RRF.

        Args:
            all_search_results: List of ranked result lists (e.g., [sparse, dense_1, dense_2])
            weights: Optional weights per list. If None, uses equal weights (1/n).

        Returns:
            Fused list sorted by RRF score descending, with new ranks assigned.
        """
        if not all_search_results:
            return []

        if weights is None:
            weight = 1 / len(all_search_results)
            weights = [weight] * len(all_search_results)

        scores: dict[tuple[str, ContentType], float] = {}
        docs: dict[tuple[str, ContentType], SearchResult] = {}

        for search_results, weight in zip(all_search_results, weights):
            for result in search_results:
                doc_key = (result.content_id, result.content_type)
                rrf_score = weight / (self.k + result.rank)
                scores[doc_key] = scores.get(doc_key, 0) + rrf_score
                if doc_key not in docs:
                    docs[doc_key] = result

        sorted_keys = sorted(scores, key=scores.get, reverse=True)

        return [
            SearchResult(content_id=key[0], content_type=key[1], rank=rank)
            for rank, key in enumerate(sorted_keys, start=1)
        ]
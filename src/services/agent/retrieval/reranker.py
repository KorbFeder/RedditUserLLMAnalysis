from fastembed.rerank.cross_encoder import TextCrossEncoder

from src.services.agent.comment_chain import CommentChain

class Reranker:
    def __init__(self: "Reranker", config: dict):
        self.rerank_config = config["search"]["rerank"]
        self.top_k = config["search"]["rerank"]["top_k"]
        self.candidate_limit = config["search"]["rerank"]["candidate_limit"]
        self.reranker = TextCrossEncoder(self.rerank_config['model_name'])

    def rank(self, query: str, documents: list[CommentChain]) -> list[CommentChain]:
        if not documents:
            return []

        candidates = documents[:self.candidate_limit]
        doc_strings = [doc.to_context_string() for doc in candidates]

        # FastEmbed rerank returns Iterable[float] - scores for each document
        scores = list(self.reranker.rerank(query, doc_strings))

        # Pair scores with indices and sort by score descending
        scored_indices = sorted(enumerate(scores), key=lambda x: x[1], reverse=True)

        # Return top_k documents in reranked order
        return [candidates[idx] for idx, _ in scored_indices[:self.top_k]]
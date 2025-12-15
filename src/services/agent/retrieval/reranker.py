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

        # FastEmbed rerank returns list of (score, text, index)
        results = list(self.reranker.rerank(query, doc_strings, top_k=self.top_k))

        # Return documents in reranked order
        return [candidates[result["index"]] for result in results]
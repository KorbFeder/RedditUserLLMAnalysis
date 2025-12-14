from sentence_transformers import CrossEncoder

from src.services.agent.comment_chain import CommentChain

class Reranker:
    def __init__(self: "Reranker", config: dict):
        self.rerank_config = config["search"]["rerank"]
        self.top_k = config["search"]["rerank"]["top_k"]
        self.candidate_limit = config["search"]["rerank"]["candidate_limit"]
        self.reranker = CrossEncoder(self.rerank_config['model_name'])

    def rank(self, query: str, documents: list[CommentChain]) -> list[CommentChain]:
        if not documents:
            return []

        candidates = documents[:self.candidate_limit]

        pairs = [(query, doc.to_context_string()) for doc in candidates]
        scores = self.reranker.predict(pairs)
        scored_docs = sorted(zip(candidates, scores), key=lambda x: x[1], reverse=True)
        return [doc for doc, score in scored_docs[:self.top_k]]
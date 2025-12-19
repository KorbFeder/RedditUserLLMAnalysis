from fastembed import TextEmbedding

from src.embedding.base import EmbeddingModelInfo

class FastEmbed:
    def __init__(self: "FastEmbed", config: dict, model_name: str):
        self.config = config
        self.model_name = model_name
        self.dimensions = self.config['dimensions']
        self._query_prefix = self.config.get('query_prefix', '')
        self._document_prefix = self.config.get('document_prefix', '')

        self.client = TextEmbedding(model_name=self.model_name)

    @property
    def info(self: "FastEmbed") -> EmbeddingModelInfo:
        return EmbeddingModelInfo(self.model_name, self.dimensions)

    @property
    def document_prefix(self: "FastEmbed") -> str:
        return self._document_prefix
    @property
    def query_prefix(self: "FastEmbed") -> str:
        return self._query_prefix

    def embed_documents(self, texts: list[str]) -> list[list[float]]: 
        texts = [self.document_prefix + text for text in texts]
        return list(self.client.embed(texts))

    def embed_query(self, text: str) -> list[float]:
        return list(self.client.embed([self.query_prefix + text]))[0]
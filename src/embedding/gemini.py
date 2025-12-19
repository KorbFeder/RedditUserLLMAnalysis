from google import genai
from google.genai.types import EmbedContentConfig

from src.embedding.base import EmbeddingModelInfo

class GeminiEmbedding:
    def __init__(self: "GeminiEmbedding", config: dict, model_name: str):
        self.config = config
        self.model_name = model_name
        self.dimensions = self.config['dimensions']
        self._query_prefix = self.config.get('query_prefix', '')
        self._document_prefix = self.config.get('document_prefix', '')

        self.client = genai.Client()

    @property
    def info(self) -> EmbeddingModelInfo:
        return EmbeddingModelInfo(self.model_name, self.dimensions)

    @property
    def document_prefix(self) -> str: 
        return self._document_prefix

    @property
    def query_prefix(self) -> str: 
        return self._query_prefix

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        texts = [self.document_prefix + text for text in texts]
        result = self.client.models.embed_content(
            model=self.model_name,
            contents=texts,
            config=EmbedContentConfig(
                task_type="RETRIEVAL_DOCUMENT",
                output_dimensionality=self.dimensions
            )
        )
        return [emb.values for emb in result.embeddings]

    def embed_query(self, text: str) -> list[float]:
        text = self.query_prefix + text
        result = self.client.models.embed_content(
            model=self.model_name,
            contents=[text],
            config=EmbedContentConfig(
                task_type="RETRIEVAL_QUERY",
                output_dimensionality=self.dimensions
            )
        )
        return result.embeddings[0].values
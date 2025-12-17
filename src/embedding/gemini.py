from google import genai

from src.embedding.base import EmbeddingModelInfo

class GeminiEmbedding:
    def __init__(self: "GeminiEmbedding", config: dict):
        self.gemini_config = config['embedding']['providers']['gemini']
        self.model_name = self.gemini_config['model_name']
        self.dimensions = self.gemini_config['dimensions']
        self._query_prefix = self.gemini_config['query_prefix']
        self._document_prefix = self.gemini_config['document_prefix']

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
        result = self.client.models.embed_content(
            model=self.model_name,
            contents=texts,
            config={"task_type": "RETRIEVAL_DOCUMENT"}
        )
        return [emb.values for emb in result.embeddings]

    def embed_query(self, text: str) -> list[float]:
        result = self.client.models.embed_content(
            model=self.model_name,
            contents=[text],
            config={"task_type": "RETRIEVAL_QUERY"}
        )
        return result.embeddings[0].values
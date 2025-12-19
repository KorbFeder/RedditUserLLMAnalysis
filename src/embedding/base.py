from typing import Protocol
from dataclasses import dataclass

@dataclass
class EmbeddingModelInfo:
    model_name: str
    dimensions: int

class EmbeddingStrategy(Protocol):
    @property
    def info(self) -> EmbeddingModelInfo: ...
    @property
    def document_prefix(self) -> str: ...
    @property
    def query_prefix(self) -> str: ...
    def embed_documents(self, texts: list[str]) -> list[list[float]]: ...
    def embed_query(self, text: str) -> list[float]: ...
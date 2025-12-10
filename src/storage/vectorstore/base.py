from typing import Protocol
from dataclasses import dataclass
from enum import Enum


class ContentType(Enum):
    SUBMISSION = 'submission'
    COMMENT = 'comment'


@dataclass
class SearchResult:
    content_id: str
    content_type: ContentType
    rank: int


class VectorStore(Protocol):
    def add(self, content_ids: list[str], content_types: list[ContentType], texts: list[str]) -> int:
        """Embed and store texts. Returns count of items added."""
        ...

    def dense_search(self, query_text: str, limit: int = 10) -> list[SearchResult]:
        """Embed query and perform vector similarity search."""
        ...

    def sparse_search(self, query_text: str, limit: int = 10) -> list[SearchResult] | None:
        """Full-text search. Returns None if not supported."""
        ...

    def get_existing_ids(self, content_ids: list[str], content_type: ContentType) -> set[str]:
        """Return content_ids that already have embeddings for the given type."""
        ...
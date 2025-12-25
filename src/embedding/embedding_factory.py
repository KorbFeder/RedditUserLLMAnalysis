import os
import logging

from langchain_core.embeddings import Embeddings
from langchain_community.embeddings.fastembed import FastEmbedEmbeddings
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential_jitter,
    retry_if_exception,
    before_sleep_log,
)

logger = logging.getLogger(__name__)


def _is_rate_limit_error(exception: Exception) -> bool:
    """Check if exception is a rate limit error."""
    error_str = str(exception).lower()
    return "429" in error_str or "quota" in error_str or "rate" in error_str


class RetryingEmbeddings(Embeddings):
    """Wrapper that adds exponential backoff retry on rate limit errors."""

    def __init__(self, embeddings: Embeddings, max_retries: int = 5):
        self.embeddings = embeddings
        self.max_retries = max_retries

        # Create retry decorator with exponential backoff + jitter
        self._retry_decorator = retry(
            retry=retry_if_exception(_is_rate_limit_error),
            wait=wait_exponential_jitter(initial=1, max=60, jitter=5),
            stop=stop_after_attempt(max_retries),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        @self._retry_decorator
        def _embed():
            return self.embeddings.embed_documents(texts)
        return _embed()

    def embed_query(self, text: str) -> list[float]:
        @self._retry_decorator
        def _embed():
            return self.embeddings.embed_query(text)
        return _embed()


def get_embeddings(config: dict) -> Embeddings:
    """Factory that returns LangChain embeddings based on config."""
    embedding_config = config["embedding"]
    provider = embedding_config["active_provider"]
    model = embedding_config["active_model"]

    match provider:
        case "fastembed":
            # ONNX_PROVIDERS env var enables GPU (e.g., "CUDAExecutionProvider")
            # Always include CPUExecutionProvider as fallback when GPU is requested
            onnx_provider = os.getenv("ONNX_PROVIDERS")
            if onnx_provider:
                providers = [onnx_provider, "CPUExecutionProvider"]
            else:
                providers = None  # Use ONNX default (CPU)
            return FastEmbedEmbeddings(model_name=model, providers=providers)
        case "gemini":
            base = GoogleGenerativeAIEmbeddings(
                model=f"models/{model}",
                task_type="retrieval_document",
            )
            return RetryingEmbeddings(base)
        case _:
            raise ValueError(f"Unknown embedding provider: {provider}")


def get_embedding_info(config: dict) -> tuple[str, int]:
    """Returns (model_name, dimensions) from config."""
    embedding_config = config["embedding"]
    provider = embedding_config["active_provider"]
    model = embedding_config["active_model"]
    model_config = embedding_config["providers"][provider][model]
    return model, model_config["dimensions"]

import os
import re
import random
import logging

from langchain_core.embeddings import Embeddings
from langchain_community.embeddings.fastembed import FastEmbedEmbeddings
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from tenacity import (
    retry,
    stop_after_attempt,
    retry_if_exception,
    before_sleep_log,
)

logger = logging.getLogger(__name__)


def _is_rate_limit_error(exception: Exception) -> bool:
    """Check if exception is a rate limit error."""
    error_str = str(exception).lower()
    return "429" in error_str or "quota" in error_str or "rate" in error_str


def _extract_retry_delay(exception: Exception) -> float | None:
    """
    Extract retry delay from API error response.

    Handles formats like:
    - "retry_delay { seconds: 20 }"
    - "Please retry in 20.5s"
    - "Retry-After: 30"
    """
    error_str = str(exception)

    # Gemini format: "retry_delay { seconds: 20 }"
    match = re.search(r"retry_delay\s*\{\s*seconds:\s*(\d+)", error_str)
    if match:
        return float(match.group(1))

    # Alternative format: "retry in X.Xs" or "retry in Xs"
    match = re.search(r"retry in (\d+\.?\d*)s", error_str, re.IGNORECASE)
    if match:
        return float(match.group(1))

    # HTTP header format: "Retry-After: X"
    match = re.search(r"retry-after:\s*(\d+)", error_str, re.IGNORECASE)
    if match:
        return float(match.group(1))

    return None


def _adaptive_wait(retry_state) -> float:
    """
    Custom wait function that respects API-provided retry delays.

    Falls back to exponential backoff with jitter if no delay is provided.
    """
    exception = retry_state.outcome.exception()

    # Try to extract API-provided retry delay
    retry_delay = _extract_retry_delay(exception)

    if retry_delay is not None:
        # Add small jitter (1-3s) to prevent thundering herd
        jitter = random.uniform(1, 3)
        wait_time = retry_delay + jitter
        logger.info(f"API requested {retry_delay}s delay, waiting {wait_time:.1f}s (with jitter)")
        return wait_time

    # Fallback: exponential backoff with jitter
    # Base delay doubles each attempt: 2, 4, 8, 16, 32... capped at 60s
    attempt = retry_state.attempt_number
    base_delay = min(2 ** attempt, 60)
    jitter = random.uniform(0, base_delay * 0.5)
    wait_time = base_delay + jitter
    logger.info(f"No retry delay in response, using exponential backoff: {wait_time:.1f}s")
    return wait_time


class RetryingEmbeddings(Embeddings):
    """
    Wrapper that adds adaptive retry logic for rate-limited APIs.

    Features:
    - Parses retry_delay from error responses (Gemini, OpenAI, etc.)
    - Falls back to exponential backoff with jitter
    - Configurable max retries (default 30 for ~10 min of retrying)
    """

    def __init__(self, embeddings: Embeddings, max_retries: int = 30):
        """
        Args:
            embeddings: The underlying embeddings to wrap
            max_retries: Max retry attempts (default 30 = ~10 min with 20s delays)
        """
        self.embeddings = embeddings

        self._retry_decorator = retry(
            retry=retry_if_exception(_is_rate_limit_error),
            wait=_adaptive_wait,
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

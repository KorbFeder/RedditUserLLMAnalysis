import os

from langchain_core.embeddings import Embeddings
from langchain_community.embeddings.fastembed import FastEmbedEmbeddings
from langchain_google_genai import GoogleGenerativeAIEmbeddings


def get_embeddings(config: dict) -> Embeddings:
    """Factory that returns LangChain embeddings based on config."""
    embedding_config = config["embedding"]
    provider = embedding_config["active_provider"]
    model = embedding_config["active_model"]

    match provider:
        case "fastembed":
            # ONNX_PROVIDERS env var enables GPU (e.g., "CUDAExecutionProvider")
            onnx_provider = os.getenv("ONNX_PROVIDERS")
            providers = [onnx_provider] if onnx_provider else None
            return FastEmbedEmbeddings(model_name=model, providers=providers)
        case "gemini":
            return GoogleGenerativeAIEmbeddings(
                model=f"models/{model}",
                task_type="retrieval_document",
            )
        case _:
            raise ValueError(f"Unknown embedding provider: {provider}")


def get_embedding_info(config: dict) -> tuple[str, int]:
    """Returns (model_name, dimensions) from config."""
    embedding_config = config["embedding"]
    provider = embedding_config["active_provider"]
    model = embedding_config["active_model"]
    model_config = embedding_config["providers"][provider][model]
    return model, model_config["dimensions"]

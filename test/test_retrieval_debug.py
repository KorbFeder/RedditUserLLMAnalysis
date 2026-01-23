"""Debug script to test retrieval components individually."""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from src.helpers.settings import load_config
from src.shared.session import create_db_session
from src.storage.vectorstore.pgvector import VectorStoreManager
from src.storage.vectorstore.pgsparse import PgSparseRetriever
from src.embedding.embedding_factory import get_embeddings, get_embedding_info


def test_retrievers(username: str, query: str):
    """Test dense and sparse retrievers separately."""
    config = load_config()
    session = create_db_session()

    # Setup dense retriever
    embeddings = get_embeddings(config)
    model_name, dimensions, max_text_length = get_embedding_info(config)
    vector_store = VectorStoreManager(
        embeddings=embeddings,
        model_name=model_name,
        dimensions=dimensions,
        session=session,
        max_text_length=max_text_length,
    )

    search_config = config.get("search", {})
    dense_k = search_config.get("dense", {}).get("limit", 50)
    sparse_k = search_config.get("sparse", {}).get("limit", 50)

    print(f"\n{'='*60}")
    print(f"Testing retrieval for user: {username}")
    print(f"Query: {query}")
    print(f"Dense k={dense_k}, Sparse k={sparse_k}")
    print(f"{'='*60}\n")

    # Test dense retriever
    print("--- DENSE RETRIEVER ---")
    dense = vector_store.as_retriever(
        search_kwargs={"k": dense_k, "filter": {"username": username}}
    )
    dense_docs = dense.invoke(query)
    print(f"Dense results: {len(dense_docs)}")
    for i, doc in enumerate(dense_docs[:5]):
        print(f"  [{i+1}] {doc.metadata.get('content_type')}: {doc.metadata.get('content_id')}")
        print(f"       {doc.page_content[:100]}...")

    # Test sparse retriever
    print("\n--- SPARSE RETRIEVER ---")
    sparse = PgSparseRetriever(
        session=session,
        username=username,
        k=sparse_k,
    )
    sparse_docs = sparse.invoke(query)
    print(f"Sparse results: {len(sparse_docs)}")
    for i, doc in enumerate(sparse_docs[:5]):
        print(f"  [{i+1}] {doc.metadata.get('content_type')}: {doc.metadata.get('content_id')}")
        print(f"       {doc.page_content[:100]}...")

    # Check overlap
    dense_ids = {d.metadata.get('content_id') for d in dense_docs}
    sparse_ids = {d.metadata.get('content_id') for d in sparse_docs}
    overlap = dense_ids & sparse_ids
    print(f"\n--- OVERLAP ---")
    print(f"Dense unique IDs: {len(dense_ids)}")
    print(f"Sparse unique IDs: {len(sparse_ids)}")
    print(f"Overlapping IDs: {len(overlap)}")

    session.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--username", default="swintec")
    parser.add_argument("-q", "--query", default="usenet pricing costs")
    args = parser.parse_args()

    test_retrievers(args.username, args.query)

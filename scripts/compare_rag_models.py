import argparse
import asyncio
import copy
import logging
from dataclasses import dataclass

from dotenv import load_dotenv

from src.services.ingestion.ingestion import IngestionService
from src.services.vectorizer.vectorizer import Vectorizer
from src.services.agent.retrieval.retrival import Retriever
from src.services.agent.comment_chain import CommentChain
from src.helpers.settings import load_config
from src.shared.session import create_db_session

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class ModelResult:
    """Results for a single embedding model."""
    provider: str
    model: str
    dimensions: int
    results: list[tuple[CommentChain, float]]

    @property
    def avg_score(self) -> float:
        if not self.results:
            return 0.0
        return sum(score for _, score in self.results) / len(self.results)

    @property
    def top_score(self) -> float:
        if not self.results:
            return 0.0
        return self.results[0][1] if self.results else 0.0


def get_models_to_compare(config: dict, model_filter: list[str] | None = None) -> list[dict]:
    """Extract all configured embedding models."""
    models = []
    providers = config.get("embedding", {}).get("providers", {})

    for provider_name, provider_models in providers.items():
        for model_name, model_config in provider_models.items():
            if model_filter and model_name not in model_filter:
                continue
            models.append({
                "provider": provider_name,
                "model": model_name,
                "dimensions": model_config.get("dimensions", 384)
            })

    return models


def create_config_for_model(base_config: dict, model: dict) -> dict:
    """Create a config copy with the specified model active."""
    config = copy.deepcopy(base_config)
    config["embedding"]["active_provider"] = model["provider"]
    config["embedding"]["active_model"] = model["model"]
    return config


async def run_comparison(username: str, query: str, model_filter: list[str] | None = None):
    """Run the full comparison pipeline."""
    db_session = create_db_session()
    config = load_config()

    models = get_models_to_compare(config, model_filter)
    if not models:
        print("No models found to compare. Check your config/default.yaml")
        return

    print(f"\n{'='*60}")
    print(f"RAG Model Comparison")
    print(f"{'='*60}")
    print(f"Username: {username}")
    print(f"Query: \"{query}\"")
    print(f"Models to compare: {len(models)}")
    print(f"{'='*60}\n")

    # Step 1: Ingest user data (once)
    print("[1/3] Ingesting user data...")
    ingestion = IngestionService(config, db_session)
    await ingestion.sync_users_comment_chain(username)
    print("      Done.\n")

    # Step 2: Embed with each model
    print("[2/3] Embedding with each model...")
    for model in models:
        model_config = create_config_for_model(config, model)
        print(f"      - {model['provider']}/{model['model']} ({model['dimensions']}d)...", end=" ", flush=True)
        vectorizer = Vectorizer(model_config, db_session)
        vectorizer.sync_embeddings(username)
        print("done")
    print()

    # Step 3: Run retrieval comparison
    print("[3/3] Running retrieval comparison...\n")
    model_results: list[ModelResult] = []

    for model in models:
        model_config = create_config_for_model(config, model)
        retriever = Retriever(model_config, db_session)
        results_with_scores = retriever.search_with_scores(query, username)

        model_results.append(ModelResult(
            provider=model["provider"],
            model=model["model"],
            dimensions=model["dimensions"],
            results=results_with_scores
        ))

    # Print results
    print_results(model_results, query)

    db_session.close()


def print_results(model_results: list[ModelResult], query: str):
    """Print formatted comparison results."""
    print(f"\n{'='*60}")
    print("RESULTS")
    print(f"{'='*60}\n")

    # Per-model detailed results
    for mr in model_results:
        print(f"--- {mr.provider}/{mr.model} ({mr.dimensions}d) ---")
        print(f"Avg reranker score: {mr.avg_score:.4f}")
        print(f"Top-1 score: {mr.top_score:.4f}")
        print(f"\nTop 5 results:")

        for i, (chain, score) in enumerate(mr.results[:5]):
            subreddit = chain.submission.subreddit if chain.submission else "?"
            title = chain.submission.title[:50] if chain.submission and chain.submission.title else "?"
            print(f"  {i+1}. [{score:.3f}] r/{subreddit}: \"{title}...\"")
        print()

    # Summary table
    print(f"{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}\n")

    # Sort by avg score descending
    sorted_results = sorted(model_results, key=lambda x: x.avg_score, reverse=True)

    print(f"{'Model':<35} | {'Avg Score':>10} | {'Top-1':>8} | {'Results':>8}")
    print("-" * 70)
    for mr in sorted_results:
        model_name = f"{mr.model}"
        print(f"{model_name:<35} | {mr.avg_score:>10.4f} | {mr.top_score:>8.3f} | {len(mr.results):>8}")

    # Winner
    if sorted_results:
        winner = sorted_results[0]
        print(f"\nWinner: {winner.model} (avg score: {winner.avg_score:.4f})")

    # Document overlap analysis
    if len(model_results) >= 2:
        print(f"\n--- Document Overlap Analysis ---")
        all_doc_sets = []
        for mr in model_results:
            doc_ids = set()
            for chain, _ in mr.results:
                if chain.comments:
                    doc_ids.add(chain.comments[0].id)
                elif chain.submission:
                    doc_ids.add(chain.submission.id)
            all_doc_sets.append((mr.model, doc_ids))

        # Compare first two models
        m1_name, m1_docs = all_doc_sets[0]
        m2_name, m2_docs = all_doc_sets[1]
        overlap = m1_docs & m2_docs
        total = m1_docs | m2_docs
        if total:
            overlap_pct = len(overlap) / len(total) * 100
            print(f"{m1_name} vs {m2_name}: {len(overlap)}/{len(total)} documents overlap ({overlap_pct:.1f}%)")


def main():
    parser = argparse.ArgumentParser(description="Compare RAG retrieval across embedding models")
    parser.add_argument("--username", "-u", required=True, help="Reddit username to analyze")
    parser.add_argument("--query", "-q", required=True, help="Query to search for")
    parser.add_argument("--models", "-m", help="Comma-separated list of models to compare (default: all)")

    args = parser.parse_args()

    model_filter = None
    if args.models:
        model_filter = [m.strip() for m in args.models.split(",")]

    asyncio.run(run_comparison(args.username, args.query, model_filter))


if __name__ == "__main__":
    main()

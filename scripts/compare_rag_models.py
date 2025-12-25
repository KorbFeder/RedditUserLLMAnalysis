import argparse
import asyncio
import copy
import json
import logging
import os
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path

import yaml
from dotenv import load_dotenv

from src.services.ingestion.ingestion import IngestionService
from src.services.vectorizer.vectorizer import Vectorizer
from src.services.agent.retrieval.retrival import Retriever
from src.services.agent.comment_chain import CommentChain
from src.helpers.settings import load_config
from src.shared.session import create_db_session
from src.reddit_providers.source_factory import create_current_source, create_historical_source

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class TestCase:
    """A single test case: username + query pair."""
    username: str
    query: str


@dataclass
class ModelResult:
    """Results for a single embedding model on a single test case."""
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

    def to_dict(self) -> dict:
        """Convert to serializable dict."""
        return {
            "provider": self.provider,
            "model": self.model,
            "dimensions": self.dimensions,
            "avg_score": self.avg_score,
            "top_score": self.top_score,
            "num_results": len(self.results),
            "top_5_results": [
                {
                    "score": score,
                    "subreddit": chain.submission.subreddit if chain.submission else None,
                    "title": chain.submission.title[:100] if chain.submission and chain.submission.title else None,
                    "comment_preview": chain.comments[0].body[:200] if chain.comments and chain.comments[0].body else None
                }
                for chain, score in self.results[:5]
            ]
        }


@dataclass
class TestCaseResult:
    """Results for all models on a single test case."""
    username: str
    query: str
    model_results: list[ModelResult]

    def to_dict(self) -> dict:
        return {
            "username": self.username,
            "query": self.query,
            "model_results": [mr.to_dict() for mr in self.model_results]
        }


@dataclass
class BenchmarkResult:
    """Complete benchmark results."""
    timestamp: str
    test_cases: list[TestCaseResult]
    summary: dict

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "test_cases": [tc.to_dict() for tc in self.test_cases],
            "summary": self.summary
        }


def load_benchmark_config(config_path: str) -> dict:
    """Load benchmark configuration from YAML file."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


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


async def ingest_users(usernames: list[str], config: dict, db_session) -> None:
    """Ingest data for all unique usernames."""
    current_source = create_current_source(config)
    historical_source = create_historical_source(config)
    ingestion = IngestionService(db_session, current_source, historical_source)

    for i, username in enumerate(usernames, 1):
        logger.info(f"[{i}/{len(usernames)}] Ingesting data for user: {username}")
        await ingestion.sync_users_comment_chain(username)
        logger.info(f"[{i}/{len(usernames)}] Completed ingestion for user: {username}")

    await ingestion.close()


def embed_users(usernames: list[str], models: list[dict], config: dict, db_session) -> None:
    """Embed all users with all models."""
    total_models = len(models)
    total_users = len(usernames)

    for m_idx, model in enumerate(models, 1):
        model_config = create_config_for_model(config, model)
        logger.info(f"[Model {m_idx}/{total_models}] Embedding with {model['provider']}/{model['model']} ({model['dimensions']}d)...")
        vectorizer = Vectorizer(model_config, db_session)

        for u_idx, username in enumerate(usernames, 1):
            logger.info(f"  [User {u_idx}/{total_users}] Embedding {username}...")
            vectorizer.sync_embeddings(username)
            logger.info(f"  [User {u_idx}/{total_users}] Completed {username}")

        logger.info(f"[Model {m_idx}/{total_models}] Completed {model['model']}")


def run_retrieval(test_case: TestCase, models: list[dict], config: dict, db_session) -> TestCaseResult:
    """Run retrieval for a single test case across all models."""
    model_results = []
    total_models = len(models)

    logger.info(f"  Query: \"{test_case.query[:60]}...\"")

    for m_idx, model in enumerate(models, 1):
        logger.info(f"    [{m_idx}/{total_models}] Searching with {model['model']}...")
        model_config = create_config_for_model(config, model)
        retriever = Retriever(model_config, db_session)
        results_with_scores = retriever.search_with_scores(test_case.query, test_case.username)

        mr = ModelResult(
            provider=model["provider"],
            model=model["model"],
            dimensions=model["dimensions"],
            results=results_with_scores
        )
        model_results.append(mr)
        logger.info(f"    [{m_idx}/{total_models}] {model['model']}: top_score={mr.top_score:.4f}, avg={mr.avg_score:.4f}, n={len(mr.results)}")

    return TestCaseResult(
        username=test_case.username,
        query=test_case.query,
        model_results=model_results
    )


def compute_summary(test_case_results: list[TestCaseResult]) -> dict:
    """Compute summary statistics across all test cases."""
    model_scores: dict[str, list[float]] = {}

    for tcr in test_case_results:
        for mr in tcr.model_results:
            if mr.model not in model_scores:
                model_scores[mr.model] = []
            model_scores[mr.model].append(mr.avg_score)

    summary = {
        "models": {}
    }

    for model, scores in model_scores.items():
        summary["models"][model] = {
            "mean_avg_score": sum(scores) / len(scores) if scores else 0,
            "num_test_cases": len(scores)
        }

    # Determine winner
    if summary["models"]:
        winner = max(summary["models"].items(), key=lambda x: x[1]["mean_avg_score"])
        summary["winner"] = winner[0]
        summary["winner_score"] = winner[1]["mean_avg_score"]

    return summary


def save_results(result: BenchmarkResult, output_dir: str, format: str = "json") -> str:
    """Save benchmark results to file."""
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"benchmark_{timestamp}.{format}"
    filepath = os.path.join(output_dir, filename)

    if format == "json":
        with open(filepath, 'w') as f:
            json.dump(result.to_dict(), f, indent=2)
    elif format == "csv":
        # Flatten to CSV format
        import csv
        with open(filepath, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["username", "query", "model", "avg_score", "top_score", "num_results"])
            for tc in result.test_cases:
                for mr in tc.model_results:
                    writer.writerow([
                        tc.username,
                        tc.query,
                        mr.model,
                        f"{mr.avg_score:.4f}",
                        f"{mr.top_score:.4f}",
                        len(mr.results)
                    ])

    return filepath


def print_results(result: BenchmarkResult) -> None:
    """Print formatted results to console."""
    print(f"\n{'='*70}")
    print("BENCHMARK RESULTS")
    print(f"{'='*70}\n")

    for tc_result in result.test_cases:
        print(f"--- User: {tc_result.username} ---")
        print(f"Query: \"{tc_result.query}\"\n")

        for mr in tc_result.model_results:
            print(f"  {mr.model} ({mr.dimensions}d)")
            print(f"    Avg score: {mr.avg_score:.4f}, Top-1: {mr.top_score:.4f}, Results: {len(mr.results)}")

            if mr.results:
                chain, score = mr.results[0]
                subreddit = chain.submission.subreddit if chain.submission else "?"
                title = chain.submission.title[:50] if chain.submission and chain.submission.title else "?"
                print(f"    Top result: [{score:.3f}] r/{subreddit}: \"{title}...\"")
            print()

    print(f"{'='*70}")
    print("SUMMARY")
    print(f"{'='*70}\n")

    print(f"{'Model':<35} | {'Mean Avg Score':>15} | {'Test Cases':>10}")
    print("-" * 65)

    for model, stats in sorted(result.summary["models"].items(), key=lambda x: x[1]["mean_avg_score"], reverse=True):
        print(f"{model:<35} | {stats['mean_avg_score']:>15.4f} | {stats['num_test_cases']:>10}")

    if "winner" in result.summary:
        print(f"\nWinner: {result.summary['winner']} (mean avg score: {result.summary['winner_score']:.4f})")


async def run_benchmark(benchmark_config_path: str, app_config_path: str | None = None) -> BenchmarkResult:
    """Run the full benchmark pipeline."""
    # Load configs
    benchmark_cfg = load_benchmark_config(benchmark_config_path)
    app_config = load_config(app_config_path) if app_config_path else load_config()

    # Parse test cases
    test_cases = [
        TestCase(username=tc["username"], query=tc["query"])
        for tc in benchmark_cfg.get("test_cases", [])
    ]

    if not test_cases:
        raise ValueError("No test cases defined in benchmark config")

    # Get unique usernames
    usernames = list(set(tc.username for tc in test_cases))

    # Get models to compare
    model_filter = benchmark_cfg.get("models") or None
    if model_filter and len(model_filter) == 0:
        model_filter = None
    models = get_models_to_compare(app_config, model_filter)

    if not models:
        raise ValueError("No models found to compare")

    print(f"\n{'='*70}")
    print("RAG BENCHMARK")
    print(f"{'='*70}")
    print(f"Test cases: {len(test_cases)}")
    print(f"Unique users: {len(usernames)} ({', '.join(usernames)})")
    print(f"Models: {len(models)}")
    print(f"{'='*70}\n")

    db_session = create_db_session()

    # Step 1: Ingest all users
    print("[1/3] Ingesting user data...")
    await ingest_users(usernames, app_config, db_session)
    print("      Done.\n")

    # Step 2: Embed with all models
    print("[2/3] Embedding with all models...")
    embed_users(usernames, models, app_config, db_session)
    print("      Done.\n")

    # Step 3: Run retrieval for each test case
    print("[3/3] Running retrieval comparisons...")
    test_case_results = []
    total_cases = len(test_cases)
    for tc_idx, tc in enumerate(test_cases, 1):
        logger.info(f"[Test {tc_idx}/{total_cases}] User: {tc.username}")
        result = run_retrieval(tc, models, app_config, db_session)
        test_case_results.append(result)
        logger.info(f"[Test {tc_idx}/{total_cases}] Completed")
    print("      Done.\n")

    db_session.close()

    # Compute summary
    summary = compute_summary(test_case_results)

    return BenchmarkResult(
        timestamp=datetime.now().isoformat(),
        test_cases=test_case_results,
        summary=summary
    )


def main():
    parser = argparse.ArgumentParser(description="Run RAG benchmark across embedding models")
    parser.add_argument(
        "--config", "-c",
        default="config/benchmark.yaml",
        help="Path to benchmark config file (default: config/benchmark.yaml)"
    )
    parser.add_argument(
        "--output", "-o",
        help="Output directory for results (overrides config)"
    )
    parser.add_argument(
        "--format", "-f",
        choices=["json", "csv"],
        help="Output format (overrides config)"
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Don't save results to file, only print"
    )

    # Legacy single-run arguments
    parser.add_argument("--username", "-u", help="Single username (legacy mode)")
    parser.add_argument("--query", "-q", help="Single query (legacy mode)")
    parser.add_argument("--models", "-m", help="Comma-separated list of models")

    args = parser.parse_args()

    # Legacy mode: single username + query
    if args.username and args.query:
        # Create temporary benchmark config
        benchmark_cfg = {
            "test_cases": [{"username": args.username, "query": args.query}],
            "models": [m.strip() for m in args.models.split(",")] if args.models else [],
            "output": {"directory": "benchmark_results", "format": "json"}
        }

        # Save temp config
        temp_config_path = "config/_temp_benchmark.yaml"
        with open(temp_config_path, 'w') as f:
            yaml.dump(benchmark_cfg, f)

        result = asyncio.run(run_benchmark(temp_config_path))
        os.remove(temp_config_path)
    else:
        # Normal mode: use config file
        result = asyncio.run(run_benchmark(args.config))

    # Print results
    print_results(result)

    # Save results
    if not args.no_save:
        benchmark_cfg = load_benchmark_config(args.config) if os.path.exists(args.config) else {}
        output_cfg = benchmark_cfg.get("output", {})

        output_dir = args.output or output_cfg.get("directory", "benchmark_results")
        output_format = args.format or output_cfg.get("format", "json")

        filepath = save_results(result, output_dir, output_format)
        print(f"\nResults saved to: {filepath}")


if __name__ == "__main__":
    main()

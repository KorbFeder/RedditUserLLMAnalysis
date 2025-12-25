#!/bin/bash
# Run RAG benchmark with GPU support in Docker
#
# Usage:
#   ./scripts/run_benchmark.sh                    # Run with default config
#   ./scripts/run_benchmark.sh -u spez -q "API"   # Run single test

set -e

# Ensure we're in the project root
cd "$(dirname "$0")/.."

# Make sure the database is running
echo "Ensuring database is running..."
docker-compose up -d db

# Wait for db to be healthy
echo "Waiting for database..."
sleep 5

# Run the benchmark
echo "Running benchmark with GPU..."
docker-compose -f docker-compose.yml -f scripts/docker-compose.benchmark.yml run --rm benchmark "$@"

echo ""
echo "Results saved to: benchmark_results/"

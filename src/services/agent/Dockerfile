FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN pip install poetry

# Copy dependency files
COPY pyproject.toml poetry.lock* ./

# Install dependencies (no venv in container)
# Core + ML (embeddings/reranking) + LLM (langchain for analysis)
RUN poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-ansi --no-root --with ml,llm

# Copy source code
COPY src/ ./src/
COPY config/ ./config/

# Set Python path
ENV PYTHONPATH=/app

# Run the agent worker
CMD ["faststream", "run", "src.services.agent.main:app"]

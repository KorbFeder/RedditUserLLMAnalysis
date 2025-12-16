# SentimentAgent

AI agent for Reddit user sentiment analysis using RAG-based LLM analysis.

## Quick Start

```bash
# Install dependencies
poetry install

# Start all services (PostgreSQL, RabbitMQ, workers)
docker-compose up -d

# Start with GPU support for vectorizer
docker-compose -f docker-compose.yml -f docker-compose.gpu.yml up -d

# Run the sentiment agent
poetry run python main.py
```

## Running Tests

```bash
# Run all tests
poetry run python -m pytest test/

# Run with verbose output
poetry run python -m pytest test/ -v

# Run a specific test file
poetry run python -m pytest test/test_ingestion.py -v

# Run tests matching a pattern
poetry run python -m pytest test/ -k "test_cache" -v
```

## Service UIs & Endpoints

| Service | URL | Credentials |
|---------|-----|-------------|
| RabbitMQ Management UI | http://localhost:15672 | `guest` / `guest` |
| PostgreSQL | localhost:5432 | `reddit` / `reddit` |

### RabbitMQ Management UI

Access at **http://localhost:15672** to:
- View queue status (`ingestion`, `vectorizer`, `agent`)
- Monitor message rates and connections
- Publish test messages manually
- Inspect message contents

## Docker Management

```bash
# Build/rebuild all containers
docker-compose build

# Rebuild a specific service
docker-compose build vectorizer
docker-compose build ingestion
docker-compose build agent

# Rebuild and restart
docker-compose up -d --build

# Rebuild without cache (fresh build)
docker-compose build --no-cache

# Stop all services
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v
```

### GPU Build Commands

```bash
# Rebuild with GPU support
docker-compose -f docker-compose.yml -f docker-compose.gpu.yml build

# Rebuild and restart with GPU
docker-compose -f docker-compose.yml -f docker-compose.gpu.yml up -d --build

# Rebuild specific service with GPU
docker-compose -f docker-compose.yml -f docker-compose.gpu.yml build vectorizer
```

## Debugging Individual Services

### View Service Logs

```bash
# Follow logs for a specific service
docker logs -f vectorizer_worker
docker logs -f ingestion_worker
docker logs -f agent_worker

# View all logs
docker-compose logs -f
```

### Run a Service Locally (for debugging)

```bash
# Stop the containerized service
docker stop vectorizer_worker

# Run locally with your debugger
poetry run python -m src.services.vectorizer.main
poetry run python -m src.services.ingestion.main
poetry run python -m src.services.agent.main
```

### Debug with Breakpoints (VS Code + Docker)

Debug services running in containers with full breakpoint support:

```bash
# Start all services in debug mode
docker-compose -f docker-compose.yml -f docker-compose.debug.yml up --build
```

**Debug ports:**
| Service | Port |
|---------|------|
| Ingestion | 5678 |
| Vectorizer | 5679 |
| Agent | 5680 |

**Workflow for debugging the full pipeline:**
1. Start containers (they run immediately with debugpy listening)
2. Set breakpoints in your code
3. Open VS Code Run and Debug panel (Ctrl+Shift+D)
4. Select **"Docker: All Workers"** and press F5 (attaches to all 3 services)
5. Trigger a job with `python main.py`
6. Breakpoints hit as the message flows through each service

**To debug a single service:**
- Select "Docker: Ingestion", "Docker: Vectorizer", or "Docker: Agent" instead

### Publish Test Messages

**Job Message Format:**
```json
{"job_id": "test-1", "username": "some_reddit_user"}
```

Via RabbitMQ UI:
1. Go to http://localhost:15672 -> Queues -> select queue (`ingestion`, `vectorizer`, or `agent`)
2. Expand "Publish message"
3. Enter the JSON payload above
4. Click "Publish message"

### Check Queue Status

```bash
docker exec rabbitmq rabbitmqctl list_queues name messages
```

## Environment Variables

Create a `.env` file:

```
OPENROUTER_API_KEY=<your-key>
DATABASE_URL=postgresql://reddit:reddit@localhost:5432/reddit_analysis
RABBITMQ_URL=amqp://guest:guest@localhost:5672
REDDIT_ID=<optional>
REDDIT_SECRET=<optional>
```

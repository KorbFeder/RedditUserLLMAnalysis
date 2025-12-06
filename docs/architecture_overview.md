# RedditUserLLMAnalysis - Architecture Overview

## Purpose

This system analyzes Reddit users by fetching their complete contribution history, storing it in a searchable format, and enabling LLM-based analysis of their content patterns and sentiment.

## High-Level Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   PullPush API  │────▶│  Repository │────▶│   Vectorizer   │
│ (api.pullpush.io)│     │  (Orchestration)  │     │ (Document Gen)  │
└─────────────────┘     └────────┬─────────┘     └────────┬────────┘
                                 │                        │
                                 ▼                        ▼
                        ┌────────────────┐       ┌────────────────┐
                        │  PostgresStore   │       │ VectorStore│
                        │  (PostgreSQL)  │       │   (ChromaDB)   │
                        └────────────────┘       └────────────────┘
```

## Data Flow

### 1. User Analysis Pipeline

```
store_user_data("username")
    │
    ├── 1. Fetch user's submissions and comments
    │       └── get_user_contributions() → (submissions[], comments[])
    │
    ├── 2. Extract unique thread IDs from user's activity
    │
    ├── 3. For each thread:
    │       └── get_thread() → (submission, all_comments[])
    │
    └── 4. Convert to document with comment tree
            └── _convert_thread_to_document() → store in ChromaDB
```

### 2. Fetching Strategy

The system uses **streaming generators** to handle pagination from the PullPush API:

```python
# Data comes in batches, yielded as they arrive
for batch in push_pull.stream_user_comments(username):
    for comment in batch:
        process(comment)
```

## Caching System

### The Problem We Solved

When analyzing a user, we first cache their comments. Later, when fetching the full thread, we need ALL comments (not just theirs) to build the comment tree correctly.

**Bug scenario (before fix):**
1. Fetch user "alice" → cache her comment on thread X
2. Fetch thread X → see cached comment, stop early
3. Result: Missing parent comments from other users → broken comment tree

### The Solution: Context-Aware Caching

We track **what type of fetch** populated the cache:

```
┌─────────────────────────────────────┐
│     user_contribution_cache_status   │
├─────────────────────────────────────┤
│ username (PK)                        │
│ newest_submission_cursor (timestamp) │
│ newest_comment_cursor (timestamp)    │
└─────────────────────────────────────┘

┌─────────────────────────────────────┐
│        thread_cache_status           │
├─────────────────────────────────────┤
│ submission_id (PK)                   │
│ newest_item_cursor (timestamp)       │
│ is_history_complete (boolean)        │  ← KEY FLAG
└─────────────────────────────────────┘
```

**Logic:**
- User fetch: Caches comments but does NOT set `is_history_complete`
- Thread fetch: Checks `is_history_complete`
  - If `false` or missing → fetch ALL comments (ignore partial cache)
  - If `true` → incremental fetch (only newer comments)
- After full thread fetch → set `is_history_complete = true`

### Cache Modes

```python
class CacheConfig(Enum):
    DEFAULT = 0    # API + cache with incremental fetching
    NO_CACHE = 1   # API only, nothing persisted
    CACHE_ONLY = 2 # Cache only, no API calls
    FULL_SAVE = 3  # Fetch all, save all (ignores existing cache)
```

## Database Schema

### PostgreSQL (Cache Layer)

```sql
-- Raw Reddit data
submissions (id PK, author, subreddit, title, selftext, score, ..., raw_json JSONB)
comments (id PK, submission_id, parent_id, author, body, score, ..., raw_json JSONB)

-- Cache metadata
user_contribution_cache_status (username PK, newest_submission_cursor, newest_comment_cursor)
thread_cache_status (submission_id PK, newest_item_cursor, is_history_complete)
```

Key indexes: `author`, `submission_id`, `parent_id`, `created_utc`

### ChromaDB (Vector Layer)

Stores thread documents as embeddings using `nomic-ai/nomic-embed-text-v1.5` model for semantic search.

## Comment Tree Building

Comments reference parents via `parent_id` (either submission ID or another comment ID):

```
Submission: "xyz"
├── Comment "c1" (parent: xyz) by bob
│   └── Comment "c2" (parent: c1) by charlie
│       └── Comment "c3" (parent: c2) by alice
```

The `_order_comments()` method builds this tree:

```python
def _order_comments(submission_id, comments):
    nodes = {c.id: CommentNode(comment=c) for c in comments}
    root = []

    for comment in comments:
        if parent_id == submission_id:
            root.append(nodes[comment.id])      # Top-level comment
        elif parent_id in nodes:
            nodes[parent_id].replies.append(nodes[comment.id])  # Nested reply
        else:
            root.append(nodes[comment.id])      # Orphaned (parent missing)

    return root
```

## Key Design Decisions

### 1. Timestamp-based Cursors (not ID-based)

We use `created_utc` timestamps instead of IDs for incremental fetching because:
- PullPush API returns data sorted by timestamp
- Timestamps are monotonically increasing
- Simpler comparison logic: `if comment.created_utc <= cursor: stop`

### 2. Raw JSON Preservation

Every submission/comment stores the complete API response in `raw_json JSONB`:
- Future-proofs against schema changes
- Enables reprocessing without re-fetching
- Debugging aid for API response analysis

### 3. SQLAlchemy with MappedAsDataclass

Models serve dual purpose:
```python
class Comment(MappedAsDataclass, Base):
    # Works as SQLAlchemy ORM model
    # AND as a typed dataclass for passing around
```

### 4. Streaming Generators for Memory Efficiency

Large users may have thousands of comments. Streaming prevents loading everything into memory:
```python
def stream_user_comments(username):
    while has_more:
        batch = fetch_batch()
        yield batch  # Process batch, then fetch next
```

## Component Responsibilities

| Component | Responsibility |
|-----------|---------------|
| `PullPushClient` | API client, pagination, rate limiting |
| `Repository` | Cache logic, fetch orchestration, mode handling |
| `PostgresStore` | Database operations, UPSERT logic |
| `Vectorizer` | High-level workflows, document generation |
| `VectorStore` | Embedding storage and retrieval |

## Configuration

```yaml
# config/default.yaml
database:
  embedding_model: "nomic-ai/nomic-embed-text-v1.5"

reddit_api:
  pushpull:
    rate_limit: 1.0      # seconds between requests
    batch_size: 100      # items per API call

use_cache: 0  # 0=DEFAULT, 1=NO_CACHE, 2=CACHE_ONLY, 3=FULL_SAVE
```

## Testing Strategy

- **Unit tests** (`test_cache_fix.py`): Mock-based tests for cache logic
- **Integration tests** (`test_integration.py`): Real API calls to verify data flow
- **Verification tests** (`test_verification.py`): Data integrity checks

Key test: `test_full_bug_scenario_with_mocks` - Simulates the exact bug scenario that led to the `is_history_complete` fix.

## Future Considerations

1. **Rate limiting**: Currently simple sleep-based; could add exponential backoff
2. **Deleted content**: PullPush may have data Reddit has since deleted
3. **Real-time updates**: Current design is batch-oriented, not streaming
4. **Multi-user analysis**: Currently processes one user at a time

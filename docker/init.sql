CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE submissions (
    id TEXT PRIMARY KEY,
    author TEXT,
    subreddit TEXT,
    title TEXT,
    selftext TEXT,
    url TEXT,
    score INTEGER,
    ups INTEGER,
    upvote_ratio REAL,
    num_comments INTEGER,
    gilded INTEGER,
    all_awardings JSONB,
    created_utc BIGINT,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    is_archived BOOLEAN NOT NULL DEFAULT TRUE,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    raw_json JSONB NOT NULL
);

CREATE TABLE comments (
    id TEXT PRIMARY KEY,
    submission_id TEXT,
    parent_id TEXT,
    author TEXT,
    body TEXT,
    score INTEGER,
    ups INTEGER,
    gilded INTEGER,
    all_awardings JSONB,
    created_utc BIGINT,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    is_archived BOOLEAN NOT NULL DEFAULT TRUE,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    raw_json JSONB NOT NULL
);

CREATE TABLE thread_cache_status (
    submission_id TEXT PRIMARY KEY,
    newest_item_cursor BIGINT,
    is_history_complete BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE user_contribution_cache_status (
    username TEXT PRIMARY KEY,
    newest_submission_cursor BIGINT,
    newest_comment_cursor BIGINT
);

CREATE INDEX idx_submissions_author ON submissions(author);
CREATE INDEX idx_submissions_subreddit ON submissions(subreddit);
CREATE INDEX idx_submissions_created_utc ON submissions(created_utc);

CREATE INDEX idx_comments_submission_id ON comments(submission_id);
CREATE INDEX idx_comments_author ON comments(author);
CREATE INDEX idx_comments_parent_id ON comments(parent_id);
CREATE INDEX idx_comments_created_utc ON comments(created_utc);

-- Embeddings table for pgvector
CREATE TABLE embeddings (
    id SERIAL PRIMARY KEY,
    content_id TEXT NOT NULL,
    content_type TEXT NOT NULL CHECK (content_type IN ('submission', 'comment')),
    embedding vector(768),
    UNIQUE(content_id, content_type)
);

-- Vector similarity search index (HNSW)
CREATE INDEX idx_embeddings_vector ON embeddings USING hnsw (embedding vector_cosine_ops);
CREATE INDEX idx_embeddings_content ON embeddings(content_id, content_type);

-- Full-text search for hybrid search
ALTER TABLE submissions ADD COLUMN search_vector tsvector
    GENERATED ALWAYS AS (to_tsvector('english', coalesce(title, '') || ' ' || coalesce(selftext, ''))) STORED;
ALTER TABLE comments ADD COLUMN search_vector tsvector
    GENERATED ALWAYS AS (to_tsvector('english', coalesce(body, ''))) STORED;

CREATE INDEX idx_submissions_fts ON submissions USING GIN (search_vector);
CREATE INDEX idx_comments_fts ON comments USING GIN (search_vector);

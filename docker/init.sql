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
    subreddit TEXT,
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

CREATE TABLE jobs (
    id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    result TEXT,
    error TEXT,
    created_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_jobs_status ON jobs(status);
CREATE INDEX idx_jobs_job_type ON jobs(job_type);
CREATE INDEX idx_jobs_created_utc ON jobs(created_utc);

CREATE TABLE user_sentiment_jobs (
    job_id TEXT PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
    username TEXT NOT NULL,
    question TEXT NOT NULL,
    service TEXT NOT NULL DEFAULT 'pending'
);

CREATE TABLE subreddit_sentiment_jobs (
    job_id TEXT PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
    subreddit TEXT NOT NULL,
    question TEXT NOT NULL,
    service TEXT NOT NULL DEFAULT 'pending'
);

CREATE TABLE user_contribution_cache_status (
    username TEXT PRIMARY KEY,
    newest_submission_cursor BIGINT,
    newest_comment_cursor BIGINT
);

CREATE TABLE subreddit_contribution_cache_status (
    subreddit TEXT PRIMARY KEY,
    newest_submission_cursor BIGINT,
    newest_comment_cursor BIGINT
);

CREATE TABLE embedding_records (
    content_id TEXT NOT NULL,
    collection_name TEXT NOT NULL,
    content_type TEXT NOT NULL,
    username TEXT NOT NULL,
    subreddit TEXT NOT NULL DEFAULT '',
    embedded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (content_id, collection_name)
);

CREATE INDEX idx_embedding_records_collection ON embedding_records(collection_name);
CREATE INDEX idx_embedding_records_username ON embedding_records(username);
CREATE INDEX idx_embedding_records_subreddit ON embedding_records(subreddit);

CREATE INDEX idx_submissions_author ON submissions(author);
CREATE INDEX idx_submissions_subreddit ON submissions(subreddit);
CREATE INDEX idx_submissions_created_utc ON submissions(created_utc);

CREATE INDEX idx_comments_submission_id ON comments(submission_id);
CREATE INDEX idx_comments_author ON comments(author);
CREATE INDEX idx_comments_parent_id ON comments(parent_id);
CREATE INDEX idx_comments_created_utc ON comments(created_utc);
CREATE INDEX idx_comments_subreddit ON comments(subreddit);
CREATE INDEX idx_comments_subreddit_created ON comments(subreddit, created_utc);

-- Full-text search for hybrid search
-- Note: Embedding tables are created dynamically by LangChain PGVectorStore per model
ALTER TABLE submissions ADD COLUMN search_vector tsvector
    GENERATED ALWAYS AS (to_tsvector('english', coalesce(title, '') || ' ' || coalesce(selftext, ''))) STORED;
ALTER TABLE comments ADD COLUMN search_vector tsvector
    GENERATED ALWAYS AS (to_tsvector('english', coalesce(body, ''))) STORED;

CREATE INDEX idx_submissions_fts ON submissions USING GIN (search_vector);
CREATE INDEX idx_comments_fts ON comments USING GIN (search_vector);

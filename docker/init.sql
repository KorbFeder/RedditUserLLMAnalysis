CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE submissions (
    id TEXT PRIMARY KEY,
    author TEXT NOT NULL,
    subreddit TEXT NOT NULL,
    title TEXT NOT NULL,
    selftext TEXT,
    url TEXT,
    score INTEGER,
    ups INTEGER,
    upvote_ratio REAL,
    num_comments INTEGER,
    gilded INTEGER,
    all_awardings JSONB,
    created_utc BIGINT NOT NULL,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    raw_json JSONB NOT NULL
);

CREATE TABLE comments (
    id TEXT PRIMARY KEY,
    submission_id TEXT NOT NULL REFERENCES submissions(id),
    parent_id TEXT,
    author TEXT NOT NULL,
    body TEXT NOT NULL,
    score INTEGER,
    ups INTEGER,
    gilded INTEGER,
    all_awardings JSONB,
    created_utc BIGINT NOT NULL,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    raw_json JSONB NOT NULL
);

CREATE INDEX idx_submissions_author ON submissions(author);
CREATE INDEX idx_submissions_subreddit ON submissions(subreddit);
CREATE INDEX idx_submissions_created_utc ON submissions(created_utc);

CREATE INDEX idx_comments_submission_id ON comments(submission_id);
CREATE INDEX idx_comments_author ON comments(author);
CREATE INDEX idx_comments_parent_id ON comments(parent_id);
CREATE INDEX idx_comments_created_utc ON comments(created_utc);

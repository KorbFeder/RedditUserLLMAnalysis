# The Bug
When building comment trees, parent comments are missing, causing orphaned comments to appear at root level (wrong tree structure).

## Root Cause

1. get_user_contributions('xyz') fetches xyz's comments across ALL threads (partial per-thread data)
2. These get cached in the database
3. Later, get_thread('abc123') sees cached comments, uses stop_at_id logic, stops fetching early
4. Parent comments (written by OTHER users) are never fetched
5. Result: incomplete trees with orphans

## Why stop_at_id Fails

The stop_at_id logic assumes: "if comment X is cached, all older comments for this thread are also cached"

- TRUE for user-based fetching (complete per-user)
- FALSE for thread-based fetching (partial per-thread when user comments are cached)
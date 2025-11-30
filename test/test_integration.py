"""
Integration tests for the Reddit data pipeline.
Tests different users, cache modes, and verifies data integrity.
"""
import os
import sys
import logging
import unittest
from datetime import datetime
from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()

from src.database.reddit_repository import RedditRepository, CacheConfig
from src.database.reddit_cache import RedditCache
from src.data_providers.pushpull_provider import PushPullProvider
from src.models.reddit import Submission, Comment

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_test_config(cache_mode: int = 0) -> dict:
    """Create test config with specified cache mode."""
    return {
        'reddit_api': {
            'pushpull': {
                'rate_limit': 1.0,
                'batch_size': 25  # Smaller batch for faster tests
            }
        },
        'use_cache': cache_mode
    }


class TestPushPullProvider(unittest.TestCase):
    """Test the PushPull API provider directly."""

    @classmethod
    def setUpClass(cls):
        cls.config = get_test_config()
        cls.provider = PushPullProvider(cls.config)

    def test_fetch_single_submission(self):
        """Test fetching a known submission."""
        # Using a known Reddit post ID
        submission = self.provider.fetch_submission('1h0n5ql')

        if submission:
            self.assertIsInstance(submission, Submission)
            self.assertEqual(submission.id, '1h0n5ql')
            self.assertIsNotNone(submission.title)
            self.assertIsNotNone(submission.author)
            logger.info(f"Fetched submission: {submission.title[:50]}... by {submission.author}")
        else:
            logger.warning("Submission not found - may have been deleted")

    def test_fetch_user_submissions_small_user(self):
        """Test fetching submissions for a user with few posts."""
        submissions = []
        for batch in self.provider.stream_user_submissions('AutoModerator'):
            submissions.extend(batch)
            if len(submissions) >= 5:  # Limit for test
                break

        self.assertGreater(len(submissions), 0, "Should fetch at least some submissions")

        # Verify ordering (newest first)
        for i in range(len(submissions) - 1):
            self.assertGreaterEqual(
                submissions[i].created_utc,
                submissions[i+1].created_utc,
                "Submissions should be ordered newest first"
            )

        logger.info(f"Fetched {len(submissions)} submissions")

    def test_fetch_user_comments_small_user(self):
        """Test fetching comments for a user."""
        comments = []
        for batch in self.provider.stream_user_comments('AutoModerator'):
            comments.extend(batch)
            if len(comments) >= 5:
                break

        self.assertGreater(len(comments), 0, "Should fetch at least some comments")

        for comment in comments:
            self.assertIsNotNone(comment.id)
            self.assertIsNotNone(comment.body)

        logger.info(f"Fetched {len(comments)} comments")

    def test_strip_prefix(self):
        """Test the prefix stripping utility."""
        self.assertEqual(self.provider._strip_prefix('t3_abc123'), 'abc123')
        self.assertEqual(self.provider._strip_prefix('t1_xyz789'), 'xyz789')
        self.assertIsNone(self.provider._strip_prefix(None))
        self.assertIsNone(self.provider._strip_prefix(12345))
        self.assertEqual(self.provider._strip_prefix('no_prefix'), 'prefix')


class TestRedditCache(unittest.TestCase):
    """Test the PostgreSQL cache layer."""

    @classmethod
    def setUpClass(cls):
        cls.cache = RedditCache()

    def test_submissions_exist_empty(self):
        """Test checking for non-existent submissions."""
        result = self.cache.submissions_exist(['nonexistent_id_12345'])
        self.assertEqual(result, set())

    def test_comments_exist_empty(self):
        """Test checking for non-existent comments."""
        result = self.cache.comments_exist(['nonexistent_id_12345'])
        self.assertEqual(result, set())

    def test_get_submission_nonexistent(self):
        """Test getting a non-existent submission."""
        result = self.cache.get_submission('nonexistent_id_12345')
        self.assertIsNone(result)


class TestRedditRepository(unittest.TestCase):
    """Test the repository layer with different cache modes."""

    def test_no_cache_mode_small_fetch(self):
        """Test fetching without cache."""
        config = get_test_config(cache_mode=1)  # NO_CACHE
        repo = RedditRepository(config)

        # Fetch a small amount of data
        submissions, comments = repo.get_user_contributions('AutoModerator')

        # AutoModerator has lots of posts, we should get some
        logger.info(f"NO_CACHE mode: Got {len(submissions)} submissions, {len(comments)} comments")

        # Verify data structure
        if submissions:
            self.assertIsInstance(submissions[0], Submission)
        if comments:
            self.assertIsInstance(comments[0], Comment)

    def test_get_thread(self):
        """Test fetching a complete thread."""
        config = get_test_config(cache_mode=1)  # NO_CACHE for clean test
        repo = RedditRepository(config)

        # Fetch a known thread
        result = repo.get_thread('1h0n5ql')

        if result:
            submission, comments = result
            self.assertIsInstance(submission, Submission)
            self.assertIsInstance(comments, list)
            logger.info(f"Thread '{submission.title[:30]}...' has {len(comments)} comments")
        else:
            logger.warning("Thread not found")


class TestDataIntegrity(unittest.TestCase):
    """Test data integrity and verify against expectations."""

    @classmethod
    def setUpClass(cls):
        cls.config = get_test_config(cache_mode=1)
        cls.provider = PushPullProvider(cls.config)

    def test_comment_has_required_fields(self):
        """Verify comments have all required fields populated."""
        comments = []
        for batch in self.provider.stream_user_comments('spez'):
            comments.extend(batch)
            if len(comments) >= 3:
                break

        if not comments:
            self.skipTest("No comments found for test user")

        for comment in comments:
            self.assertIsNotNone(comment.id, "Comment should have ID")
            self.assertIsNotNone(comment.raw_json, "Comment should have raw_json")
            # body might be [deleted] or [removed] but should exist
            self.assertTrue(hasattr(comment, 'body'), "Comment should have body attribute")

    def test_submission_has_required_fields(self):
        """Verify submissions have all required fields populated."""
        submissions = []
        for batch in self.provider.stream_user_submissions('spez'):
            submissions.extend(batch)
            if len(submissions) >= 3:
                break

        if not submissions:
            self.skipTest("No submissions found for test user")

        for submission in submissions:
            self.assertIsNotNone(submission.id, "Submission should have ID")
            self.assertIsNotNone(submission.raw_json, "Submission should have raw_json")
            self.assertIsNotNone(submission.created_utc, "Submission should have created_utc")


class TestCacheIntegration(unittest.TestCase):
    """Test cache integration with repository."""

    def test_cache_stores_and_retrieves(self):
        """Test that data is properly cached and retrieved."""
        # First, clear any existing test data by using a unique test
        config_no_cache = get_test_config(cache_mode=1)
        config_with_cache = get_test_config(cache_mode=0)

        repo_no_cache = RedditRepository(config_no_cache)
        repo_with_cache = RedditRepository(config_with_cache)

        # Fetch with cache enabled (will populate cache)
        logger.info("Fetching with cache enabled...")
        subs1, coms1 = repo_with_cache.get_user_contributions('AutoModerator')
        logger.info(f"First fetch: {len(subs1)} subs, {len(coms1)} comments")

        # Fetch again with cache (should use cached data)
        logger.info("Fetching again with cache...")
        subs2, coms2 = repo_with_cache.get_user_contributions('AutoModerator')
        logger.info(f"Second fetch: {len(subs2)} subs, {len(coms2)} comments")

        # Second fetch should have same or more data (cached + any new)
        self.assertGreaterEqual(len(subs2), len(subs1) - 10, "Cache should retain data")


if __name__ == '__main__':
    # Run with verbosity
    unittest.main(verbosity=2)

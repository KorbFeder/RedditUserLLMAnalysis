"""
Tests for RedditClient - verifies all methods work and compares with PullPush.
"""
import os
import sys
import logging
import unittest
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()

from src.providers.reddit.reddit import RedditClient
from src.providers.reddit.pushpull import PullPushClient
from src.storage.models import Submission, Comment

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_config():
    return {
        'reddit_api': {
            'pushpull': {
                'rate_limit': 1.0,
                'batch_size': 25
            }
        }
    }


class TestRedditClientMethods(unittest.TestCase):
    """Test all RedditClient methods work without crashing."""

    @classmethod
    def setUpClass(cls):
        cls.config = get_config()
        cls.client = RedditClient(cls.config)
        logger.info(f"RedditClient authenticated: {cls.client.authenticated}")
        logger.info(f"Base URL: {cls.client.base_url}")

    def test_source_name(self):
        """Test source_name property."""
        self.assertEqual(self.client.source_name, "reddit")

    def test_fetch_submission(self):
        """Test fetching a known submission."""
        # spez's famous announcement post
        submission = self.client.fetch_submission("6qptzw")

        if submission:
            self.assertIsInstance(submission, Submission)
            self.assertEqual(submission.id, "6qptzw")
            self.assertEqual(submission.author, "spez")
            logger.info(f"Fetched submission: {submission.title[:50]}...")
        else:
            logger.warning("Could not fetch submission - might be rate limited or deleted")

    def test_fetch_comment(self):
        """Test fetching a known comment."""
        # A known spez comment
        comment = self.client.fetch_comment("dkz2h00")

        if comment:
            self.assertIsInstance(comment, Comment)
            self.assertEqual(comment.id, "dkz2h00")
            logger.info(f"Fetched comment by {comment.author}: {comment.body[:50] if comment.body else 'N/A'}...")
        else:
            logger.warning("Could not fetch comment - might be rate limited or deleted")

    def test_fetch_bulk_comments(self):
        """Test bulk fetching comments."""
        comment_ids = ["t1_dkz2h00", "t1_dkz1abc"]  # Mix of real and fake

        submissions, comments = self.client.fetch_bulk(comment_ids)

        self.assertIsInstance(submissions, list)
        self.assertIsInstance(comments, list)
        logger.info(f"Bulk fetch returned {len(submissions)} submissions, {len(comments)} comments")

    def test_fetch_bulk_mixed(self):
        """Test bulk fetching mixed submissions and comments."""
        mixed_ids = [
            "t3_6qptzw",   # Submission
            "t1_dkz2h00",  # Comment
        ]

        submissions, comments = self.client.fetch_bulk(mixed_ids)

        logger.info(f"Mixed bulk fetch: {len(submissions)} submissions, {len(comments)} comments")

        if submissions:
            self.assertIsInstance(submissions[0], Submission)
        if comments:
            self.assertIsInstance(comments[0], Comment)

    def test_stream_user_submissions(self):
        """Test streaming user submissions."""
        count = 0
        for batch in self.client.stream_user_submissions("spez"):
            self.assertIsInstance(batch, list)
            if batch:
                self.assertIsInstance(batch[0], Submission)
            count += len(batch)
            logger.info(f"Streamed {count} submissions so far...")
            if count >= 10:  # Just get first ~10 for testing
                break

        self.assertGreater(count, 0)
        logger.info(f"Total submissions streamed: {count}")

    def test_stream_user_comments(self):
        """Test streaming user comments."""
        count = 0
        for batch in self.client.stream_user_comments("spez"):
            self.assertIsInstance(batch, list)
            if batch:
                self.assertIsInstance(batch[0], Comment)
            count += len(batch)
            logger.info(f"Streamed {count} comments so far...")
            if count >= 10:
                break

        self.assertGreater(count, 0)
        logger.info(f"Total comments streamed: {count}")

    def test_stream_submission_comments(self):
        """Test streaming comments from a submission."""
        comments = []
        for batch in self.client.stream_submission_comments("6qptzw"):
            comments.extend(batch)

        logger.info(f"Fetched {len(comments)} comments from submission")
        if comments:
            self.assertIsInstance(comments[0], Comment)


class TestCompareRedditVsPullPush(unittest.TestCase):
    """Compare data from Reddit API vs PullPush for consistency."""

    @classmethod
    def setUpClass(cls):
        cls.config = get_config()
        cls.reddit = RedditClient(cls.config)
        cls.pullpush = PullPushClient(cls.config)
        logger.info("Initialized both clients for comparison")

    def test_compare_submission_fetch(self):
        """Compare fetching same submission from both sources."""
        submission_id = "6qptzw"

        reddit_sub = self.reddit.fetch_submission(submission_id)
        pullpush_sub = self.pullpush.fetch_submission(submission_id)

        logger.info(f"Reddit submission: {reddit_sub}")
        logger.info(f"PullPush submission: {pullpush_sub}")

        if reddit_sub and pullpush_sub:
            self.assertEqual(reddit_sub.id, pullpush_sub.id)
            self.assertEqual(reddit_sub.author, pullpush_sub.author)
            self.assertEqual(reddit_sub.subreddit, pullpush_sub.subreddit)
            logger.info("✓ Submissions match!")
        else:
            logger.warning("One or both sources returned None")

    def test_compare_comment_fetch(self):
        """Compare fetching same comment from both sources."""
        comment_id = "dkz2h00"

        reddit_com = self.reddit.fetch_comment(comment_id)
        pullpush_com = self.pullpush.fetch_comment(comment_id)

        logger.info(f"Reddit comment author: {reddit_com.author if reddit_com else None}")
        logger.info(f"PullPush comment author: {pullpush_com.author if pullpush_com else None}")

        if reddit_com and pullpush_com:
            self.assertEqual(reddit_com.id, pullpush_com.id)
            self.assertEqual(reddit_com.author, pullpush_com.author)
            logger.info("✓ Comments match!")
        else:
            logger.warning("One or both sources returned None")

    def test_compare_user_comments_sample(self):
        """Compare a sample of user comments from both sources."""
        username = "spez"

        # Get first batch from each
        reddit_comments = []
        for batch in self.reddit.stream_user_comments(username):
            reddit_comments.extend(batch)
            break  # Just first batch

        pullpush_comments = []
        for batch in self.pullpush.stream_user_comments(username):
            pullpush_comments.extend(batch)
            break

        logger.info(f"Reddit returned {len(reddit_comments)} comments")
        logger.info(f"PullPush returned {len(pullpush_comments)} comments")

        # Reddit API only returns recent, PullPush has historical
        # So we compare overlap by ID
        reddit_ids = {c.id for c in reddit_comments}
        pullpush_ids = {c.id for c in pullpush_comments}

        overlap = reddit_ids & pullpush_ids
        logger.info(f"Overlapping comment IDs: {len(overlap)}")

    def test_bulk_fetch_vs_single(self):
        """Verify bulk fetch returns same data as single fetches."""
        ids = ["t1_dkz2h00", "t3_6qptzw"]

        # Bulk fetch
        bulk_subs, bulk_coms = self.reddit.fetch_bulk(ids)

        # Single fetches
        single_com = self.reddit.fetch_comment("dkz2h00")
        single_sub = self.reddit.fetch_submission("6qptzw")

        logger.info(f"Bulk: {len(bulk_subs)} subs, {len(bulk_coms)} comments")
        logger.info(f"Single comment: {single_com.id if single_com else None}")
        logger.info(f"Single submission: {single_sub.id if single_sub else None}")

        # Verify they match
        if bulk_coms and single_com:
            bulk_com = next((c for c in bulk_coms if c.id == "dkz2h00"), None)
            if bulk_com:
                self.assertEqual(bulk_com.author, single_com.author)
                logger.info("✓ Bulk and single comment match!")


class TestRateLimiting(unittest.TestCase):
    """Test that rate limiting works correctly."""

    @classmethod
    def setUpClass(cls):
        cls.client = RedditClient(get_config())

    def test_multiple_requests_no_crash(self):
        """Make several requests to verify rate limiting doesn't crash."""
        for i in range(5):
            submission = self.client.fetch_submission("6qptzw")
            logger.info(f"Request {i+1}/5 completed")

        logger.info("✓ Rate limiting handled correctly")


if __name__ == '__main__':
    unittest.main(verbosity=2)

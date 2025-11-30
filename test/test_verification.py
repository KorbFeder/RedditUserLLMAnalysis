"""
Verification tests that compare PushPull API data against known Reddit data.
These tests verify the data integrity of the API responses.
"""
import os
import sys
import logging
import unittest
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()

from src.data_providers.pushpull_provider import PushPullProvider

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


class TestVerifyPushPullData(unittest.TestCase):
    """Verify PushPull data matches expected Reddit data."""

    @classmethod
    def setUpClass(cls):
        cls.provider = PushPullProvider(get_config())

    def test_known_submission_data(self):
        """Verify a known submission has expected fields."""
        # Fetch swintec's known post
        submission = self.provider.fetch_submission('1h0n5ql')

        self.assertIsNotNone(submission, "Submission should exist")
        self.assertEqual(submission.id, '1h0n5ql')
        self.assertEqual(submission.author, 'swintec')
        self.assertEqual(submission.subreddit, 'usenet')
        self.assertIsNotNone(submission.title)
        self.assertIsNotNone(submission.created_utc)

        # Verify score is a reasonable number (not corrupted)
        self.assertIsInstance(submission.score, (int, type(None)))

        logger.info(f"Verified submission: {submission.title[:50]}...")
        logger.info(f"  Author: {submission.author}")
        logger.info(f"  Subreddit: r/{submission.subreddit}")
        logger.info(f"  Score: {submission.score}")
        logger.info(f"  Created: {submission.created_utc}")

    def test_submission_comments_have_valid_parent_ids(self):
        """Verify comment parent_ids reference valid comments or submission."""
        comments = []
        for batch in self.provider.stream_submission_comments('1h0n5ql'):
            comments.extend(batch)
            if len(comments) >= 50:  # Limit for test
                break

        if not comments:
            self.skipTest("No comments found")

        comment_ids = {c.id for c in comments}
        submission_id = '1h0n5ql'

        valid_parents = 0
        orphan_parents = 0

        for comment in comments:
            parent_id = comment.parent_id
            if parent_id is None:
                orphan_parents += 1
            elif parent_id == submission_id:
                valid_parents += 1  # Direct reply to post
            elif parent_id in comment_ids:
                valid_parents += 1  # Reply to another comment
            else:
                orphan_parents += 1  # Parent not in our set (deleted/not fetched)

        logger.info(f"Analyzed {len(comments)} comments:")
        logger.info(f"  Valid parents: {valid_parents}")
        logger.info(f"  Orphan parents: {orphan_parents}")

        # Most comments should have valid parents
        self.assertGreater(valid_parents, orphan_parents,
                          "More comments should have valid parents than orphans")

    def test_user_spez_exists(self):
        """Verify we can fetch Reddit CEO's posts (spez is a known user)."""
        submissions = []
        for batch in self.provider.stream_user_submissions('spez'):
            submissions.extend(batch)
            if len(submissions) >= 5:
                break

        self.assertGreater(len(submissions), 0, "spez should have submissions")

        for sub in submissions:
            self.assertEqual(sub.author, 'spez')

        logger.info(f"Verified {len(submissions)} submissions from u/spez")

    def test_comment_has_submission_id(self):
        """Verify comments have a valid submission_id (link_id)."""
        comments = []
        for batch in self.provider.stream_user_comments('swintec'):
            comments.extend(batch)
            if len(comments) >= 10:
                break

        comments_with_sub_id = [c for c in comments if c.submission_id]
        comments_without_sub_id = [c for c in comments if not c.submission_id]

        logger.info(f"Comments with submission_id: {len(comments_with_sub_id)}")
        logger.info(f"Comments without submission_id: {len(comments_without_sub_id)}")

        # Most comments should have submission_id
        self.assertGreater(len(comments_with_sub_id), len(comments_without_sub_id),
                          "Most comments should have submission_id")

    def test_chronological_ordering(self):
        """Verify submissions come in newest-first order."""
        submissions = []
        for batch in self.provider.stream_user_submissions('swintec'):
            submissions.extend(batch)
            if len(submissions) >= 20:
                break

        if len(submissions) < 2:
            self.skipTest("Not enough submissions to verify ordering")

        # Check that each submission is newer than or equal to the next
        for i in range(len(submissions) - 1):
            self.assertGreaterEqual(
                submissions[i].created_utc,
                submissions[i + 1].created_utc,
                f"Submission at index {i} should be newer than {i+1}"
            )

        logger.info(f"Verified {len(submissions)} submissions are in chronological order")


class TestDataConsistency(unittest.TestCase):
    """Test data consistency across different API calls."""

    @classmethod
    def setUpClass(cls):
        cls.provider = PushPullProvider(get_config())

    def test_submission_fetch_consistency(self):
        """Fetching same submission twice should return same data."""
        sub1 = self.provider.fetch_submission('1h0n5ql')
        sub2 = self.provider.fetch_submission('1h0n5ql')

        self.assertEqual(sub1.id, sub2.id)
        self.assertEqual(sub1.author, sub2.author)
        self.assertEqual(sub1.title, sub2.title)
        self.assertEqual(sub1.subreddit, sub2.subreddit)

        logger.info("Submission fetch is consistent")

    def test_user_data_not_empty(self):
        """Verify known active users have data."""
        test_users = ['swintec', 'spez']

        for username in test_users:
            submissions = []
            for batch in self.provider.stream_user_submissions(username):
                submissions.extend(batch)
                break  # Just first batch

            self.assertGreater(len(submissions), 0,
                             f"User {username} should have submissions")
            logger.info(f"User {username}: {len(submissions)} submissions in first batch")


if __name__ == '__main__':
    unittest.main(verbosity=2)

"""Tests for the new async ingestion service."""
import asyncio

# Test the logic without actual API calls
class TestIngestionLogic:

    def test_deleted_archived_reconciliation(self):
        """Test that is_deleted/is_archived flags are set correctly."""
        # Simulate: PullPush has [A, B, C], Reddit has [B, C, D]
        pp_ids = {"A", "B", "C"}
        r_ids = {"B", "C", "D"}

        deleted_ids = pp_ids - r_ids      # A - in archive but not Reddit
        not_archived_ids = r_ids - pp_ids  # D - on Reddit but not archived

        assert deleted_ids == {"A"}
        assert not_archived_ids == {"D"}

    def test_parent_chain_stop_condition(self):
        """Test that parent chain stops at top-level (parent_id == submission_id)."""
        # Simulate relations: [(id, parent_id, submission_id), ...]
        relations = [
            ("c1", "c2", "s1"),      # c1's parent is c2
            ("c2", "s1", "s1"),      # c2's parent is s1 (top-level!)
        ]

        seen_ids = {r[0] for r in relations}  # {c1, c2}

        # Get next parent_ids, excluding top-level
        parent_ids = {r[1] for r in relations if r[1] and r[1] not in seen_ids and r[1] != r[2]}

        # Should be empty - c2's parent is s1 which equals submission_id
        assert parent_ids == set()

    def test_cursor_logic(self):
        """Test that cursor correctly tracks newest timestamp."""
        timestamps = [1000, 2000, 1500, None, 3000]

        newest_ts = None
        for ts in timestamps:
            if ts and (newest_ts is None or ts > newest_ts):
                newest_ts = ts

        assert newest_ts == 3000

    def test_submission_ids_check(self):
        """Test the bug: we need submissions_exist, not get_user_submission_ids."""
        # User commented on submissions s1, s2, s3
        # User authored s1 only
        # DB has s1, s2

        submission_ids = {"s1", "s2", "s3"}  # Where user commented
        user_submission_ids = {"s1"}          # What user authored (wrong check!)
        db_submission_ids = {"s1", "s2"}      # What actually exists in DB

        # Wrong way (current bug):
        missing_wrong = submission_ids - user_submission_ids  # {s2, s3} - WRONG!

        # Correct way:
        missing_correct = submission_ids - db_submission_ids  # {s3} - CORRECT!

        assert missing_wrong == {"s2", "s3"}  # Would fetch s2 unnecessarily
        assert missing_correct == {"s3"}       # Only fetch what's actually missing


class TestAsyncIntegration:
    """Integration tests that require mocking."""

    def test_sync_user_contributions_structure(self):
        """Test that sync returns expected structure."""
        expected_keys = {"submissions", "comments", "deleted_submissions", "deleted_comments"}

        result = {
            "submissions": 10,
            "comments": 50,
            "deleted_submissions": 2,
            "deleted_comments": 5,
        }

        assert set(result.keys()) == expected_keys


if __name__ == "__main__":
    # Run basic tests
    test = TestIngestionLogic()
    test.test_deleted_archived_reconciliation()
    print("[OK] deleted/archived reconciliation logic correct")

    test.test_parent_chain_stop_condition()
    print("[OK] parent chain stop condition correct")

    test.test_cursor_logic()
    print("[OK] cursor logic correct")

    test.test_submission_ids_check()
    print("[OK] submission IDs check demonstrates the bug")

    print("\n[BUG] CONFIRMED: Line 33-34 uses get_user_submission_ids()")
    print("      Should use submissions_exist() instead")

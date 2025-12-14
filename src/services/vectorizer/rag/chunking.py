from src.storage.models import Comment, Submission


class DocumentBuilder:
    def comment(self: "DocumentBuilder", submission: Submission, user_comment: Comment, parent_comment: Comment | None) -> str:
        document = []
        document.append(f"[SUBREDDIT] r/{submission.subreddit}")
        document.append(f"[SUBMISSION_TITLE] {submission.author}: {submission.title}")
        if parent_comment:
            document.append(f"[PARENT_COMMENT] {parent_comment.author}: {parent_comment.body}")
        document.append(f"[USER_COMMENT] {user_comment.author}: {user_comment.body}")
        return "\n".join(document)

    def submission(self: "DocumentBuilder", submission: Submission) -> str:
        document = []
        document.append(f"[SUBREDDIT] r/{submission.subreddit}")
        document.append(f"[USER_SUBMISSION_TITLE] {submission.author}: {submission.title}")
        document.append(f"[BODY] {submission.selftext}")
        return "\n".join(document)
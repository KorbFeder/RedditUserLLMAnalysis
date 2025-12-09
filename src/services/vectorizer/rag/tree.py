import logging
from dataclasses import dataclass, field

from src.storage.models import Comment

logger = logging.getLogger(__name__)

@dataclass
class CommentNode:
    comment: Comment
    replies: list["CommentNode"] = field(default_factory=list)

def order_comments(submission_id: str, comments: list[Comment]) -> list[CommentNode]:
    nodes = {c.id: CommentNode(comment=c) for c in comments}
    root = []

    for comment in comments:
        parent_id = comment.parent_id

        # in case the comment does not have a parent id (faulty or deleted comment)
        if not parent_id or isinstance(parent_id, int):
            logger.warning(f"Adding comment {comment.id} to root, cause parent_id field seems corrupted")
            root.append(nodes[comment.id])
            continue

        if parent_id == submission_id:
            root.append(nodes[comment.id])
        elif parent_id in nodes:
            nodes[parent_id].replies.append(nodes[comment.id])
        else:
            logger.warning(f"The comment {comment.id} parent {parent_id} could not be found, adding it to root")
            root.append(nodes[comment.id])

    return root 
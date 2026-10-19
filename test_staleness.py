from datetime import datetime, timezone
from bson import ObjectId
from app.main import serialize


def make_doc(summary_hash, tags_hash, stage_hash):
    now = datetime.now(timezone.utc)
    return {
        "_id": ObjectId(), "user_id": "u1", "title": "T", "content": "body",
        "content_hash": "current", "version": 2, "status": "completed",
        "stages": {
            "processing": {"status": "completed", "content_hash": stage_hash},
            "enriching": {"status": "completed", "content_hash": stage_hash},
        },
        "summary": "summary", "tags": ["tag"], "summary_hash": summary_hash,
        "tags_hash": tags_hash, "created_at": now, "updated_at": now,
    }


def test_result_current_only_when_every_derived_hash_matches():
    assert serialize(make_doc("current", "current", "current"))["result_is_current"] is True
    assert serialize(make_doc("old", "current", "current"))["result_is_current"] is False
    assert serialize(make_doc("current", "old", "current"))["result_is_current"] is False
    assert serialize(make_doc("current", "current", "old"))["result_is_current"] is False

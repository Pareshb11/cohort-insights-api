import asyncio, hashlib, random, re
from datetime import datetime, timezone
from bson import ObjectId
from app.database import db, redis
from app.config import settings

ACTIVE = {"queued", "processing", "enriching"}

def digest(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()

def cache_key(content_hash: str) -> str:
    return f"content-result:{content_hash}"

def active_key(user_id: str) -> str:
    return f"active-documents:{user_id}"

async def reserve_slot(user_id: str) -> bool:
    # Atomic per-user admission. Redis outage fails closed rather than bypassing the limit.
    script = """
    local n = tonumber(redis.call('GET', KEYS[1]) or '0')
    if n >= tonumber(ARGV[1]) then return 0 end
    redis.call('INCR', KEYS[1])
    redis.call('EXPIRE', KEYS[1], 3600)
    return 1
    """
    try:
        return bool(await redis().eval(script, 1, active_key(user_id), settings.active_limit))
    except Exception as exc:
        raise RuntimeError("Rate limiter unavailable") from exc

async def release_slot(user_id: str):
    script = """
    local n = tonumber(redis.call('GET', KEYS[1]) or '0')
    if n <= 1 then redis.call('DEL', KEYS[1]) else redis.call('DECR', KEYS[1]) end
    return 1
    """
    try:
        await redis().eval(script, 1, active_key(user_id))
    except Exception:
        # Reconciliation can repair this derived counter; document state remains authoritative.
        pass

def make_summary(title: str, content: str) -> str:
    words = re.findall(r"\b[\w'-]+\b", content)
    excerpt = " ".join(words[:35])
    return f"{title}: {excerpt}"[:500]

def make_tags(summary: str) -> list[str]:
    words = [w.lower() for w in re.findall(r"[A-Za-z][A-Za-z0-9'-]{2,}", summary)]
    stop = {"the", "and", "for", "with", "this", "that", "from", "into", "your"}
    result = []
    for word in words:
        if word not in stop and word not in result:
            result.append(word)
        if len(result) == 5:
            break
    return result or ["general"]

async def process_document(doc_id: str, version: int):
    collection = db().documents
    doc = await collection.find_one({"_id": ObjectId(doc_id), "version": version})
    if not doc or doc["status"] not in ACTIVE:
        return
    user_id, content_hash = doc["user_id"], doc["content_hash"]
    try:
        # Stage 1 is skipped only if the cache contains a complete result for this content hash.
        cached = await redis().get(cache_key(content_hash))
        if cached:
            import json
            result = json.loads(cached)
            await collection.update_one(
                {"_id": doc["_id"], "version": version},
                {"$set": {"summary": result["summary"], "tags": result["tags"],
                          "summary_hash": content_hash, "tags_hash": content_hash,
                          "stages.processing.status": "completed",
                          "stages.processing.content_hash": content_hash,
                          "stages.enriching.status": "completed",
                          "stages.enriching.content_hash": content_hash,
                          "status": "completed", "updated_at": datetime.now(timezone.utc)}}
            )
            await release_slot(user_id)
            return

        # Claim stage 1 with a version-qualified conditional update.
        claimed = await collection.update_one(
            {"_id": doc["_id"], "version": version, "stages.processing.status": "pending"},
            {"$set": {"status": "processing", "stages.processing.status": "running",
                      "updated_at": datetime.now(timezone.utc)}}
        )
        if not claimed.modified_count:
            return
        await asyncio.sleep(random.uniform(10, 20))
        if random.random() < 0.10:
            raise StageFailure("processing", "Simulated processing failure")
        summary = make_summary(doc["title"], doc["content"])
        updated = await collection.update_one(
            {"_id": doc["_id"], "version": version, "stages.processing.status": "running"},
            {"$set": {"summary": summary, "summary_hash": content_hash,
                      "stages.processing.status": "completed",
                      "stages.processing.content_hash": content_hash,
                      "status": "enriching", "stages.enriching.status": "running",
                      "updated_at": datetime.now(timezone.utc)}}
        )
        if not updated.modified_count:
            return  # PATCH won the race; stale worker must not publish.

        await asyncio.sleep(random.uniform(5, 15))
        if random.random() < 0.10:
            raise StageFailure("enriching", "Simulated enriching failure")
        tags = make_tags(summary)
        updated = await collection.update_one(
            {"_id": doc["_id"], "version": version, "stages.enriching.status": "running"},
            {"$set": {"tags": tags, "tags_hash": content_hash,
                      "stages.enriching.status": "completed", "stages.enriching.content_hash": content_hash,
                      "status": "completed", "updated_at": datetime.now(timezone.utc)}}
        )
        if updated.modified_count:
            import json
            await redis().set(cache_key(content_hash), json.dumps({"summary": summary, "tags": tags}),
                              ex=settings.cache_ttl_seconds)
            await release_slot(user_id)
    except StageFailure as exc:
        stage = exc.stage
        await collection.update_one(
            {"_id": ObjectId(doc_id), "version": version, f"stages.{stage}.status": {"$in": ["running", "pending"]}},
            {"$set": {"status": "failed", f"stages.{stage}.status": "failed",
                      f"stages.{stage}.error": str(exc), "updated_at": datetime.now(timezone.utc)}}
        )
        await release_slot(user_id)
    except Exception as exc:
        # Surface unexpected errors on the current stage, never silently swallow them.
        current = await collection.find_one({"_id": ObjectId(doc_id), "version": version})
        if current:
            stage = "enriching" if current.get("stages", {}).get("processing", {}).get("status") == "completed" else "processing"
            await collection.update_one(
                {"_id": ObjectId(doc_id), "version": version},
                {"$set": {"status": "failed", f"stages.{stage}.status": "failed",
                          f"stages.{stage}.error": str(exc)[:300], "updated_at": datetime.now(timezone.utc)}}
            )
            await release_slot(user_id)

class StageFailure(Exception):
    def __init__(self, stage: str, message: str):
        self.stage = stage
        super().__init__(message)

async def worker_loop(stop_event: asyncio.Event):
    while not stop_event.is_set():
        try:
            # Claim queued documents atomically. The version snapshot prevents stale workers
            # from writing results after a PATCH increments version.
            doc = await db().documents.find_one_and_update(
                {"status": "queued"},
                {"$set": {"worker_claimed_at": datetime.now(timezone.utc)}},
                sort=[("created_at", 1)],
                return_document=True,
            )
            if doc:
                await process_document(str(doc["_id"]), doc["version"])
                continue
        except Exception:
            # Keep the worker alive; health endpoint exposes dependency failures.
            pass
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=settings.worker_poll_seconds)
        except asyncio.TimeoutError:
            pass

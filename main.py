import asyncio, hashlib, logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from bson import ObjectId
from bson.errors import InvalidId
from fastapi import FastAPI, HTTPException, Query, Request, status
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError
from app.database import connect, close, db, redis
from app.models import DocumentSubmit, DocumentPatch
from app.pipeline import digest, reserve_slot, release_slot, worker_loop, cache_key, ACTIVE

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cohort-insights")

@asynccontextmanager
async def lifespan(app: FastAPI):
    await connect()
    stop = asyncio.Event()
    task = asyncio.create_task(worker_loop(stop))
    app.state.worker_stop = stop
    app.state.worker_task = task
    yield
    stop.set()
    await task
    await close()

app = FastAPI(title="Cohort Insights API", version="1.0.0", lifespan=lifespan)

def serialize(doc: dict) -> dict:
    stages = doc.get("stages", {})
    current_hash = doc["content_hash"]
    current = (doc.get("summary_hash") == current_hash and doc.get("tags_hash") == current_hash
               and stages.get("processing", {}).get("content_hash") == current_hash
               and stages.get("enriching", {}).get("content_hash") == current_hash
               and stages.get("processing", {}).get("status") == "completed"
               and stages.get("enriching", {}).get("status") == "completed")
    return {
        "id": str(doc["_id"]), "user_id": doc["user_id"], "title": doc["title"],
        "content": doc["content"], "status": doc["status"], "version": doc["version"],
        "content_hash": current_hash, "stages": stages, "summary": doc.get("summary"),
        "tags": doc.get("tags"), "summary_hash": doc.get("summary_hash"),
        "tags_hash": doc.get("tags_hash"), "client_doc_ref": doc.get("client_doc_ref"),
        "created_at": doc["created_at"], "updated_at": doc["updated_at"],
        "result_is_current": current
    }

async def owned_document(document_id: str, user_id: str):
    try:
        oid = ObjectId(document_id)
    except (InvalidId, TypeError):
        raise HTTPException(404, "Document not found")
    doc = await db().documents.find_one({"_id": oid, "user_id": user_id})
    if not doc:
        raise HTTPException(404, "Document not found")
    return doc

@app.get("/health")
async def health():
    try:
        await db().command("ping")
        await redis().ping()
        return {"status": "ok", "mongodb": "ok", "redis": "ok"}
    except Exception as exc:
        raise HTTPException(503, detail=f"Dependency health check failed: {type(exc).__name__}")

@app.post("/documents", status_code=status.HTTP_201_CREATED)
async def submit(payload: DocumentSubmit):
    content_hash = digest(payload.content)
    collection = db().documents
    now = datetime.now(timezone.utc)

    # Crosswalk policy: client_doc_ref is a stable upsert key. A repeat ref with
    # identical content returns the existing document; different content replaces it
    # in-place (same internal ID), increments version, and invalidates derived fields.
    if payload.client_doc_ref:
        existing = await collection.find_one({"client_doc_ref": payload.client_doc_ref})
        if existing:
            if existing["user_id"] != payload.user_id:
                raise HTTPException(409, "client_doc_ref already belongs to another user")
            if existing["content_hash"] == content_hash:
                return serialize(existing)
            return await replace_content(existing, payload.content, content_hash, payload.user_id, expected_version=None)

    # Content cache only serves complete results. Check before consuming an active slot.
    try:
        import json
        cached = await redis().get(cache_key(content_hash))
    except Exception:
        cached = None
    if cached:
        import json
        result = json.loads(cached)
        doc = {"user_id": payload.user_id, "title": payload.title, "content": payload.content,
               "content_hash": content_hash, "version": 1, "status": "completed",
               "stages": {"processing": {"status": "completed", "attempts": 0, "content_hash": content_hash},
                          "enriching": {"status": "completed", "attempts": 0, "content_hash": content_hash}},
               "summary": result["summary"], "tags": result["tags"], "summary_hash": content_hash,
               "tags_hash": content_hash, "client_doc_ref": payload.client_doc_ref,
               "created_at": now, "updated_at": now}
        try:
            inserted = await collection.insert_one(doc)
        except DuplicateKeyError:
            raise HTTPException(409, "client_doc_ref already exists")
        doc["_id"] = inserted.inserted_id
        return serialize(doc)

    try:
        allowed = await reserve_slot(payload.user_id)
    except RuntimeError:
        raise HTTPException(503, "Rate limiter unavailable")
    if not allowed:
        raise HTTPException(429, "At most 3 documents may be active per user")
    doc = {"user_id": payload.user_id, "title": payload.title, "content": payload.content,
           "content_hash": content_hash, "version": 1, "status": "queued",
           "stages": {"processing": {"status": "pending", "attempts": 0},
                      "enriching": {"status": "pending", "attempts": 0}},
           "summary": None, "tags": None, "summary_hash": None, "tags_hash": None,
           "client_doc_ref": payload.client_doc_ref, "created_at": now, "updated_at": now}
    try:
        inserted = await collection.insert_one(doc)
    except DuplicateKeyError:
        await release_slot(payload.user_id)
        raise HTTPException(409, "client_doc_ref already exists")
    doc["_id"] = inserted.inserted_id
    return serialize(doc)

async def replace_content(existing: dict, content: str, content_hash: str, user_id: str, expected_version: int | None):
    collection = db().documents
    old_active = existing["status"] in ACTIVE
    new_active = not bool(await redis().get(cache_key(content_hash)))
    if new_active and not old_active:
        try:
            allowed = await reserve_slot(user_id)
        except RuntimeError:
            raise HTTPException(503, "Rate limiter unavailable")
        if not allowed:
            raise HTTPException(429, "At most 3 documents may be active per user")
    query = {"_id": existing["_id"], "user_id": user_id, "version": existing["version"]}
    if expected_version is not None:
        query["version"] = expected_version
    now = datetime.now(timezone.utc)
    cached = await redis().get(cache_key(content_hash))
    update = {"content": content, "content_hash": content_hash, "version": existing["version"] + 1,
              "status": "queued" if not cached else "completed",
              "summary": None, "tags": None, "summary_hash": None, "tags_hash": None,
              "stages": {"processing": {"status": "pending", "attempts": 0},
                         "enriching": {"status": "pending", "attempts": 0}},
              "updated_at": now}
    if cached:
        import json
        result = json.loads(cached)
        update.update({"summary": result["summary"], "tags": result["tags"],
                       "summary_hash": content_hash, "tags_hash": content_hash,
                       "stages": {"processing": {"status": "completed", "attempts": 0, "content_hash": content_hash},
                                  "enriching": {"status": "completed", "attempts": 0, "content_hash": content_hash}}})
    result = await collection.find_one_and_update(query, {"$set": update}, return_document=ReturnDocument.AFTER)
    if not result:
        if new_active and not old_active:
            await release_slot(user_id)
        raise HTTPException(409, "Document was modified concurrently; fetch it and retry")
    if old_active and not new_active:
        await release_slot(user_id)
    return serialize(result)

@app.patch("/documents/{document_id}")
async def patch_document(document_id: str, payload: DocumentPatch, user_id: str = Query(...)):
    doc = await owned_document(document_id, user_id)
    if payload.expected_version is not None and payload.expected_version != doc["version"]:
        raise HTTPException(409, "Version conflict")
    return await replace_content(doc, payload.content, digest(payload.content), user_id, payload.expected_version)

@app.get("/documents/{document_id}")
async def get_document(document_id: str, user_id: str = Query(...)):
    return serialize(await owned_document(document_id, user_id))

@app.get("/users/{user_id}/documents")
async def list_documents(user_id: str, requester_id: str = Query(...),
                         page: int = Query(1, ge=1), page_size: int = Query(20, ge=1, le=100),
                         status_filter: str | None = Query(None, alias="status")):
    if requester_id != user_id:
        raise HTTPException(404, "User not found")
    query = {"user_id": user_id}
    if status_filter:
        allowed = {"queued", "processing", "enriching", "completed", "failed"}
        if status_filter not in allowed:
            raise HTTPException(422, "Invalid status filter")
        query["status"] = status_filter
    cursor = db().documents.find(query).sort("created_at", -1).skip((page - 1) * page_size).limit(page_size)
    items = [serialize(doc) async for doc in cursor]
    return {"page": page, "page_size": page_size, "items": items}

@app.get("/documents/by-ref/{client_doc_ref}")
async def by_ref(client_doc_ref: str, user_id: str = Query(...)):
    doc = await db().documents.find_one({"client_doc_ref": client_doc_ref, "user_id": user_id})
    if not doc:
        raise HTTPException(404, "Document not found")
    return serialize(doc)

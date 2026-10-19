# Cohort Insights API

Backend take-home implementation using Python 3.11, FastAPI, MongoDB, Redis, and Docker Compose. The AI work is intentionally simulated; no external model is called.

## Run locally

```bash
cp .env.example .env
docker compose up --build
```

API docs: http://localhost:8000/docs  
Health: http://localhost:8000/health

Run unit tests locally with `pip install -r requirements.txt && pytest`.

## Endpoints

- `POST /documents` — submit a document. JSON: `user_id`, `title`, `content`, optional `client_doc_ref`.
- `PATCH /documents/{document_id}?user_id=...` — replace content in place; optionally send `expected_version` in JSON to enforce optimistic concurrency.
- `GET /documents/{document_id}?user_id=...` — status and current derived fields.
- `GET /users/{user_id}/documents?requester_id=...&page=1&page_size=20&status=completed` — newest-first pagination. `requester_id` must equal path `user_id`.
- `GET /documents/by-ref/{client_doc_ref}?user_id=...` — indexed crosswalk lookup.
- `GET /health` — checks MongoDB and Redis.

## Assumptions and decisions

1. **Identity:** authentication is outside this take-home's scope. Endpoints accept `user_id`/`requester_id` as identity claims for demonstrating ownership checks; production must derive identity from a verified JWT/session, never trust these query/body fields.
2. **Crosswalk repeat behavior:** `client_doc_ref` is globally unique. Same ref + same owner + same content hash returns the existing document. Same ref + same owner + different content replaces the existing document in place, increments its version, and clears derived output before reprocessing. A ref owned by another user returns 409 to avoid silently transferring ownership.
3. **Staleness:** content updates increment a monotonically increasing `version` and replace the content hash. Both stage writes filter on the captured version and expected stage state. A worker whose version has lost the race cannot publish. Summary and tags each store their producing `content_hash`. Readers consider output current only when both hashes and both stage hashes equal the document's current `content_hash` and both stages are completed. PATCH clears summary/tags and their hashes atomically in the same MongoDB update that changes content/version. This makes a mixed-version result unobservable through the API serializer.
4. **Failure behavior:** processing/enriching have independent simulated 10% failure probabilities. The failed stage is explicitly marked failed. Enriching failures preserve the completed summary, and do not rerun processing automatically. A new PATCH starts both stages from scratch. (A production retry endpoint/backoff policy would be added.)
5. **Rate limit:** Redis uses an atomic Lua admission script for a maximum of three active jobs per user. The counter is a derived operational counter with a TTL; Redis outages fail closed for submissions. Production would reconcile counters from MongoDB after crashes and use a durable queue/outbox to close the database/Redis dual-write window.
6. **Caching:** complete results only are cached under `content-result:{sha256(content)}`, independent of document ID. Cache entries include both summary and tags, so partial results are never treated as complete. Redis cache errors degrade to processing rather than blocking a submission.
7. **Indexes:** `user_id + created_at desc` supports listing; `user_id + status + created_at desc` supports filtered listing; sparse unique `client_doc_ref` supports crosswalk resolution without a collection scan; `content_hash + user_id + status` supports cache/deduplication queries; `status + updated_at` supports worker queue scans.
8. **Concurrency:** `expected_version` is optional. When provided and stale, PATCH returns 409. Even without it, the Mongo update uses the version observed during read as a compare-and-swap guard; a concurrent update returns 409.
9. **Pagination:** page/skip/limit is implemented to meet the assignment API. At large offsets use cursor pagination over `(created_at, _id)` instead.
10. **Queue implementation:** this compact submission uses an atomic MongoDB claim loop as a runnable background worker rather than introducing a separate broker. For multiple API replicas, run a dedicated worker deployment and use a durable queue (e.g. Redis Streams/Celery) with leases and idempotent stage claims.

## Schema & Staleness Design

Each document has a monotonically increasing `version` and a SHA-256 `content_hash`. The `summary_hash` and `tags_hash` record the exact content hash that generated each derived field; each stage also records its output hash and state. On PATCH, one atomic MongoDB update increments `version`, changes `content` and `content_hash`, clears summary/tags and their producing hashes, resets both stages, and moves the document to queued (unless a complete result is available for the new content hash).

Every worker captures the version and content hash at the start. Its writes include `_id`, captured `version`, and the expected stage status in the MongoDB filter. Therefore, if PATCH wins while a stage is running, the worker's conditional update matches zero documents and cannot publish output for the old version. The enriching stage reads the summary that was committed for its captured version and also writes only when the same version remains current. MongoDB's single-document update atomicity prevents readers from seeing a partially applied PATCH.

The response's `result_is_current` is true only when summary hash, tags hash, both stage hashes, and the document content hash all match, and both stages are completed. The API never labels old derived fields current. During reprocessing the old summary/tags are cleared atomically, so callers see no mixed old/new result. A content hash detects equality; the version counter additionally protects against the ABA/racing-worker problem where content changes while a worker is in flight.

## At 100×

With 500K documents for one user, the `user_id` prefix makes that user's records contiguous in the logical index range. Index size and working-set pressure increase, and listing or status-filtered listing for that user may traverse a large range. Compound indexes reduce document fetches for the supported sort/filter pattern, but cannot make a single user's hot range disappear. I would measure index size, cache hit ratio, query plans, and p95/p99 latency, then use cursor pagination and possibly archive cold completed documents to a separate collection or time-partitioned storage. I would avoid creating many redundant indexes because each one increases write amplification and RAM requirements.

For horizontal scale, I would not shard only by `user_id`: a very large tenant can become a hot shard and concentrate writes and reads on one shard. A candidate shard key is a hashed `document_id`/ObjectId-derived key for even distribution, with a supporting `{user_id, created_at, _id}` index for listing. This trades single-shard user queries for balanced writes; if tenant-local query efficiency dominates, a compound tenant-plus-bucket key (for example `user_id` plus a stable hash bucket) can distribute a large tenant while keeping a bounded number of buckets per user. Crosswalk uniqueness across shards needs special care because MongoDB unique indexes generally must include the shard key; I would either shard the crosswalk separately by hashed `client_doc_ref`, or maintain a dedicated mapping collection whose shard/index design preserves global uniqueness.

At 100× submit QPS, a single Redis counter per user can become a hot key, and the current MongoDB/Redis dual-write is not transactional. I would keep admission atomic with a Lua script but add idempotent job IDs, leases, periodic reconciliation from durable job state, and metrics for counter drift. If a few users dominate traffic, use a partitioned queue and carefully designed per-user admission records rather than blindly splitting the counter (splitting counters can violate the hard limit). Redis Cluster capacity and latency should be load-tested; on Redis failure, submissions should fail closed or be admitted through a durable fallback with explicit limits.

Skip/limit pagination degrades at deep offsets because MongoDB must walk past skipped index entries, and inserts can cause page drift. I would use keyset pagination on the descending `(created_at, _id)` pair: the client sends the last seen tuple as an opaque cursor, and the next query filters below that tuple while using the matching compound index. This is stable under new inserts, bounded in work per page, and avoids deep offset scans. I would also set page-size caps and consider snapshot semantics if the product requires a consistent multi-page export.

## What I would improve with more time

- Add pytest + `httpx.AsyncClient` integration tests using disposable MongoDB/Redis services for ownership, 429 admission, crosswalk duplicate/replacement, cache hits, and PATCH-vs-worker races.
- Move processing to a dedicated worker service with durable queue/outbox semantics and lease recovery.
- Add bounded retry with exponential backoff for failed stages, attempt counters, and a dead-letter state.
- Use verified JWT claims for identity and structured request/trace IDs for logs.
- Reconcile Redis active counters after worker crashes; expose Prometheus metrics.
- Add cursor pagination and a transactional/idempotent crosswalk update flow.

## API example

```bash
curl -X POST http://localhost:8000/documents \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"u1","title":"Example","content":"Python APIs process cohort data."}'
```

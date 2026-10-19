from motor.motor_asyncio import AsyncIOMotorClient
from redis.asyncio import Redis
from app.config import settings

mongo_client: AsyncIOMotorClient | None = None
redis_client: Redis | None = None

async def connect():
    global mongo_client, redis_client
    mongo_client = AsyncIOMotorClient(settings.mongo_uri, serverSelectionTimeoutMS=3000)
    redis_client = Redis.from_url(settings.redis_url, decode_responses=True)
    db = mongo_client[settings.mongo_db]
    await db.documents.create_index([("user_id", 1), ("created_at", -1)])
    await db.documents.create_index([("user_id", 1), ("status", 1), ("created_at", -1)])
    await db.documents.create_index([("client_doc_ref", 1)], unique=True, sparse=True)
    await db.documents.create_index([("content_hash", 1), ("user_id", 1), ("status", 1)])
    await db.documents.create_index([("status", 1), ("updated_at", 1)])
    await mongo_client.admin.command("ping")
    await redis_client.ping()

async def close():
    if mongo_client:
        mongo_client.close()
    if redis_client:
        await redis_client.aclose()

def db():
    if not mongo_client:
        raise RuntimeError("MongoDB is not connected")
    from app.config import settings
    return mongo_client[settings.mongo_db]

def redis():
    if not redis_client:
        raise RuntimeError("Redis is not connected")
    return redis_client

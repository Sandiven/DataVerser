# app/database.py
from motor.motor_asyncio import AsyncIOMotorClient
from .config import settings
import asyncio

client = AsyncIOMotorClient(settings.MONGODB_URI)
db = client[settings.DATABASE_NAME]

async def create_indexes():
    # indexes for fast sorts / lookups
    await db.uploads.create_index("content_hash", unique=False)
    await db.schemas.create_index([("source_id", 1), ("version", -1)])
    await db.records.create_index([("source_id", 1)])
    await db.logs.create_index("uploaded_at")
    # in app/database.py (inside create_indexes)
    await db.queries.create_index("query_id", unique=True)
    await db.query_results.create_index("query_id", unique=True)
    await db.records.create_index([("source_id", 1), ("uploaded_at", -1)])

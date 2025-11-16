# app/services/storage_service.py
import os
import hashlib
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorGridFSBucket
from app.database import client, db
from typing import Optional

# create a GridFS bucket instance (uses the same client)
_gridfs_bucket: Optional[AsyncIOMotorGridFSBucket] = None

def get_gridfs_bucket() -> AsyncIOMotorGridFSBucket:
    global _gridfs_bucket
    if _gridfs_bucket is None:
        _gridfs_bucket = AsyncIOMotorGridFSBucket(db)
    return _gridfs_bucket

def hash_bytes(content: bytes) -> str:
    h = hashlib.sha256()
    h.update(content)
    return h.hexdigest()

async def save_raw_file_bytes(filename: str, content: bytes, metadata: dict = None) -> dict:
    """
    Save raw bytes into GridFS. If a file with the same content hash already
    exists, returns the existing record (idempotent).
    Returns metadata dict containing:
      - file_id (GridFS id)
      - content_hash
      - already_exists (bool)
      - uploaded_at
    """
    if metadata is None:
        metadata = {}

    content_hash = hash_bytes(content)

    # check for existing upload by hash in uploads collection
    existing = await db.uploads.find_one({"content_hash": content_hash})
    if existing:
        return {
            "file_id": existing.get("file_id"),
            "content_hash": content_hash,
            "already_exists": True,
            "upload_id": str(existing.get("_id")),
            "uploaded_at": existing.get("uploaded_at")
        }

    # store in gridfs
    bucket = get_gridfs_bucket()
    file_id = await bucket.upload_from_stream(
        filename,
        content,
        metadata={**metadata, "content_hash": content_hash, "saved_at": datetime.utcnow()}
    )

    upload_doc = {
        "filename": filename,
        "file_id": file_id,
        "content_hash": content_hash,
        "metadata": metadata,
        "uploaded_at": datetime.utcnow()
    }
    res = await db.uploads.insert_one(upload_doc)
    return {
        "file_id": file_id,
        "content_hash": content_hash,
        "already_exists": False,
        "upload_id": str(res.inserted_id),
        "uploaded_at": upload_doc["uploaded_at"]
    }

async def get_raw_file_by_hash(content_hash: str):
    """Return upload record by hash."""
    return await db.uploads.find_one({"content_hash": content_hash})

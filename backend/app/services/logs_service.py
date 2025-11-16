# app/services/logs_service.py

from datetime import datetime
from app.database import db
from typing import Optional, Dict, Any


def _format_log(log: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalizes log object into a consistent frontend-friendly format.
    """

    return {
        "id": str(log.get("_id")),
        "timestamp": log.get("timestamp", datetime.utcnow()).isoformat(),

        # filenames may be missing in older logs
        "filename": log.get("filename") or log.get("source_id") or "unknown",

        # standardize schema version
        "schemaVersion": log.get("schema_version") or log.get("schemaVersion") or "v1",

        # safe default
        "record_count": log.get("record_count", 0),

        # infer status
        "status": "Success" if "failed" not in str(log.get("message", "")).lower() else "Failed",

        # safe structured preview
        "structured_preview": log.get("structured_preview", []),

        # safe cleaning stats
        "cleaning_stats": log.get("cleaning_stats", {}),

        # preserve original message
        "message": log.get("message", "")
    }


async def save_log(source_id: str, message: str, extra: Optional[Dict[str, Any]] = None):
    """
    Save a log entry.
    """

    if not source_id:
        source_id = "unknown"

    log_entry = {
        "source_id": source_id,
        "message": message,
        "timestamp": datetime.utcnow(),
    }

    if extra and isinstance(extra, dict):
        for k, v in extra.items():
            log_entry[k] = v

    await db.logs.insert_one(log_entry)
    return True


async def get_logs_for_source(source_id: str, limit: int = 50):
    cursor = (
        db.logs
        .find({"source_id": source_id})
        .sort("timestamp", -1)
        .limit(limit)
    )

    out = []
    async for log in cursor:
        out.append(_format_log(log))
    return out


async def get_all_logs(limit: int = 200):
    cursor = (
        db.logs
        .find()
        .sort("timestamp", -1)
        .limit(limit)
    )

    out = []
    async for log in cursor:
        out.append(_format_log(log))
    return out

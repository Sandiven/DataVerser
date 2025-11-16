from fastapi import APIRouter
from ..database import db
import re
from app.services import logs_service


router = APIRouter(prefix="/logs", tags=["Logs"])


def determine_status(message: str):
    """Infer status for UI based on message"""
    if "Uploaded" in message or "Success" in message:
        return "Success"
    if "Failed" in message:
        return "Failed"
    return "Completed_With_Errors"


def extract_schema_version(message: str):
    """Extract schema version from message, fallback to v1.0"""
    match = re.search(r"schema_v=([\d.]+)", message)
    if match:
        return f"v{match.group(1)}"
    return "v1.0"


@router.get("/")
async def get_all_logs():
    logs = await logs_service.get_all_logs()

    # Normalize log entries for frontend
    normalized = []
    for log in logs:
        normalized.append({
            "id": log.get("_id"),
            "timestamp": log.get("timestamp"),
            "message": log.get("message"),
            "source_id": log.get("source_id"),

            # 🔥 NEW FIELDS NEEDED FOR UI
            "filename": log.get("filename"),
            "record_count": log.get("record_count", 0),
            "schema_version": log.get("schema_version"),
            "structured_preview": log.get("structured_preview", []),
            "cleaning_stats": log.get("cleaning_stats", {})
        })

    return normalized



@router.get("/{filename}")
async def get_logs_for_filename(filename: str):
    logs = await logs_service.get_logs_for_source(filename)
    
    normalized = []
    for log in logs:
        normalized.append({
            "id": log.get("_id"),
            "timestamp": log.get("timestamp"),
            "message": log.get("message"),
            "source_id": log.get("source_id"),
            "filename": log.get("filename"),
            "record_count": log.get("record_count", 0),
            "schema_version": log.get("schema_version"),
            "structured_preview": log.get("structured_preview", []),
            "cleaning_stats": log.get("cleaning_stats", {})
        })

    return normalized

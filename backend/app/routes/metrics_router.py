from fastapi import APIRouter
from ..database import db

router = APIRouter(prefix="/metrics", tags=["Metrics"])


@router.get("/")
async def get_metrics():
    """
    Returns dashboard metrics exactly as frontend expects:
    {
        totalRecords,
        totalRuns,
        successRate,
        activeVersion
    }
    """
    try:
        # Total processed rows
        total_records = await db.records.count_documents({})

        # Total runs (logs)
        total_runs = await db.logs.count_documents({})

        # Successful runs
        success_logs = await db.logs.count_documents({
            "$or": [
                {"message": {"$regex": "Uploaded", "$options": "i"}},
                {"message": {"$regex": "Success", "$options": "i"}},
                {"message": {"$regex": "completed", "$options": "i"}},
            ]
        })

        success_rate = (success_logs / total_runs * 100) if total_runs else 0

        # Latest schema version
        latest_schema = await db.schemas.find_one(
            {},
            sort=[("schema_version", -1)]
        )

        active_version = (
            f"v{latest_schema.get('schema_version')}"
            if latest_schema else "v1.0"
        )

        return {
            "totalRecords": total_records,
            "totalRuns": total_runs,
            "successRate": round(success_rate, 1),
            "activeVersion": active_version
        }

    except:
        return {
            "totalRecords": 0,
            "totalRuns": 0,
            "successRate": 0,
            "activeVersion": "v1.0"
        }

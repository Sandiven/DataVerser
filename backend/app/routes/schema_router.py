# app/routes/schema_router.py
from fastapi import APIRouter, Query, HTTPException
from app.services import schema_service
from app.database import db

router = APIRouter(prefix="/schema", tags=["Schema"])

@router.get("/sources")
async def list_source_ids():
    """Get all source_ids that have schemas"""
    source_ids = await db.schemas.distinct("source_id")
    return {"source_ids": source_ids}

@router.get("/")
async def get_schema(source_id: str = Query(...)):
    schema = await schema_service.get_latest_schema(source_id)
    if not schema:
        raise HTTPException(status_code=404, detail="Schema not found")
    return schema

@router.get("/history")
async def get_schema_history(source_id: str = Query(...), limit: int = Query(100)):
    history = await schema_service.get_schema_history(source_id, limit=limit)
    # Return empty history instead of 404 - it's valid to have no history
    return history

@router.get("/latest")
async def get_latest_schema(source_id: str = Query(...)):
    """
    Return the latest schema version for a given source_id.
    Frontend expects this route.
    """
    latest = await schema_service.get_latest_schema(source_id)
    if not latest:
        raise HTTPException(status_code=404, detail="Schema not found")
    return latest

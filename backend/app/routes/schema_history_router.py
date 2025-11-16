# app/routes/schema_history_router.py
from fastapi import APIRouter, Query
from app.services.schema_service import get_schema_history

router = APIRouter(prefix="/schema-history", tags=["Schema History"])


@router.get("/")
async def schema_history(filename: str = Query(...), limit: int = 100):
    """
    Query parameters:
      - filename: required
      - limit: optional max versions to return
    """
    result = await get_schema_history(filename, limit=limit)
    return result

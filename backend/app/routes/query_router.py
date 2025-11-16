from fastapi import APIRouter, HTTPException, BackgroundTasks
from app.models.query_models import QueryRequest, QueryResponse
from app.services.query_service import run_query, get_query_results, get_queries_for_source

router = APIRouter(prefix="/queries", tags=["Queries"])


@router.post("/", response_model=QueryResponse)
async def execute_query(request: QueryRequest, background_tasks: BackgroundTasks):
    """
    Accepts a QueryRequest and triggers execution.
    """
    try:
        query_id = await run_query(request)
        return QueryResponse(
            query_id=query_id,
            status="queued",
            results=None,
            result_count=None
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{query_id}")
async def fetch_query_result(query_id: str):
    """
    Get results of a completed query.
    """
    result = await get_query_results(query_id)
    if not result:
        raise HTTPException(status_code=404, detail="Query not found")
    return result


@router.get("/source/{source_id}")
async def fetch_queries_by_source(source_id: str):
    """
    Fetch all queries submitted for a specific source.
    """
    results = await get_queries_for_source(source_id)
    return {"source_id": source_id, "queries": results}

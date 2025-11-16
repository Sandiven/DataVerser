import uuid
from typing import Optional, Dict, Any
from app.database import db
from app.services.llm_adapter import MockLLMAdapter
from app.services.semantic_extractor import SemanticExtractor
from app.utils.mongo import clean_mongo_document

llm = MockLLMAdapter()


# ---------------------------------------------------------
# EXECUTE QUERY ENTRY POINT
# ---------------------------------------------------------
async def run_query(request):
    """
    Creates a query record and schedules or executes it.
    """
    query_id = str(uuid.uuid4())

    await db.queries.insert_one({
        "query_id": query_id,
        "source_id": request.source_id,
        "mode": request.mode,
        "filter": request.filter,
        "query": request.query,
        "status": "queued",
    })

    # Execute immediately (sync)
    await run_query_logic(query_id, request)

    return query_id


# ---------------------------------------------------------
# INTERNAL LOGIC
# ---------------------------------------------------------
async def run_query_logic(query_id: str, request):
    """
    Main query execution logic.
    """
    source_id = request.source_id
    mode = request.mode

    # -------------------------------
    # BUILD MONGO FILTER
    # -------------------------------

    if mode == "mongo":
        mongo_filter = request.filter or {}

    elif mode == "mongo_nl":
        mongo_filter = await llm.generate_mongo_filter(request.query)

    elif mode == "semantic":
        extractor = SemanticExtractor()
        # fallback: interpret semantic extraction as simple search
        mongo_filter = extractor.extract_from_text(request.query)  
        mongo_filter = {}  # for now, semantic → no filter

    else:
        raise ValueError(f"Invalid query mode: {mode}")

    # -------------------------------
    # RUN QUERY
    # -------------------------------
    cursor = db.records.find(
        {"source_id": source_id, **mongo_filter}
    ).limit(2000)

    results = []
    async for doc in cursor:
        results.append(clean_mongo_document(doc))

    # -------------------------------
    # STORE RESULTS
    # -------------------------------
    await db.query_results.insert_one({
        "query_id": query_id,
        "results": results,
        "result_count": len(results)
    })

    await db.queries.update_one(
        {"query_id": query_id},
        {"$set": {"status": "done"}}
    )


# ---------------------------------------------------------
# GET RESULT BY QUERY ID
# ---------------------------------------------------------
async def get_query_results(query_id: str):
    doc = await db.query_results.find_one({"query_id": query_id})
    return clean_mongo_document(doc)


# ---------------------------------------------------------
# GET ALL QUERIES FOR A SOURCE
# ---------------------------------------------------------
async def get_queries_for_source(source_id: str):
    cursor = db.queries.find({"source_id": source_id})

    data = []
    async for doc in cursor:
        data.append(clean_mongo_document(doc))

    return data

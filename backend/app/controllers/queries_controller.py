# app/controllers/queries_controller.py

from fastapi import HTTPException, BackgroundTasks
from typing import Dict, Any, Optional

from app.services import query_service
from app.services import logs_service


class QueryController:

    @staticmethod
    async def handle_post_query(background_tasks: BackgroundTasks, payload: Dict[str, Any]):
        """
        Decides whether query is async or sync, validates inputs,
        delegates processing to query_service.
        """

        source_id = payload.get("source_id")
        mode = payload.get("mode")

        if not source_id or not mode:
            raise HTTPException(status_code=400, detail="source_id and mode are required")

        is_async = bool(payload.get("async", False))
        limit = payload.get("limit", None)

        # save 'pending' query entry in DB
        saved = await query_service.save_query_record(source_id, mode, payload, status="pending")
        query_id = saved["query_id"]

        # async query → background task
        if is_async:
            background_tasks.add_task(
                query_service.run_query_and_store_result,
                query_id, source_id, mode, payload, limit
            )
            await logs_service.save_log(source_id, f"Query scheduled: {query_id} (mode={mode})")
            return {"query_id": query_id, "status": "scheduled"}

        # sync query → process immediately
        await logs_service.save_log(source_id, f"Query started sync: {query_id} (mode={mode})")

        try:
            await query_service.run_query_and_store_result(
                query_id, source_id, mode, payload, limit
            )
            res = await query_service.get_query_results(query_id)
            return {
                "query_id": query_id,
                "status": "done",
                "results": res.get("results", []),
                "result_count": res.get("result_count", 0),
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @staticmethod
    async def handle_get_records(source_id: Optional[str], query_id: Optional[str], limit: int):
        """
        Fetch either:
        - query results (if query_id)
        - source documents (if source_id)
        """

        if query_id:
            res = await query_service.get_query_results(query_id)
            if not res:
                raise HTTPException(status_code=404, detail="Query results not found")
            return res

        if source_id:
            from app.database import db
            cursor = (
                db.records
                .find({"source_id": source_id})
                .sort("uploaded_at", -1)
                .limit(min(limit, 1000))
            )

            results = []
            async for doc in cursor:
                doc["_id"] = str(doc["_id"])
                results.append(doc)

            return {"source_id": source_id, "count": len(results), "results": results}

        raise HTTPException(status_code=400, detail="Either query_id or source_id must be provided")

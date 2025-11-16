# app/routes/upload_router.py

from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from app.services import storage_service, schema_service, logs_service
from app.database import db
from datetime import datetime
from typing import Optional
import logging

# Use backend-side ETL adapter
from app.services.dynamic_etl_adapter import run_dynamic_etl_bytes

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/upload", tags=["Upload"])


@router.post("/")
async def upload_file(
    file: UploadFile = File(...),
    source_id: Optional[str] = Form(None),
    version: Optional[str] = Form(None)
):
    filename = file.filename or "unknown"
    source_id = source_id or filename

    try:
        # ----------------------------------------------------
        # 1. Read raw bytes
        # ----------------------------------------------------
        content_bytes = await file.read()

        # ----------------------------------------------------
        # 2. Run Dynamic ETL (adapter)
        # ----------------------------------------------------
        try:
            logger.info(f"Running dynamic ETL for {filename}")
            etl_result = await run_dynamic_etl_bytes(content_bytes, filename)
            logger.info(f"Dynamic ETL extracted {etl_result.get('row_count', 0)} rows")
        except Exception as e:
            error_msg = f"Dynamic ETL failed: {str(e)}"
            logger.exception(error_msg)
            await logs_service.save_log(source_id, error_msg)
            raise HTTPException(status_code=500, detail=error_msg)

        # ----------------------------------------------------
        # 3. Save raw file in storage (GridFS)
        # ----------------------------------------------------
        storage_meta = {"source_id": source_id, "filename": filename}

        saved = await storage_service.save_raw_file_bytes(
            filename, content_bytes, metadata=storage_meta
        )

        # ----------------------------------------------------
        # 4. Insert processed rows into MongoDB
        # ----------------------------------------------------
        structured = etl_result.get("structured_data") or []
        record_ids = []

        if structured:
            docs = [
                {
                    "source_id": source_id,
                    "schema_version": None,   # will be updated soon
                    "data": row,
                    "uploaded_at": datetime.utcnow()
                }
                for row in structured
            ]
            res = await db.records.insert_many(docs)
            record_ids = [str(_id) for _id in res.inserted_ids]

        # ----------------------------------------------------
        # 5. Save schema version
        # ----------------------------------------------------
        schema_obj = etl_result.get("schema") or {}
        sv = await schema_service.save_schema_version(source_id, schema_obj)

        # ----------------------------------------------------
        # 6. Update stored records with schema version
        # ----------------------------------------------------
        if record_ids and sv and sv.get("schema_version"):
            await db.records.update_many(
                {"_id": {"$in": res.inserted_ids}},
                {"$set": {"schema_version": sv["schema_version"]}}
            )

        # ----------------------------------------------------
        # 7. Save LOG ENTRY with structured preview
        # ----------------------------------------------------
        structured_preview = etl_result.get("structured_data", [])[:5]

        await logs_service.save_log(
            source_id,
            f"Uploaded file {filename} (schema_v={sv.get('schema_version')})",
            extra={
                "filename": filename,
                "upload_id": saved.get("upload_id"),
                "schema_version": sv.get("schema_version"),
                "record_count": len(structured),
                "structured_preview": structured_preview,
                "cleaning_stats": etl_result.get("cleaning_stats", {})
            }
        )

        # ----------------------------------------------------
        # 8. Final response to frontend
        # ----------------------------------------------------
        return {
            "message": "Uploaded successfully",
            "filename": filename,
            "source_id": source_id,
            "upload_id": saved.get("upload_id"),
            "content_hash": saved.get("content_hash"),
            "schema_version": sv.get("schema_version"),
            "schema_id": sv.get("schema_id"),
            "schema_diff": sv.get("schema_diff"),
            "record_count": len(record_ids),
            "cleaning_stats": etl_result.get("cleaning_stats", {})
        }

    except Exception as e:
        await logs_service.save_log(source_id, f"Upload failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

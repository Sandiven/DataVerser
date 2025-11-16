# app/services/schema_service.py
from app.database import db
from datetime import datetime
from app.services.schema_diff_service import compare_schemas
from typing import Dict, Any, Optional, List
import uuid


def build_schema_from_dataframe(df):
    schema = {"fields": {}}

    for col in df.columns:
        series = df[col].dropna().tolist()
        example = series[0] if series else None

        # infer type
        if example is None:
            dtype = "null"
        elif isinstance(example, bool):
            dtype = "boolean"
        elif isinstance(example, int):
            dtype = "integer"
        elif isinstance(example, float):
            dtype = "number"
        else:
            dtype = "string"

        schema["fields"][col] = {
            "samples": series[:5],
            "example": example,
            "type": dtype
        }

    return schema

# Helper: enrich a schema object coming from ETL with canonical fields
def _enrich_field(field_name: str, sample_values: List, etl_field_meta: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Basic enrichment:
      - infer simple type based on sample_values (very lightweight)
      - add example_value, confidence, nullable, compatible_dbs
      - suggested_index heuristic
    """
    etl_field_meta = etl_field_meta or {}
    example = None
    inferred_type = "string"
    confidence = 0.6
    nullable = True

    # pick example if present
    if sample_values and len(sample_values) > 0:
        example = sample_values[0]
        # simple type inference
        try:
            if example is None:
                inferred_type = "null"
            elif isinstance(example, bool):
                inferred_type = "boolean"
            elif isinstance(example, int):
                inferred_type = "integer"
            elif isinstance(example, float):
                inferred_type = "number"
            else:
                # try parse number-like string
                s = str(example)
                if s.isdigit():
                    inferred_type = "integer"
                else:
                    try:
                        float(s)
                        inferred_type = "number"
                    except Exception:
                        inferred_type = "string"
            confidence = 0.95
        except Exception:
            inferred_type = "string"
            confidence = 0.6

    # suggested index heuristic
    suggested_index = False
    if field_name.lower().endswith(("id", "_id")) or "email" in field_name.lower():
        suggested_index = True

    return {
        "path": field_name,
        "type": inferred_type,
        "nullable": nullable,
        "example_value": example,
        "confidence": confidence,
        "compatible_dbs": ["mongodb"],   # user requested MongoDB only
        "suggested_index": suggested_index,
        "etl_meta": etl_field_meta
    }

def _canonicalize_schema_from_etl(schema_obj: Dict) -> Dict:
    """
    Input schema_obj is produced by ETL. We create canonical representation:
    {
      "schema_id": "...",
      "fields": [ {field-doc}, ... ],
      "generated_at": ...,
      "summary": {...}
    }
    This is intentionally small and Mongo-focused.
    """
    # ETL may produce different shapes; be defensive
    fields_list = []
    sample_values_map = {}
    # ETL commonly returns something like {'fields': {...}} or {'schemas': {'json': ['a','b']}}
    # Best-effort extraction:
    if not schema_obj:
        schema_obj = {}

    # If ETL gave per-field samples:
    if isinstance(schema_obj.get("fields"), dict):
        for fname, meta in schema_obj["fields"].items():
            # meta may include 'samples' or 'example'
            samples = meta.get("samples") if isinstance(meta.get("samples"), list) else [meta.get("example")] if meta.get("example") is not None else []
            fields_list.append(_enrich_field(fname, samples, etl_field_meta=meta))
    else:
        # fallback: flatten "schemas" lists or top-level list of keys
        if isinstance(schema_obj.get("schemas"), dict):
            union_fields = set()
            for v in schema_obj["schemas"].values():
                if isinstance(v, (list, tuple)):
                    union_fields.update([str(x) for x in v if x is not None])
            for fname in sorted(union_fields):
                fields_list.append(_enrich_field(fname, [], etl_field_meta={}))
        else:
            # maybe ETL returns a list of columns
            if isinstance(schema_obj, (list, tuple)):
                for fname in schema_obj:
                    fields_list.append(_enrich_field(str(fname), [], {}))
            else:
                # last resort: try keys
                for k in schema_obj.keys():
                    fields_list.append(_enrich_field(str(k), [], {}))

    canonical = {
        "schema_id": str(uuid.uuid4()),
        "fields": fields_list,
        "generated_at": datetime.utcnow(),
        "summary": {
            "field_count": len(fields_list)
        },
        # raw etl schema included for traceability
        "raw_schema": schema_obj
    }
    return canonical

async def save_schema_version(source_id: str, schema_obj: Dict) -> Dict:
    """
    Save a new schema version for a source_id.
    If previous schema identical (by raw_schema), returns previous version (idempotent).
    """
    # canonicalize
    canonical = _canonicalize_schema_from_etl(schema_obj)

    # find last schema for this source_id
    last = await db.schemas.find_one({"source_id": source_id}, sort=[("version", -1)])
    if last:
        last_raw = last.get("raw_schema")
        # quick idempotency check: if raw_schema equals canonical.raw_schema, reuse
        if last_raw == canonical["raw_schema"]:
            return {"schema_version": last["version"], "schema_id": last["schema_id"], "schema_diff": None, "reused": True}

        last_version = int(last.get("version", 0) or 0)
    else:
        last_version = 0
        last_raw = None

    new_version = last_version + 1
    schema_diff = compare_schemas(last_raw, canonical["raw_schema"]) if last_raw is not None else compare_schemas({}, canonical["raw_schema"])

    doc = {
        "source_id": source_id,
        "schema_id": canonical["schema_id"],
        "version": new_version,
        "schema": canonical,
        "raw_schema": canonical["raw_schema"],
        "schema_diff": schema_diff,
        "created_at": datetime.utcnow()
    }

    await db.schemas.insert_one(doc)
    return {"schema_version": new_version, "schema_id": canonical["schema_id"], "schema_diff": schema_diff, "reused": False}

async def get_latest_schema(source_id: str) -> Optional[Dict]:
    doc = await db.schemas.find_one({"source_id": source_id}, sort=[("version", -1)])
    if not doc:
        return None
    return {
        "schema_id": doc.get("schema_id"),
        "version": doc.get("version"),
        "schema": doc.get("schema"),
        "schema_diff": doc.get("schema_diff"),
        "created_at": doc.get("created_at")
    }

async def get_schema_history(source_id: str, limit: int = 100):
    cursor = db.schemas.find({"source_id": source_id}).sort("version", 1).limit(limit)
    history = []
    async for item in cursor:
        history.append({
            "schema_id": item.get("schema_id"),
            "version": item.get("version"),
            "schema": item.get("schema"),
            "schema_diff": item.get("schema_diff"),
            "created_at": item.get("created_at")
        })
    return {"source_id": source_id, "history": history}

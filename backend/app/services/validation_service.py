from app.database import db
from datetime import datetime
from .validators import VALIDATORS
from app.services.semantic_extractor import extract_semantic_schema

def extract_schema(file_content: bytes, filename: str = None):
    """
    Extract schema from ANY file type:
    - CSV
    - JSON
    - YAML
    - HTML
    - TXT
    - PDF (via semantic extractor)
    - Mixed unstructured data

    Returns:
      detected_types: list of matched validators
      schemas: { validator_type : schema_fields }
    """

    # 1️⃣ SAFELY DECODE FILE BYTES TO TEXT
    try:
        text = file_content.decode("utf-8", errors="ignore")
    except:
        try:
            text = file_content.decode("latin-1", errors="ignore")
        except:
            text = str(file_content)

    results = []

    # 2️⃣ RUN ALL STRUCTURED / SEMI-STRUCTURED VALIDATORS
    for validator in VALIDATORS:
        try:
            r = validator(text)
            if r:
                results.append(r)
        except Exception:
            continue

    detected_types = [r["type"] for r in results]

    # 3️⃣ COLLECT STRUCTURED SCHEMAS (CSV/JSON/YAML/etc.)
    schemas = {r["type"]: r["schema"] for r in results if r.get("schema")}

    # 4️⃣ DETECT IF FILE IS UNSTRUCTURED
    unstructured_types = {
        "free_text",
        "ocr_noise",
        "malformed_json",
        "javascript",
        "html_table",
        "yaml",          # YAML often mixed
        "kvp"            # Key-value text
    }

    # If ANY unstructured type is detected → run semantic extractor
    if (
        not schemas  # no structured schema found
        or any(t in unstructured_types for t in detected_types)
        or filename.endswith(".pdf")
        or filename.endswith(".txt")
    ):
        try:
            semantic = extract_semantic_schema(file_content, filename)
            schemas["semantic"] = semantic
        except Exception:
            pass

    # 5️⃣ RETURN FINAL STRUCTURED + SEMANTIC SCHEMA
    return {
        "detected_types": detected_types,
        "schemas": schemas
    }


async def get_schema_history(filename: str):
    cursor = db.documents.find({"filename": filename}).sort("uploaded_at", 1)
    history = []
    async for item in cursor:
        history.append({
            "schema": item.get("schema"),
            "uploaded_at": item.get("uploaded_at")
        })

    return {
        "filename": filename,
        "history": history
    }

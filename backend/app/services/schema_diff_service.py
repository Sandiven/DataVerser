# app/services/schema_diff_service.py
def _flatten_schema(schema_obj: dict):
    """
    Accepts the schema object produced by extract_schema:
      {
        "detected_types": [...],
        "schemas": {
           "json": ["a","b"],
           "csv": ["c","d"]
        }
      }
    Returns a set of field names (union of all discovered field lists).
    If the schema_obj structure is different, this function attempts best-effort flattening.
    """
    fields = set()

    if not schema_obj:
        return fields

    # If schema_obj has 'schemas' mapping, use it
    if isinstance(schema_obj, dict) and "schemas" in schema_obj and isinstance(schema_obj["schemas"], dict):
        for v in schema_obj["schemas"].values():
            if isinstance(v, (list, tuple)):
                fields.update([str(x) for x in v if x is not None])
    else:
        # If schema_obj is a simple list/dict fallback
        if isinstance(schema_obj, dict):
            for k, v in schema_obj.items():
                if isinstance(v, (list, tuple)):
                    fields.update([str(x) for x in v if x is not None])
        elif isinstance(schema_obj, (list, tuple)):
            fields.update([str(x) for x in schema_obj if x is not None])

    return fields


def compare_schemas(old_schema: dict, new_schema: dict) -> dict:
    """
    Compare two schema objects and return added / removed / unchanged fields.
    Works by flattening both schemas into field-name sets.
    """
    old_fields = _flatten_schema(old_schema) if old_schema else set()
    new_fields = _flatten_schema(new_schema) if new_schema else set()

    added = sorted(list(new_fields - old_fields))
    removed = sorted(list(old_fields - new_fields))
    unchanged = sorted(list(new_fields & old_fields))

    return {
        "added_fields": added,
        "removed_fields": removed,
        "unchanged_fields": unchanged,
        "old_count": len(old_fields),
        "new_count": len(new_fields)
    }

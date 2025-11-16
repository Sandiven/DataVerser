import tempfile
import os
import json
import re
import pandas as pd
import sys

# Add ETL root to path for imports
ETL_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../etl"))
if ETL_ROOT not in sys.path:
    sys.path.insert(0, ETL_ROOT)

# Import new ETL modules
from extract.extract import extract_data
from transform.transform_main import run_transform_pipeline


# -------------------------------------------------------------------
# Convert nested dicts/lists into JSON strings so duplicated() works
# -------------------------------------------------------------------
def make_hashable_df(df: pd.DataFrame) -> pd.DataFrame:
    df_obj = df.astype(object)
    return df_obj.applymap(
        lambda x: json.dumps(x, sort_keys=True)
        if isinstance(x, (dict, list))
        else x
    )


# -------------------------------------------------------------------
# COLUMN CLEANING HELPERS (Safe Version)
# -------------------------------------------------------------------
def clean_column_name(col):
    """Convert bad names (None, '', null, unknown) to 'unknown'."""
    if col is None:
        return "unknown"

    col = str(col).strip()

    if col.lower() in ("", "none", "null", "unknown"):
        return "unknown"

    return col


def remove_garbage_prefixes(col):
    """Remove leading garbage prefixes such as: unknown_, none_, null_"""
    return re.sub(r'^(?:unknown|none|null)_+', '', col, flags=re.IGNORECASE) or "unknown"


def fix_duplicate_columns(columns):
    """Ensure unique column names by appending suffixes."""
    new_cols = []
    seen = {}

    for col in columns:
        if col not in seen:
            seen[col] = 0
            new_cols.append(col)
        else:
            seen[col] += 1
            new_cols.append(f"{col}_{seen[col]}")

    return new_cols


# -------------------------------------------------------------------
# MAIN ENTRY: Dynamic ETL adapter
# -------------------------------------------------------------------
async def run_dynamic_etl_bytes(file_bytes: bytes, filename: str):
    """
    Backend → Dynamic ETL adapter.
    Safe for heavily nested JSON.
    Includes cleaning metrics.
    """

    # 1. Temp file preserving extension
    file_ext = os.path.splitext(filename)[1]
    with tempfile.NamedTemporaryFile(delete=False, suffix=file_ext) as tmp:
        tmp.write(file_bytes)
        tmp_path = tmp.name

    try:
        # ----------------------------------------------------
        # 2. Extract raw DataFrame
        # ----------------------------------------------------
        # New ETL returns tuple (df, fragments) if return_fragments=True, else just df
        extract_result = extract_data(tmp_path, return_fragments=False)
        
        # Handle both tuple and single return value
        if isinstance(extract_result, tuple):
            raw_df, fragments = extract_result
        else:
            raw_df = extract_result

        if raw_df is None or raw_df.empty:
            return {
                "structured_data": [],
                "schema": {"fields": [], "dtypes": {}},
                "row_count": 0,
                "cleaning_stats": {
                    "nullsRemoved": 0,
                    "rowsDroppedEmpty": 0,
                    "duplicatesDropped": 0,
                    "typesCast": 0,
                    "formatsFixed": 0
                }
            }

        # ----------------------------------------------------
        # 3. POST-EXTRACTION COLUMN CLEANUP (SAFE VERSION A)
        # ----------------------------------------------------

        # Step 1 — normalize column names
        raw_df.columns = [clean_column_name(c) for c in raw_df.columns]
        raw_df.columns = [remove_garbage_prefixes(c) for c in raw_df.columns]

        # Step 2 — drop garbage columns (empty / null)
        cols_to_drop = []
        for col in raw_df.columns:
            ser = raw_df[col]
            non_missing = ser.replace("", pd.NA).dropna()
            if non_missing.empty:
                cols_to_drop.append(col)

        if cols_to_drop:
            raw_df = raw_df.drop(columns=list(set(cols_to_drop)))

        # Step 3 — fix duplicate column names
        if len(raw_df.columns) != len(set(raw_df.columns)):
            raw_df.columns = fix_duplicate_columns(raw_df.columns)

        # ----------------------------------------------------
        # Return empty result if everything got dropped
        # ----------------------------------------------------
        if raw_df.empty:
            return {
                "structured_data": [],
                "schema": {"fields": [], "dtypes": {}},
                "row_count": 0,
                "cleaning_stats": {
                    "nullsRemoved": 0,
                    "rowsDroppedEmpty": 0,
                    "duplicatesDropped": 0,
                    "typesCast": 0,
                    "formatsFixed": 0
                }
            }

        # ----------------------------------------------------
        # BEFORE TRANSFORM: Snapshot Cleaning Stats
        #----------------------------------------------------
        safe_raw_df = make_hashable_df(raw_df)

        raw_nulls = raw_df.isna().sum().sum()
        raw_empty_rows = len(raw_df[raw_df.isna().all(axis=1)])
        raw_dupes = int(safe_raw_df.duplicated().sum())

        raw_dtypes = raw_df.dtypes.astype(str).to_dict()

        # ----------------------------------------------------
        # 4. TRANSFORM LAYER
        # ----------------------------------------------------
        processed_df = run_transform_pipeline(raw_df)

        safe_processed_df = make_hashable_df(processed_df)

        processed_nulls = processed_df.isna().sum().sum()
        processed_empty_rows = len(processed_df[processed_df.isna().all(axis=1)])
        processed_dupes = int(safe_processed_df.duplicated().sum())

        processed_dtypes = processed_df.dtypes.astype(str).to_dict()

        # ----------------------------------------------------
        # 5. Cleaning Metrics
        # ----------------------------------------------------
        cleaning_stats = {
            "nullsRemoved": int(raw_nulls - processed_nulls),
            "rowsDroppedEmpty": int(raw_empty_rows - processed_empty_rows),
            "duplicatesDropped": int(raw_dupes - processed_dupes),
            "typesCast": sum(1 for col in processed_dtypes if raw_dtypes.get(col) != processed_dtypes.get(col)),
            "formatsFixed": 0
        }

        # ----------------------------------------------------
        # 6. Final Output
        # ----------------------------------------------------
        structured_data = processed_df.to_dict(orient="records")

        schema = {
            "fields": list(processed_df.columns),
            "dtypes": processed_df.dtypes.astype(str).to_dict()
        }

        return {
            "structured_data": structured_data,
            "schema": schema,
            "row_count": len(structured_data),
            "cleaning_stats": cleaning_stats,
            "processed_df": processed_df
        }

    finally:
        try:
            os.remove(tmp_path)
        except:
            pass

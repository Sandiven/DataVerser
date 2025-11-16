# etl/extract/extract.py

import os
import time
import pandas as pd
from .file_handlers import READERS
from .smart_readers import smart_read_parts


def detect_file_type(file_path):
    """
    Detects file type based on its extension.
    """
    ext = os.path.splitext(file_path)[1].lower().replace('.', '')
    return ext


def extract_data(file_path, return_fragments=False):
    """
    Extracts data using the correct reader from file_handlers,
    measures time taken, and handles errors gracefully.
    
    Args:
        file_path: Path to file to extract
        return_fragments: If True, also return parsed_fragments_summary
    
    Returns:
        DataFrame or tuple (DataFrame, parsed_fragments_summary)
    """
    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        file_type = detect_file_type(file_path)
        print(f"\n📂 Detected file type: {file_type.upper()}")

        reader = READERS.get(file_type)
        if reader is None:
            print(f"⚠️ Unsupported file type: {file_type}")
            if return_fragments:
                return pd.DataFrame(), {}
            return pd.DataFrame()

        # Start timing extraction
        start_time = time.time()
        
        # For txt/html/md files, use smart_read_parts to get fragment summary
        parsed_fragments = {}
        if file_type in ['txt', 'html', 'md']:
            parts = smart_read_parts(file_path)
            # Count actual fragments, not rows
            json_count = 0
            if not parts["json"].empty:
                # Count unique JSON objects (rough estimate: number of rows or 1 if single object)
                json_count = max(1, len(parts["json"]))
            
            html_count = 0
            if not parts["html"].empty:
                html_count = max(1, len(parts["html"]))
            
            csv_count = 0
            if not parts["csv"].empty:
                csv_count = max(1, len(parts["csv"]))
            
            parsed_fragments = {
                "json_fragments": json_count,
                "html_tables": html_count,
                "csv_fragments": csv_count,
                "kv_pairs": 1 if not parts["kv"].empty else 0,
                "raw_text": 1 if not parts["raw_text"].empty else 0
            }
            # Combine parts
            dfs = [df for df in parts.values() if not df.empty]
            if dfs:
                df = pd.concat(dfs, ignore_index=True, sort=False)
            else:
                df = pd.DataFrame()
        else:
            df = reader(file_path)
            # Simple fragment detection for other types
            parsed_fragments = {
                "json_fragments": 1 if file_type == "json" else 0,
                "html_tables": 1 if file_type == "html" else 0,
                "csv_fragments": 1 if file_type in ["csv", "tsv"] else 0,
                "kv_pairs": 0,
                "raw_text": 0
            }
        
        duration = time.time() - start_time

        # Flatten nested JSON if detected
        if file_type == "json" and not df.empty:
            try:
                df = pd.json_normalize(df.to_dict(orient="records"))
            except Exception:
                pass

        record_count = len(df)
        print(f"✅ Extracted {record_count} records from {file_path} in {duration:.2f}s")

        if not df.empty:
            print(df.head())
        else:
            print("⚠️ No data extracted.")

        if return_fragments:
            return df, parsed_fragments
        return df

    except Exception as e:
        print(f"❌ Error during extraction: {e}")
        if return_fragments:
            return pd.DataFrame(), {}
        return pd.DataFrame()


# ==========================================================
# Standalone Runner (Optional)
# ==========================================================
if __name__ == "__main__":
    BASE = os.path.join(os.path.dirname(__file__), "..", "..")
    DATA_DIR = os.path.join(BASE, "data")

    test_files = [
        os.path.join(DATA_DIR, "day1.json"),
        os.path.join(DATA_DIR, "day2.csv"),
        os.path.join(DATA_DIR, "day3.html"),
        os.path.join(DATA_DIR, "day4.txt"),
    ]

    for file in test_files:
        print("\n-----------------------------------------")
        extract_data(file)

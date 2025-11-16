#!/usr/bin/env python
"""
ETL Pipeline Runner - Single file to test and run ETL on any file in tests/data folder.

Usage:
    python run_etl.py                    # Test all files in tests/data
    python run_etl.py mixed_html_in_txt.txt  # Test specific file
    python run_etl.py --api                # Test via API (requires server running)
"""

import os
import sys
import argparse
import logging
import json
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s'
)

# Add project root to path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from extract.extract import extract_data, detect_file_type
from transform.transform_main import run_transform_pipeline
from load.schema_generator import SchemaGenerator
from load.schema_evolution import SchemaEvolution
from load.db_loaders import get_loader

# MongoDB config
import os as os_module
mongo_config = {
    "connection_string": os_module.getenv("MONGO_CONNECTION_STRING", ""),
    "host": os_module.getenv("MONGO_HOST", "localhost"),
    "port": int(os_module.getenv("MONGO_PORT", 27017)),
    "database": os_module.getenv("MONGO_DB", "etl_db"),
    "user": os_module.getenv("MONGO_USER", ""),
    "password": os_module.getenv("MONGO_PASSWORD", "")
}


def process_file(file_path, source_id=None, load_to_mongo=True):
    """
    Process a single file through the complete ETL pipeline.
    
    Args:
        file_path: Path to file to process
        source_id: Optional source identifier (defaults to filename)
        load_to_mongo: Whether to load data into MongoDB
    
    Returns:
        dict with processing results
    """
    print(f"\n{'='*70}")
    print(f"Processing: {os.path.basename(file_path)}")
    print(f"{'='*70}")
    
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return None
    
    try:
        # Generate source_id from filename if not provided
        if not source_id:
            source_id = os.path.splitext(os.path.basename(file_path))[0].replace(" ", "_")
        
        # STEP 1: EXTRACT
        print("\n[1] EXTRACTING...")
        df, fragments = extract_data(file_path, return_fragments=True)
        print(f"   ✅ Extracted {len(df)} rows, {len(df.columns)} columns")
        if fragments:
            frag_summary = ", ".join([f"{k}:{v}" for k, v in fragments.items() if v > 0])
            if frag_summary:
                print(f"   Fragments: {frag_summary}")
        
        if df.empty:
            print("   ⚠️  No data extracted")
            return None
        
        # STEP 2: TRANSFORM
        print("\n[2] TRANSFORMING...")
        df_transformed = run_transform_pipeline(df, enable_enrichment=True, enable_conversions=True)
        print(f"   ✅ Transformed to {len(df_transformed)} rows, {len(df_transformed.columns)} columns")
        
        # STEP 3: GENERATE SCHEMA
        print("\n[3] GENERATING SCHEMA...")
        schema_generator = SchemaGenerator()
        schema = schema_generator.generate_schema(df_transformed, source_id, fragments)
        print(f"   ✅ Schema: {schema['schema_id']}")
        print(f"   Fields: {len(schema['fields'])}")
        
        # STEP 4: TRACK EVOLUTION
        print("\n[4] TRACKING SCHEMA EVOLUTION...")
        schema_evolution = SchemaEvolution(storage_path="schemas")
        schema = schema_evolution.add_schema(source_id, schema)
        print(f"   ✅ Version: {schema.get('version', 1)}")
        
        # STEP 5: LOAD TO MONGODB
        mongo_loaded = False
        if load_to_mongo and (mongo_config.get("connection_string") or mongo_config.get("host")):
            print("\n[5] LOADING TO MONGODB...")
            try:
                loader = get_loader("mongodb", mongo_config)
                collection_name = f"source_{source_id}"
                loader.load_data(df_transformed, collection_name, schema)
                mongo_loaded = True
                print(f"   ✅ Loaded to MongoDB")
                print(f"   Database: {mongo_config.get('database', 'etl_db')}")
                print(f"   Collection: {collection_name}")
            except Exception as e:
                print(f"   ⚠️  MongoDB load failed: {e}")
        else:
            print("\n[5] MONGODB...")
            print("   ℹ️  Skipped (not configured or disabled)")
        
        # SUMMARY
        print(f"\n{'='*70}")
        print("✅ PROCESSING COMPLETE")
        print(f"{'='*70}")
        print(f"File: {os.path.basename(file_path)}")
        print(f"Source ID: {source_id}")
        print(f"Records: {len(df_transformed)}")
        print(f"Fields: {len(schema['fields'])}")
        print(f"Schema ID: {schema['schema_id']}")
        if mongo_loaded:
            print(f"MongoDB: ✅ Loaded to {mongo_config.get('database', 'etl_db')}.source_{source_id}")
        
        return {
            "source_id": source_id,
            "file": file_path,
            "rows": len(df_transformed),
            "columns": len(df_transformed.columns),
            "schema_id": schema["schema_id"],
            "mongodb_loaded": mongo_loaded
        }
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return None


def get_test_files():
    """Get all test files from tests/data folder."""
    test_dir = Path("tests/data")
    if not test_dir.exists():
        return []
    
    files = []
    for ext in ['.txt', '.json', '.csv', '.html', '.md', '.pdf', '.xlsx', '.xml']:
        files.extend(test_dir.glob(f"*{ext}"))
    
    return sorted(files)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="ETL Pipeline Runner")
    parser.add_argument("file", nargs="?", help="Specific file to process (from tests/data)")
    parser.add_argument("--api", action="store_true", help="Test via API (requires server running)")
    parser.add_argument("--no-mongo", action="store_true", help="Skip MongoDB loading")
    parser.add_argument("--source-id", help="Custom source_id for the file")
    
    args = parser.parse_args()
    
    print("="*70)
    print("ETL Pipeline Runner")
    print("="*70)
    
    # API mode
    if args.api:
        print("\n⚠️  API mode requires server to be running")
        print("   Start server: python run_api.py")
        print("\nTesting via API...")
        try:
            import requests
            test_files = get_test_files()
            for file_path in test_files[:2]:  # Test first 2 files
                filename = file_path.name
                source_id = args.source_id or os.path.splitext(filename)[0]
                print(f"\nUploading {filename}...")
                with open(file_path, 'rb') as f:
                    files = {'file': (filename, f, 'text/plain')}
                    data = {'source_id': source_id}
                    response = requests.post("http://localhost:5000/upload", files=files, data=data)
                    if response.status_code == 201:
                        result = response.json()
                        print(f"✅ Uploaded: {result.get('record_count')} records")
                    else:
                        print(f"❌ Failed: {response.status_code}")
        except ImportError:
            print("❌ requests library not installed. Install with: pip install requests")
        except Exception as e:
            print(f"❌ API test failed: {e}")
        return
    
    # File processing mode
    if args.file:
        # Process specific file
        file_path = Path("tests/data") / args.file
        if not file_path.exists():
            file_path = Path(args.file)  # Try as absolute path
        
        if not file_path.exists():
            print(f"❌ File not found: {args.file}")
            return
        
        process_file(str(file_path), args.source_id, load_to_mongo=not args.no_mongo)
    else:
        # Process all files in tests/data
        test_files = get_test_files()
        
        if not test_files:
            print("❌ No test files found in tests/data/")
            return
        
        print(f"\nFound {len(test_files)} file(s) in tests/data/")
        print("Processing all files...\n")
        
        results = []
        for file_path in test_files:
            source_id = args.source_id or os.path.splitext(file_path.name)[0].replace(" ", "_")
            result = process_file(str(file_path), source_id, load_to_mongo=not args.no_mongo)
            if result:
                results.append(result)
        
        # Final summary
        print(f"\n{'='*70}")
        print("FINAL SUMMARY")
        print(f"{'='*70}")
        print(f"Files processed: {len(results)}/{len(test_files)}")
        if results:
            print(f"\nResults:")
            for r in results:
                mongo_status = "✅" if r.get('mongodb_loaded') else "❌"
                print(f"  {mongo_status} {r['source_id']}: {r['rows']} rows, {r['columns']} columns")
        print(f"\n{'='*70}")


if __name__ == "__main__":
    main()


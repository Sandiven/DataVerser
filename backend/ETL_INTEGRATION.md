# ETL Integration

This document describes how the ETL pipeline has been integrated with the backend FastAPI application.

## Overview

The backend now uses the comprehensive ETL pipeline from the root `etl/` folder. The integration includes:

1. **ETL Adapter** (`backend/app/services/dynamic_etl_adapter.py`): Bridges the gap between:
   - Backend's async/bytes interface
   - ETL's sync/file-path interface

2. **Updated Upload Router**: Automatically uses the ETL pipeline for file processing

3. **Dependencies**: All required packages have been added to `backend/requirements.txt`

## How It Works

### File Upload Flow

1. User uploads a file via `/upload` endpoint
2. Backend receives file as bytes
3. **Dynamic ETL Adapter**:
   - Creates a temporary file from bytes
   - Runs the full ETL pipeline (Extract → Transform → Load)
   - Converts results to backend's expected format
   - Cleans up temporary file

### ETL Pipeline Steps

1. **Extract**: Detects file type and extracts data using appropriate handlers (JSON, CSV, Excel, HTML, XML, etc.)
2. **Transform**: 
   - Cleans data
   - Validates data
   - Normalizes data
   - Enriches data (optional)
   - Converts types (optional)
3. **Load**: Backend handles storage separately using its own async MongoDB connection

### Key Features

- **Async-Safe**: All synchronous operations run in an executor to avoid blocking the event loop
- **Error Handling**: Graceful fallback if transformation fails
- **Multiple File Formats**: Supports JSON, CSV, Excel, HTML, XML, TXT, TSV, Parquet
- **Schema Extraction**: Automatically extracts and infers schema from processed data
- **Backward Compatible**: Falls back to basic ETL if dynamic ETL is unavailable

## File Structure

```
Project/
├── backend/
│   ├── app/
│   │   ├── services/
│   │   │   └── dynamic_etl_adapter.py  # ETL adapter
│   │   └── routes/
│   │       └── upload_router.py       # Uses ETL adapter
│   └── requirements.txt
└── etl/                              # ETL pipeline (root level)
    ├── extract/                       # File extraction
    ├── transform/                     # Data transformation
    └── load/                          # Schema generation & evolution
```

## Dependencies Added

The following packages were added to `backend/requirements.txt`:
- `beautifulsoup4` - HTML parsing
- `lxml` - XML/HTML parsing
- `openpyxl` - Excel file support
- `xlrd` - Legacy Excel file support
- `html5lib` - HTML parsing fallback
- `et-xmlfile` - Excel XML support
- `webencodings` - HTML encoding support
- `python-dateutil` - Date parsing

## Usage

The integration is automatic. When you upload a file:

```python
# The upload router automatically:
# 1. Uses the ETL adapter from app/services/dynamic_etl_adapter.py
# 2. Processes file through extract → transform pipeline
# 3. Returns structured data and schema
```

## Configuration

The dynamic ETL uses the same MongoDB connection as the backend (via environment variables):
- `MONGO_URI` or `MONGODB_URI`
- `MONGO_DB` or `DATABASE_NAME`

## Notes

- The adapter creates temporary files for processing. These are automatically cleaned up.
- Transformation errors are logged but don't stop the pipeline (raw data is used as fallback).
- The sync loader from dynamic_etl is optional and can be disabled (backend handles storage).


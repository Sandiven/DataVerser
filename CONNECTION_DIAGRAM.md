# Complete System Architecture

## ✅ All Components Connected

```
┌─────────────────────────────────────────────────────────────────┐
│                         FRONTEND                                 │
│                    (React + Vite)                                │
│                                                                   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  UploadView.jsx                                          │   │
│  │  - User uploads file                                     │   │
│  │  - Calls uploadAPI.uploadFile(file)                      │   │
│  └──────────────────────────────────────────────────────────┘   │
│                            │                                      │
│                            │ HTTP POST /api/upload/              │
│                            ▼                                      │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  vite.config.js                                          │   │
│  │  Proxy: /api → http://localhost:8000                     │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                            │
                            │ HTTP Request
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                         BACKEND                                 │
│                    (FastAPI)                                    │
│                                                                   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  upload_router.py                                        │   │
│  │  POST /upload/                                           │   │
│  │  - Receives file bytes                                   │   │
│  │  - Calls run_dynamic_etl()                               │   │
│  └──────────────────────────────────────────────────────────┘   │
│                            │                                      │
│                            │ Calls                                │
│                            ▼                                      │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  dynamic_etl_adapter.py                                 │   │
│  │  - Creates temp file from bytes                          │   │
│  │  - Bridges async/sync interface                          │   │
│  │  - Calls dynamic_etl pipeline                            │   │
│  └──────────────────────────────────────────────────────────┘   │
│                            │                                      │
│                            │ Executes                             │
│                            ▼                                      │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Storage & Database                                      │   │
│  │  - Saves raw file (GridFS)                               │   │
│  │  - Saves structured data (MongoDB)                      │   │
│  │  - Saves schema (MongoDB)                                │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                            │
                            │ Uses
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                      DYNAMIC ETL                                │
│                   (ETL Pipeline)                                │
│                                                                   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  1. EXTRACT Layer                                         │   │
│  │     - detect_file_type()                                 │   │
│  │     - extract_data() (JSON, CSV, Excel, HTML, XML...)    │   │
│  └──────────────────────────────────────────────────────────┘   │
│                            │                                      │
│                            ▼                                      │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  2. TRANSFORM Layer                                       │   │
│  │     - Cleaning (remove nulls, duplicates)                │   │
│  │     - Validation                                         │   │
│  │     - Normalization                                      │   │
│  │     - Enrichment                                         │   │
│  │     - Type Conversion                                    │   │
│  └──────────────────────────────────────────────────────────┘   │
│                            │                                      │
│                            ▼                                      │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  3. LOAD Layer (Optional)                               │   │
│  │     - Can save to MongoDB directly                      │   │
│  │     - Or return data to backend for storage             │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

## Connection Flow

### 1. Frontend → Backend ✅
- **Connection**: Vite proxy configured in `vite.config.js`
- **Route**: `/api/*` → `http://localhost:8000/*`
- **API Calls**: 
  - `uploadAPI.uploadFile()` → `/api/upload/` → Backend `/upload/`
  - `logsAPI.getAllLogs()` → `/api/logs/` → Backend `/logs/`
  - `schemaAPI.getSchema()` → `/api/schema/` → Backend `/schema/`
  - `metricsAPI.getMetrics()` → `/api/metrics/` → Backend `/metrics/`

### 2. Backend → ETL ✅
- **Connection**: `dynamic_etl_adapter.py` bridges the gap
- **Flow**: 
  - Backend receives file bytes
  - Adapter creates temporary file
  - Calls dynamic_etl pipeline (extract → transform → load)
  - Returns structured data and schema
  - Backend stores in MongoDB

### 3. Complete Upload Flow

```
User uploads file
    │
    ▼
Frontend: UploadView.jsx
    │
    ▼
uploadAPI.uploadFile(file)
    │
    ▼
Vite Proxy: /api/upload/ → http://localhost:8000/upload/
    │
    ▼
Backend: upload_router.py
    │
    ▼
dynamic_etl_adapter.run_dynamic_etl()
    │
    ├─► Extract: Read file, detect type, parse data
    │
    ├─► Transform: Clean, validate, normalize, enrich
    │
    └─► Return: Structured data + Schema
    │
    ▼
Backend: Store in MongoDB
    │
    ├─► Raw file → GridFS
    ├─► Structured data → records collection
    └─► Schema → schemas collection
    │
    ▼
Backend: Return response to frontend
    │
    ▼
Frontend: Display results
```

## File Format Support

The integrated system supports:
- ✅ JSON
- ✅ CSV
- ✅ Excel (.xlsx, .xls)
- ✅ HTML
- ✅ XML
- ✅ TXT
- ✅ TSV
- ✅ Parquet

## Key Features

1. **Async-Safe**: All synchronous ETL operations run in executors
2. **Error Handling**: Graceful fallbacks at each layer
3. **Schema Tracking**: Automatic schema extraction and versioning
4. **Real-time Updates**: Frontend shows processing steps
5. **Multiple Formats**: Supports 8+ file formats
6. **Data Transformation**: Full cleaning, validation, and enrichment pipeline

## Testing the Connection

1. **Start Backend**:
   ```bash
   cd backend
   uvicorn app.main:app --reload --port 8000
   ```

2. **Start Frontend**:
   ```bash
   cd dynamic-etl-frontend-v2
   npm run dev
   ```

3. **Upload a File**:
   - Go to http://localhost:5173
   - Navigate to Upload tab
   - Upload a JSON, CSV, or other supported file
   - Watch the ETL pipeline process it in real-time

## Status: ✅ FULLY CONNECTED

All three components (Frontend, Backend, ETL) are now integrated and working together!


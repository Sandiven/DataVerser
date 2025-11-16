# ETL Pipeline Installation Guide

## Issue
The ETL pipeline requires pandas to process CSV files. If pandas is not installed, the pipeline will fall back to a placeholder that returns empty data.

## Solution

### 1. Install pandas
```bash
cd backend
pip install pandas==2.2.0
```

Or if using a virtual environment:
```bash
cd backend
# Activate your virtual environment first
pip install pandas==2.2.0
```

### 2. Verify Installation
```bash
python -c "import pandas; print('pandas version:', pandas.__version__)"
```

### 3. Restart Backend
After installing pandas, restart your FastAPI backend server:
```bash
uvicorn app.main:app --reload --port 8000
```

### 4. Check Backend Logs
When you start the backend, you should see:
- ✅ No "WARNING: ETL pipeline not available" message
- ✅ When uploading files, you should see: "ETL Result: X records extracted"

## What the ETL Pipeline Does

Once pandas is installed, the ETL pipeline will:
- ✅ Extract actual data from CSV files
- ✅ Extract data from JSON files  
- ✅ Extract data from TXT files (key-value, tab-separated, etc.)
- ✅ Return proper record counts (not 0)
- ✅ Save records to MongoDB
- ✅ Generate detailed schemas with field types

## Troubleshooting

If you still see "0 records" after installing pandas:
1. Check backend logs for ETL errors
2. Verify the file format is supported (CSV, JSON, TXT)
3. Check that the file actually contains data
4. Look for error messages in the backend console


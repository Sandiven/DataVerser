# Backend Setup Instructions

## Installation Steps

To connect the backend with the ETL pipeline, you need to install all dependencies:

### 1. Install Backend Dependencies

```bash
cd backend
pip install -r requirements.txt
```

This will install all required packages including:
- pandas (for data processing)
- beautifulsoup4, lxml, html5lib (for HTML/XML parsing)
- openpyxl, xlrd (for Excel file support)
- All other backend dependencies

### 2. Verify Installation

Check if pandas is installed:
```bash
python -c "import pandas; print('pandas version:', pandas.__version__)"
```

### 3. Verify ETL Path

The backend should automatically find the `etl` directory at the project root. Verify it exists:
```bash
# From project root
ls etl/
```

### 4. Start the Backend

```bash
cd backend
uvicorn app.main:app --reload --port 8000
```

## Troubleshooting

### Error: "ETL modules not available"

**Possible causes:**

1. **Dependencies not installed**
   ```bash
   cd backend
   pip install -r requirements.txt
   ```

2. **Wrong Python environment**
   - Make sure you're using the correct virtual environment
   - Activate your virtual environment before running the backend

3. **Path issues**
   - Ensure `etl` directory exists at the project root (same level as `backend`)
   - The adapter automatically resolves the path, but verify the structure:
     ```
     Project/
     ├── backend/
     │   └── app/
     │       └── services/
     │           └── dynamic_etl_adapter.py
     └── etl/
         ├── extract/
         ├── transform/
         └── load/
     ```

4. **Missing packages**
   - Check if pandas is installed: `python -c "import pandas"`
   - Install missing packages: `pip install pandas beautifulsoup4 lxml openpyxl xlrd`

### Check Backend Logs

When starting the backend, it should load the ETL modules from the `etl/` directory at the project root.

If you see an error instead, check the error message for specific missing dependencies.

## Quick Test

After installation, test the connection:

```bash
# Start backend
cd backend
uvicorn app.main:app --reload

# In another terminal, test the upload endpoint
curl -X POST "http://localhost:8000/upload/" \
  -F "file=@path/to/test.csv"
```

You should see the ETL pipeline process the file successfully.


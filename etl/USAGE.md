# ETL Pipeline Usage Guide

## Quick Start

### 1. Setup Environment

Create `.env` file (copy from `env_template.txt`):
```env
MONGO_CONNECTION_STRING=mongodb+srv://user:pass@cluster.mongodb.net/database_name?retryWrites=true&w=majority
```

### 2. Run ETL on Test Files

```bash
# Process all files in tests/data/
python run_etl.py

# Process specific file
python run_etl.py mixed_html_in_txt.txt

# Process with custom source_id
python run_etl.py mock_input.txt --source-id my_custom_source
```

### 3. Start API Server

```bash
python run_api.py
```

Then access API at `http://localhost:5000`

## Main Files

- **`run_etl.py`** - Main ETL runner (processes files in tests/data)
- **`run_api.py`** - Start the REST API server
- **`README.md`** - Full documentation

## Project Structure

```
etl/
├── extract/          # Data extraction
├── transform/        # Data transformation
├── load/             # Schema generation & database loading
├── api/              # REST API server
├── tests/data/       # Test files
├── run_etl.py        # Main ETL runner ⭐
├── run_api.py        # API server starter
└── README.md         # Documentation
```

## What `run_etl.py` Does

For each file, it:
1. ✅ Extracts data (JSON, HTML, CSV, key-value pairs)
2. ✅ Transforms and cleans data
3. ✅ Generates database schema
4. ✅ Tracks schema evolution
5. ✅ Loads data into MongoDB (if configured)

## Examples

```bash
# Test all files
python run_etl.py

# Test specific file
python run_etl.py mixed_html_in_txt.txt

# Test without MongoDB
python run_etl.py --no-mongo

# Test via API (server must be running)
python run_etl.py --api
```

That's it! Simple and clean. 🚀


# Dynamic ETL Pipeline for Unstructured Data

A comprehensive ETL pipeline that ingests unstructured data files (.txt, .pdf, .md) containing mixed content (JSON, HTML, CSV, key-value pairs), extracts and cleans fields, generates database schemas, tracks schema evolution, and provides a REST API with LLM-driven query translation.

## Features

- **Multi-format Extraction**: Handles .txt, .pdf, .md files with mixed content (JSON, HTML tables, CSV, key-value pairs)
- **Smart Parsing**: Automatically detects and extracts structured fragments from unstructured text
- **Dynamic Schema Generation**: Generates schemas compatible with PostgreSQL, MongoDB, Neo4j, and JSON Schema
- **Schema Evolution**: Tracks schema changes over time with backward compatibility
- **REST API**: Full API with endpoints for upload, schema retrieval, querying, and more
- **LLM Query Translation**: Translates natural language queries to database queries
- **Multi-Database Support**: Load data into PostgreSQL, MongoDB, or Neo4j

## Installation

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

2. For PDF OCR support, install system dependencies:
   - **Windows**: Install [Tesseract OCR](https://github.com/UB-Mannheim/tesseract/wiki) and [Poppler](https://github.com/oschwartz10612/poppler-windows/releases)
   - **Linux**: `sudo apt-get install tesseract-ocr poppler-utils`
   - **macOS**: `brew install tesseract poppler`

3. (Optional) For LLM query translation, set environment variable:
```bash
export OPENAI_API_KEY="your-api-key"
export OPENAI_MODEL="gpt-3.5-turbo"  # or gpt-4
```

## Usage

### Starting the API Server

```bash
python -m api.server
```

The server will start on `http://localhost:5000`

### API Endpoints

#### 1. Upload File
```bash
curl -X POST http://localhost:5000/upload \
  -F "file=@path/to/file.txt" \
  -F "source_id=my_source" \
  -F "version=1"
```

Response:
```json
{
  "status": "ok",
  "source_id": "my_source",
  "file_id": "file_20251115_001",
  "schema_id": "schema_my_source_20251115_120000",
  "parsed_fragments_summary": {
    "json_fragments": 2,
    "html_tables": 1,
    "csv_fragments": 1,
    "kv_pairs": 1,
    "raw_text": 0
  },
  "record_count": 150
}
```

#### 2. Get Schema
```bash
curl http://localhost:5000/schema?source_id=my_source
```

#### 3. Get Schema History
```bash
curl http://localhost:5000/schema/history?source_id=my_source
```

#### 4. Execute Query
```bash
curl -X POST http://localhost:5000/query \
  -H "Content-Type: application/json" \
  -d '{
    "source_id": "my_source",
    "nl_query": "Find all products with price greater than 100",
    "db_type": "postgresql"
  }'
```

#### 5. Get Records
```bash
curl http://localhost:5000/records?source_id=my_source
```

### Programmatic Usage

```python
from extract.extract import extract_data
from transform.transform_main import run_transform_pipeline
from load.schema_generator import SchemaGenerator
from load.schema_evolution import SchemaEvolution

# Extract
df, fragments = extract_data("data.txt", return_fragments=True)

# Transform
df_transformed = run_transform_pipeline(df)

# Generate Schema
schema_gen = SchemaGenerator()
schema = schema_gen.generate_schema(df_transformed, "source_1", fragments)

# Track Evolution
schema_evol = SchemaEvolution()
schema = schema_evol.add_schema("source_1", schema)

# Get PostgreSQL DDL
ddl = schema_gen.generate_postgresql_ddl(schema, "products")
print(ddl)
```

## Project Structure

```
etl/
├── extract/          # Extraction layer
│   ├── extract.py           # Main extraction logic
│   ├── file_handlers.py     # File type handlers
│   ├── smart_readers.py     # Mixed format parsers
│   └── pdf_readers.py       # PDF extraction
├── transform/        # Transformation layer
│   ├── transform_main.py    # Pipeline orchestrator
│   ├── cleaning.py          # Data cleaning
│   ├── normalization.py     # Type normalization
│   ├── enrichment.py        # Data enrichment
│   └── converters.py         # Type conversion
├── load/             # Load layer
│   ├── schema_generator.py  # Schema generation
│   ├── schema_evolution.py  # Schema versioning
│   └── db_loaders.py        # Database loaders
├── api/              # REST API
│   └── server.py            # Flask API server
└── tests/            # Test data
    └── data/                # Sample files
```

## Database Configuration

Set environment variables for database connections:

```bash
# PostgreSQL
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DB=etl_db
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=password

# MongoDB
# Option 1: Use full connection string (recommended for MongoDB Atlas)
export MONGO_CONNECTION_STRING="mongodb+srv://username:password@cluster.mongodb.net/database_name?retryWrites=true&w=majority"

# Option 2: Use individual components (for local MongoDB)
# export MONGO_HOST=localhost
# export MONGO_PORT=27017
# export MONGO_DB=etl_db
# export MONGO_USER=
# export MONGO_PASSWORD=

# Neo4j
export NEO4J_HOST=localhost
export NEO4J_PORT=7687
export NEO4J_USER=neo4j
export NEO4J_PASSWORD=password
```

## Schema Evolution

The system automatically tracks schema changes:

- **Added fields**: New columns detected in subsequent uploads
- **Removed fields**: Fields that disappear
- **Modified fields**: Type changes or nullability changes
- **Migration scripts**: Auto-generated SQL/NoSQL migration commands

Example schema evolution:
```json
{
  "schema_id": "schema_v2",
  "version": 2,
  "migration_notes": "Added pricing.price as decimal, previous price stored as string in legacy_price",
  "changes": {
    "added": [{"name": "pricing.price", "type": "decimal"}],
    "removed": [],
    "modified": [{"field": "price_usd", "old": {"type": "string"}, "new": {"type": "decimal"}}]
  }
}
```

## Testing

### Quick Test - Process All Files

Test all files in `tests/data/` folder:
```bash
python run_etl.py
```

### Test Specific File

Test a specific file:
```bash
python run_etl.py mixed_html_in_txt.txt
python run_etl.py mock_input.txt
```

### Test via API

```bash
# Terminal 1: Start server
python run_api.py

# Terminal 2: Test via API
python run_etl.py --api
```

### Options

```bash
python run_etl.py [filename] [options]

Options:
  --no-mongo      Skip MongoDB loading
  --source-id     Custom source identifier
  --api           Test via API (requires server)
```

### Test Files Available

Files in `tests/data/`:
- `mixed_html_in_txt.txt` - Mixed format (JSON, HTML, CSV, key-value)
- `mock_input.txt` - Complex real-world example
- `day1.json`, `day2.csv`, `day3.html`, etc. - Various formats

## Limitations & Future Improvements

- LLM query translation currently uses simple rule-based fallback if OpenAI API is not configured
- PDF OCR requires system-level Tesseract installation
- Database connections are optional (system works without DBs for schema generation)
- Schema evolution migration scripts are generated but not automatically executed

## License

MIT


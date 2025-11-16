"""
API server for the ETL pipeline.

Provides REST endpoints for:
- File upload
- Schema retrieval
- Schema history
- Query execution
- Record retrieval
"""

import os
import sys
import logging
import uuid
import json
from datetime import datetime
from typing import Optional, Dict, Any
from flask import Flask, request, jsonify, send_file
from werkzeug.utils import secure_filename
import pandas as pd

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    # Load .env file from project root
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env_path = os.path.join(project_root, '.env')
    if os.path.exists(env_path):
        load_dotenv(dotenv_path=env_path)
        print(f"✅ Loaded environment variables from {env_path}")
    else:
        print(f"⚠️  .env file not found at {env_path}")
        print("   Create a .env file (copy from env_template.txt) or set environment variables manually.")
except ImportError:
    print("⚠️  python-dotenv not installed. Install with: pip install python-dotenv")
    print("   Environment variables must be set manually.")

# Fix imports to work from project root
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from extract.extract import extract_data, detect_file_type
from extract.smart_readers import smart_read_parts
from transform.transform_main import run_transform_pipeline
from load.schema_generator import SchemaGenerator
from load.schema_evolution import SchemaEvolution
from load.db_loaders import get_loader

logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB max file size
app.config['UPLOAD_FOLDER'] = 'uploads'

# Initialize components
schema_generator = SchemaGenerator()
schema_evolution = SchemaEvolution(storage_path="schemas")
file_storage = {}  # In-memory storage: source_id -> {files: [], schemas: []}

# Database connections (configure as needed)
db_configs = {
    "postgresql": {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", 5432)),
        "database": os.getenv("POSTGRES_DB", "etl_db"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "")
    },
    "mongodb": {
        "connection_string": os.getenv("MONGO_CONNECTION_STRING", ""),  # Full MongoDB URL (e.g., mongodb+srv://user:pass@cluster.mongodb.net/)
        "host": os.getenv("MONGO_HOST", "localhost"),
        "port": int(os.getenv("MONGO_PORT", 27017)),
        "database": os.getenv("MONGO_DB", "etl_db"),
        "user": os.getenv("MONGO_USER", ""),
        "password": os.getenv("MONGO_PASSWORD", "")
    },
    "neo4j": {
        "host": os.getenv("NEO4J_HOST", "localhost"),
        "port": int(os.getenv("NEO4J_PORT", 7687)),
        "database": os.getenv("NEO4J_DB", "neo4j"),
        "user": os.getenv("NEO4J_USER", "neo4j"),
        "password": os.getenv("NEO4J_PASSWORD", "")
    }
}

# Create upload directory
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs("schemas", exist_ok=True)


@app.route('/upload', methods=['POST'])
def upload_file():
    """
    Upload a file for processing.
    
    Expected form data:
    - file: binary file
    - source_id: (optional) source identifier
    - version: (optional) version number
    
    Returns:
        JSON with file_id, schema_id, parsed_fragments_summary
    """
    try:
        if 'file' not in request.files:
            return jsonify({"error": "No file provided"}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "No file selected"}), 400
        
        source_id = request.form.get('source_id') or f"source_{uuid.uuid4().hex[:8]}"
        version = request.form.get('version')
        
        # Save file
        filename = secure_filename(file.filename)
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], f"{source_id}_{filename}")
        file.save(file_path)
        
        # Extract data
        logger.info(f"Extracting data from {file_path}")
        file_type = detect_file_type(file_path)
        
        # Use smart_read_parts to get fragment summary
        if file_type in ['txt', 'html', 'md']:
            parts = smart_read_parts(file_path)
            parsed_fragments = {
                "json_fragments": 1 if not parts["json"].empty else 0,
                "html_tables": 1 if not parts["html"].empty else 0,
                "csv_fragments": 1 if not parts["csv"].empty else 0,
                "kv_pairs": 1 if not parts["kv"].empty else 0,
                "raw_text": 1 if not parts["raw_text"].empty else 0
            }
            # Combine parts for processing
            dfs = [df for df in parts.values() if not df.empty]
            if dfs:
                df = pd.concat(dfs, ignore_index=True, sort=False)
            else:
                df = pd.DataFrame()
        else:
            df = extract_data(file_path)
            parsed_fragments = {
                "json_fragments": 1 if file_type == "json" else 0,
                "html_tables": 1 if file_type == "html" else 0,
                "csv_fragments": 1 if file_type == "csv" else 0,
                "kv_pairs": 0,
                "raw_text": 0
            }
        
        # Transform data
        logger.info("Transforming data")
        df_transformed = run_transform_pipeline(df, enable_enrichment=True, enable_conversions=True)
        
        # Generate schema
        logger.info("Generating schema")
        schema = schema_generator.generate_schema(df_transformed, source_id, parsed_fragments)
        
        # Track schema evolution
        schema = schema_evolution.add_schema(source_id, schema)
        
        # Store file info
        file_id = f"file_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        if source_id not in file_storage:
            file_storage[source_id] = {"files": [], "schemas": []}
        
        file_storage[source_id]["files"].append({
            "file_id": file_id,
            "filename": filename,
            "file_path": file_path,
            "uploaded_at": datetime.now().isoformat(),
            "version": version
        })
        file_storage[source_id]["schemas"].append(schema)
        
        # Load into MongoDB (if configured)
        mongo_loaded = False
        try:
            mongo_config = db_configs["mongodb"]
            if mongo_config.get("connection_string") or mongo_config.get("host"):
                logger.info("Attempting to load data into MongoDB...")
                loader = get_loader("mongodb", mongo_config)
                # Collection name based on source_id
                collection_name = f"source_{source_id}"
                loader.load_data(df_transformed, collection_name, schema)
                mongo_loaded = True
                logger.info(f"✅ Data successfully loaded into MongoDB collection: {collection_name}")
        except Exception as e:
            logger.error(f"❌ Failed to load data into MongoDB: {e}")
            logger.error("   Check your MongoDB connection string and network settings")
            # Don't fail the upload, just log the error
        
        response_data = {
            "status": "ok",
            "source_id": source_id,
            "file_id": file_id,
            "schema_id": schema["schema_id"],
            "parsed_fragments_summary": parsed_fragments,
            "record_count": len(df_transformed),
            "mongodb_loaded": mongo_loaded
        }
        
        if mongo_loaded:
            response_data["mongodb_collection"] = f"source_{source_id}"
            response_data["mongodb_database"] = db_configs["mongodb"].get("database", "etl_db")
        
        return jsonify(response_data), 201
    
    except Exception as e:
        logger.exception("Upload failed")
        return jsonify({"error": str(e)}), 500


@app.route('/schema', methods=['GET'])
def get_schema():
    """
    Get current schema for a source_id.
    
    Query params:
    - source_id: source identifier
    
    Returns:
        Schema dictionary
    """
    source_id = request.args.get('source_id')
    if not source_id:
        return jsonify({"error": "source_id required"}), 400
    
    schema = schema_evolution.get_schema(source_id)
    if not schema:
        return jsonify({"error": "Schema not found"}), 404
    
    return jsonify(schema), 200


@app.route('/schema/history', methods=['GET'])
def get_schema_history():
    """
    Get schema history for a source_id.
    
    Query params:
    - source_id: source identifier
    
    Returns:
        List of schema versions with diffs
    """
    source_id = request.args.get('source_id')
    if not source_id:
        return jsonify({"error": "source_id required"}), 400
    
    history = schema_evolution.get_schema_history(source_id)
    if not history:
        return jsonify({"error": "Schema history not found"}), 404
    
    return jsonify(history), 200


@app.route('/records', methods=['GET'])
def get_records():
    """
    Get records for a query.
    
    Query params:
    - source_id: source identifier
    - query_id: (optional) query identifier for async queries
    
    Returns:
        List of records
    """
    source_id = request.args.get('source_id')
    query_id = request.args.get('query_id')
    
    if not source_id:
        return jsonify({"error": "source_id required"}), 400
    
    # For now, return stored data
    # In production, this would query the actual database
    if source_id in file_storage:
        # Return all records from all files for this source
        records = []
        for file_info in file_storage[source_id]["files"]:
            file_path = file_info["file_path"]
            if os.path.exists(file_path):
                df = extract_data(file_path)
                df_transformed = run_transform_pipeline(df)
                records.extend(df_transformed.to_dict('records'))
        
        return jsonify({
            "source_id": source_id,
            "record_count": len(records),
            "records": records[:100]  # Limit to 100 for response
        }), 200
    
    return jsonify({"error": "Source not found"}), 404


@app.route('/query', methods=['POST'])
def execute_query():
    """
    Execute a query (with optional LLM translation).
    
    Request body:
    {
        "source_id": "source_123",
        "nl_query": "Find all products with price > 100",  # Natural language
        "db_query": "SELECT * FROM products WHERE price > 100",  # Or direct DB query
        "db_type": "postgresql"  # postgresql, mongodb, neo4j
    }
    
    Returns:
        Query results
    """
    try:
        data = request.get_json()
        source_id = data.get('source_id')
        nl_query = data.get('nl_query')
        db_query = data.get('db_query')
        db_type = data.get('db_type', 'postgresql')
        
        if not source_id:
            return jsonify({"error": "source_id required"}), 400
        
        # If natural language query provided, translate to DB query
        if nl_query and not db_query:
            db_query = translate_nl_to_query(nl_query, source_id, db_type)
        
        if not db_query:
            return jsonify({"error": "No query provided"}), 400
        
        # Get schema to determine table/collection name
        schema = schema_evolution.get_schema(source_id)
        if not schema:
            return jsonify({"error": "Schema not found"}), 404
        
        # Execute query
        loader = get_loader(db_type, db_configs.get(db_type, {}))
        loader.connect()
        
        if db_type == "postgresql":
            # For PostgreSQL, use direct SQL
            results = loader.query(db_query)
        elif db_type == "mongodb":
            # For MongoDB, expect JSON query
            results = loader.query(db_query)
        elif db_type == "neo4j":
            # For Neo4j, use Cypher
            results = loader.query(db_query)
        else:
            return jsonify({"error": f"Unsupported database type: {db_type}"}), 400
        
        loader.close()
        
        return jsonify({
            "source_id": source_id,
            "query": db_query,
            "result_count": len(results),
            "results": results
        }), 200
    
    except Exception as e:
        logger.exception("Query execution failed")
        return jsonify({"error": str(e)}), 500


def translate_nl_to_query(nl_query: str, source_id: str, db_type: str) -> str:
    """
    Translate natural language query to database query using LLM.
    
    Supports OpenAI API or simple rule-based fallback.
    """
    # Get schema for context
    schema = schema_evolution.get_schema(source_id)
    if not schema:
        return None
    
    logger.info(f"Translating NL query: {nl_query}")
    
    # Try OpenAI API if available
    try:
        import openai
        api_key = os.getenv("OPENAI_API_KEY")
        if api_key:
            openai.api_key = api_key
            
            # Build schema context
            fields_str = ", ".join([f["name"] for f in schema.get("fields", [])])
            schema_context = f"Available fields: {fields_str}"
            
            # Create prompt
            system_prompt = f"""You are a SQL query translator. Translate natural language queries to {db_type} queries.
Schema: {schema_context}
Return only the query, no explanations."""
            
            try:
                # Use OpenAI API (adjust for your version)
                if hasattr(openai, 'ChatCompletion'):
                    response = openai.ChatCompletion.create(
                        model=os.getenv("OPENAI_MODEL", "gpt-3.5-turbo"),
                        messages=[
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": nl_query}
                        ],
                        temperature=0.1
                    )
                    query = response.choices[0].message.content.strip()
                    logger.info(f"LLM translated query: {query}")
                    return query
            except Exception as e:
                logger.warning(f"OpenAI API call failed: {e}, using fallback")
    except ImportError:
        logger.debug("OpenAI not installed, using rule-based translation")
    except Exception as e:
        logger.warning(f"LLM translation error: {e}, using fallback")
    
    # Fallback: Simple rule-based translation
    nl_lower = nl_query.lower()
    fields = [f["name"] for f in schema.get("fields", [])]
    
    if db_type == "postgresql":
        # Simple pattern matching for SQL
        if "find all" in nl_lower or "get all" in nl_lower or "list" in nl_lower:
            # Look for WHERE conditions
            if "where" in nl_lower or "with" in nl_lower:
                # Extract field and condition
                query = "SELECT * FROM data_table WHERE "
                # Simple extraction (very basic)
                for field in fields:
                    if field.lower() in nl_lower:
                        if ">" in nl_query:
                            value = nl_query.split(">")[-1].strip().split()[0]
                            query += f"{field} > {value}"
                        elif "<" in nl_query:
                            value = nl_query.split("<")[-1].strip().split()[0]
                            query += f"{field} < {value}"
                        elif "=" in nl_query or "equals" in nl_lower:
                            # Extract value after equals
                            parts = nl_query.split("=")
                            if len(parts) > 1:
                                value = parts[-1].strip().split()[0]
                                query += f"{field} = '{value}'"
                        break
                return query
            return "SELECT * FROM data_table LIMIT 100"
        return "SELECT * FROM data_table LIMIT 100"
    
    elif db_type == "mongodb":
        # Simple MongoDB query
        query_dict = {"collection": "data_collection", "filter": {}, "projection": {}}
        return json.dumps(query_dict)
    
    elif db_type == "neo4j":
        # Simple Cypher query
        return "MATCH (n) RETURN n LIMIT 100"
    
    return None


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({"status": "healthy"}), 200


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    app.run(host='0.0.0.0', port=5000, debug=True)


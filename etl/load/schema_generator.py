"""
Schema generation module.

Generates database schemas from DataFrame structures with metadata
for multiple database types (PostgreSQL, MongoDB, Neo4j, JSON Schema).
"""

import logging
import pandas as pd
import json
from datetime import datetime
from typing import Dict, List, Any, Optional
import re

logger = logging.getLogger(__name__)


class SchemaGenerator:
    """Generates schemas from DataFrames with multi-DB compatibility."""
    
    def __init__(self):
        self.compatible_dbs = ["postgresql", "mongodb", "neo4j", "json_schema"]
    
    def infer_field_type(self, series: pd.Series, field_name: str) -> Dict[str, Any]:
        """
        Infer field type with confidence score and example value.
        Returns dict with type, nullable, example_value, confidence.
        """
        field_info = {
            "name": field_name,
            "path": f"$.{field_name}",
            "nullable": series.isna().any(),
            "example_value": None,
            "confidence": 0.0,
            "source_offsets": None,
            "suggested_index": False
        }
        
        # Get non-null values for analysis
        non_null = series.dropna()
        if len(non_null) == 0:
            field_info["type"] = "string"
            field_info["confidence"] = 0.5
            return field_info
        
        # Get example value
        field_info["example_value"] = str(non_null.iloc[0])[:100]  # Limit length
        
        # Type inference with confidence
        dtype = series.dtype
        dtype_str = str(dtype).lower()
        
        # Check for numeric types
        if pd.api.types.is_integer_dtype(dtype):
            field_info["type"] = "integer"
            field_info["confidence"] = 0.95
            field_info["suggested_index"] = True
        elif pd.api.types.is_float_dtype(dtype):
            field_info["type"] = "decimal"
            field_info["confidence"] = 0.95
        elif pd.api.types.is_bool_dtype(dtype):
            field_info["type"] = "boolean"
            field_info["confidence"] = 0.95
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            field_info["type"] = "date"
            field_info["confidence"] = 0.95
        elif pd.api.types.is_string_dtype(dtype) or dtype == 'object' or 'category' in dtype_str:
            # Check for date-like strings
            if self._looks_like_date(non_null):
                field_info["type"] = "date"
                field_info["confidence"] = 0.85
            # Check for numeric strings
            elif self._looks_like_number(non_null):
                field_info["type"] = "decimal"  # Union type: could be string or number
                field_info["confidence"] = 0.70
            else:
                field_info["type"] = "string"
                field_info["confidence"] = 0.90
        
        # Check for ID-like fields (high index potential)
        if any(keyword in field_name.lower() for keyword in ['id', 'key', 'pk', '_id']):
            field_info["suggested_index"] = True
            if "type" in field_info and field_info["type"] == "string":
                field_info["confidence"] = 0.98
        
        # Ensure type is always set (fallback for edge cases)
        if "type" not in field_info:
            field_info["type"] = "string"
            field_info["confidence"] = 0.5
        
        return field_info
    
    def _looks_like_date(self, series: pd.Series) -> bool:
        """Check if series contains date-like strings."""
        if len(series) == 0:
            return False
        sample = series.head(10).astype(str)
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
            r'\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
            r'\d{2}-\d{2}-\d{4}',  # DD-MM-YYYY
            r'\d{4}/\d{2}/\d{2}',   # YYYY/MM/DD
        ]
        matches = 0
        for val in sample:
            for pattern in date_patterns:
                if re.search(pattern, val):
                    matches += 1
                    break
        return matches >= len(sample) * 0.5  # At least 50% match
    
    def _looks_like_number(self, series: pd.Series) -> bool:
        """Check if series contains numeric strings."""
        if len(series) == 0:
            return False
        sample = series.head(10).astype(str)
        numeric_count = 0
        for val in sample:
            try:
                float(re.sub(r'[^\d.]', '', val))
                numeric_count += 1
            except:
                pass
        return numeric_count >= len(sample) * 0.7  # At least 70% numeric
    
    def generate_schema(
        self,
        df: pd.DataFrame,
        source_id: str,
        parsed_fragments: Optional[Dict[str, int]] = None
    ) -> Dict[str, Any]:
        """
        Generate schema from DataFrame.
        
        Args:
            df: Input DataFrame
            source_id: Source identifier
            parsed_fragments: Summary of parsed fragments (json_fragments, html_tables, etc.)
        
        Returns:
            Schema dictionary with metadata
        """
        if df.empty:
            return self._empty_schema(source_id)
        
        schema_id = f"schema_{source_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Infer field types
        fields = []
        for col in df.columns:
            field_info = self.infer_field_type(df[col], col)
            fields.append(field_info)
        
        # Identify primary key candidates
        primary_key_candidates = []
        for field in fields:
            if field["suggested_index"] and "id" in field["name"].lower():
                primary_key_candidates.append(field["name"])
        
        # If no ID fields, use first field as candidate
        if not primary_key_candidates and fields:
            primary_key_candidates.append(fields[0]["name"])
        
        schema = {
            "schema_id": schema_id,
            "generated_at": datetime.now().isoformat() + "Z",
            "compatible_dbs": self.compatible_dbs,
            "fields": fields,
            "primary_key_candidates": primary_key_candidates,
            "migration_notes": None,
            "parsed_fragments_summary": parsed_fragments or {}
        }
        
        return schema
    
    def _empty_schema(self, source_id: str) -> Dict[str, Any]:
        """Return empty schema structure."""
        return {
            "schema_id": f"schema_{source_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "generated_at": datetime.now().isoformat() + "Z",
            "compatible_dbs": self.compatible_dbs,
            "fields": [],
            "primary_key_candidates": [],
            "migration_notes": None,
            "parsed_fragments_summary": {}
        }
    
    def generate_postgresql_ddl(self, schema: Dict[str, Any], table_name: str) -> str:
        """Generate PostgreSQL DDL from schema."""
        ddl = [f"CREATE TABLE IF NOT EXISTS {table_name} ("]
        
        field_defs = []
        for field in schema["fields"]:
            pg_type = self._pandas_to_postgresql_type(field["type"])
            nullable = "NULL" if field["nullable"] else "NOT NULL"
            field_defs.append(f"    {field['name']} {pg_type} {nullable}")
        
        ddl.append(",\n".join(field_defs))
        
        # Add primary key if candidates exist
        if schema["primary_key_candidates"]:
            pk = schema["primary_key_candidates"][0]
            ddl.append(f",\n    PRIMARY KEY ({pk})")
        
        ddl.append("\n);")
        
        # Add indexes
        for field in schema["fields"]:
            if field["suggested_index"]:
                idx_name = f"idx_{table_name}_{field['name']}"
                ddl.append(f"\nCREATE INDEX IF NOT EXISTS {idx_name} ON {table_name} ({field['name']});")
        
        return "\n".join(ddl)
    
    def generate_mongodb_schema(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Generate MongoDB JSON schema."""
        properties = {}
        required = []
        
        for field in schema["fields"]:
            mongo_type = self._pandas_to_mongodb_type(field["type"])
            properties[field["name"]] = {
                "bsonType": mongo_type,
                "description": f"Field: {field['name']}"
            }
            if not field["nullable"]:
                required.append(field["name"])
        
        return {
            "$jsonSchema": {
                "bsonType": "object",
                "required": required,
                "properties": properties
            }
        }
    
    def generate_neo4j_schema(self, schema: Dict[str, Any], label: str) -> Dict[str, Any]:
        """Generate Neo4j node schema."""
        properties = {}
        for field in schema["fields"]:
            neo4j_type = self._pandas_to_neo4j_type(field["type"])
            properties[field["name"]] = {
                "type": neo4j_type,
                "nullable": field["nullable"]
            }
        
        return {
            "label": label,
            "properties": properties,
            "constraints": [
                {"property": pk, "type": "UNIQUE"}
                for pk in schema["primary_key_candidates"][:1]  # First PK only
            ]
        }
    
    def generate_json_schema(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Generate JSON Schema (for object storage)."""
        properties = {}
        required = []
        
        for field in schema["fields"]:
            json_type = self._pandas_to_json_schema_type(field["type"])
            properties[field["name"]] = {
                "type": json_type,
                "description": f"Field: {field['name']}"
            }
            if not field["nullable"]:
                required.append(field["name"])
        
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": required,
            "properties": properties
        }
    
    def _pandas_to_postgresql_type(self, pandas_type: str) -> str:
        """Map pandas type to PostgreSQL type."""
        mapping = {
            "integer": "INTEGER",
            "decimal": "DECIMAL(18, 2)",
            "string": "TEXT",
            "date": "TIMESTAMP",
            "boolean": "BOOLEAN"
        }
        return mapping.get(pandas_type, "TEXT")
    
    def _pandas_to_mongodb_type(self, pandas_type: str) -> str:
        """Map pandas type to MongoDB BSON type."""
        mapping = {
            "integer": "int",
            "decimal": "double",
            "string": "string",
            "date": "date",
            "boolean": "bool"
        }
        return mapping.get(pandas_type, "string")
    
    def _pandas_to_neo4j_type(self, pandas_type: str) -> str:
        """Map pandas type to Neo4j type."""
        mapping = {
            "integer": "Integer",
            "decimal": "Float",
            "string": "String",
            "date": "DateTime",
            "boolean": "Boolean"
        }
        return mapping.get(pandas_type, "String")
    
    def _pandas_to_json_schema_type(self, pandas_type: str) -> str:
        """Map pandas type to JSON Schema type."""
        mapping = {
            "integer": "integer",
            "decimal": "number",
            "string": "string",
            "date": "string",  # JSON Schema uses string for dates
            "boolean": "boolean"
        }
        return mapping.get(pandas_type, "string")


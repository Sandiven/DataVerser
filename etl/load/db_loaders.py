"""
Database loaders for multiple database types.

Supports PostgreSQL, MongoDB, and Neo4j.
"""

import logging
import pandas as pd
from typing import Dict, Any, Optional, List
import json

logger = logging.getLogger(__name__)


class DatabaseLoader:
    """Base class for database loaders."""
    
    def __init__(self, connection_config: Dict[str, Any]):
        """
        Initialize database loader.
        
        Args:
            connection_config: Database connection parameters
        """
        self.connection_config = connection_config
        self.connection = None
    
    def connect(self):
        """Establish database connection."""
        raise NotImplementedError
    
    def load_data(self, df: pd.DataFrame, table_name: str, schema: Dict[str, Any]):
        """Load DataFrame into database."""
        raise NotImplementedError
    
    def query(self, query: str) -> List[Dict[str, Any]]:
        """Execute query and return results."""
        raise NotImplementedError
    
    def close(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()


class PostgreSQLLoader(DatabaseLoader):
    """PostgreSQL database loader."""
    
    def __init__(self, connection_config: Dict[str, Any]):
        super().__init__(connection_config)
        self.connection = None
    
    def connect(self):
        """Connect to PostgreSQL."""
        try:
            import psycopg2
            self.connection = psycopg2.connect(
                host=self.connection_config.get("host", "localhost"),
                port=self.connection_config.get("port", 5432),
                database=self.connection_config.get("database", "etl_db"),
                user=self.connection_config.get("user", "postgres"),
                password=self.connection_config.get("password", "")
            )
            logger.info("Connected to PostgreSQL")
        except ImportError:
            logger.warning("psycopg2 not installed. PostgreSQL loading disabled.")
            self.connection = None
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            self.connection = None
    
    def load_data(self, df: pd.DataFrame, table_name: str, schema: Dict[str, Any]):
        """Load DataFrame into PostgreSQL table."""
        if not self.connection:
            self.connect()
        
        if not self.connection:
            logger.error("No PostgreSQL connection available")
            return
        
        try:
            from sqlalchemy import create_engine
            connection_string = (
                f"postgresql://{self.connection_config.get('user', 'postgres')}:"
                f"{self.connection_config.get('password', '')}@"
                f"{self.connection_config.get('host', 'localhost')}:"
                f"{self.connection_config.get('port', 5432)}/"
                f"{self.connection_config.get('database', 'etl_db')}"
            )
            engine = create_engine(connection_string)
            df.to_sql(table_name, engine, if_exists='append', index=False)
            logger.info(f"Loaded {len(df)} rows into PostgreSQL table {table_name}")
        except Exception as e:
            logger.error(f"Failed to load data into PostgreSQL: {e}")
    
    def query(self, query: str) -> List[Dict[str, Any]]:
        """Execute SQL query."""
        if not self.connection:
            self.connect()
        
        if not self.connection:
            return []
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            cursor.close()
            return results
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return []


class MongoLoader(DatabaseLoader):
    """MongoDB database loader."""
    
    def __init__(self, connection_config: Dict[str, Any]):
        super().__init__(connection_config)
        self.client = None
        self.db = None
    
    def connect(self):
        """Connect to MongoDB."""
        try:
            from pymongo import MongoClient
            
            # Check if full connection string is provided (e.g., MongoDB Atlas)
            connection_string = self.connection_config.get('connection_string', '')
            
            if connection_string:
                # Use full connection string (supports mongodb:// and mongodb+srv://)
                self.client = MongoClient(connection_string)
                
                # Extract database name from connection string or use config
                database = self.connection_config.get("database", "etl_db")
                
                # Try to extract database name from connection string
                # Format: mongodb+srv://user:pass@cluster.net/dbname?options
                if "/" in connection_string:
                    # Split by / to get parts after the host
                    parts = connection_string.split("/")
                    if len(parts) >= 4:  # mongodb+srv://user:pass@host/dbname?options
                        db_part = parts[3].split("?")[0]  # Remove query parameters
                        if db_part and db_part.strip():
                            database = db_part.strip()
                            logger.info(f"Extracted database name from connection string: {database}")
                    elif len(parts) == 3:  # mongodb://host:port/dbname?options
                        db_part = parts[2].split("?")[0]
                        if db_part and db_part.strip():
                            database = db_part.strip()
                            logger.info(f"Extracted database name from connection string: {database}")
                
                logger.info(f"Using database: {database}")
                self.db = self.client[database]
            else:
                # Build connection string from individual components
                user = self.connection_config.get('user', '')
                password = self.connection_config.get('password', '')
                host = self.connection_config.get('host', 'localhost')
                port = self.connection_config.get('port', 27017)
                
                if user and password:
                    connection_string = f"mongodb://{user}:{password}@{host}:{port}/"
                else:
                    connection_string = f"mongodb://{host}:{port}/"
                
                self.client = MongoClient(connection_string)
                self.db = self.client[self.connection_config.get("database", "etl_db")]
            
            logger.info("Connected to MongoDB")
        except ImportError:
            logger.warning("pymongo not installed. MongoDB loading disabled.")
            self.client = None
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            self.client = None
    
    def load_data(self, df: pd.DataFrame, collection_name: str, schema: Dict[str, Any]):
        """Load DataFrame into MongoDB collection."""
        if not self.client:
            self.connect()
        
        if not self.client:
            logger.error("No MongoDB connection available")
            return
        
        if df.empty:
            logger.warning(f"DataFrame is empty, nothing to load into MongoDB")
            return
        
        try:
            collection = self.db[collection_name]
            records = df.to_dict('records')
            
            # Convert NaN to None for MongoDB
            for record in records:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
            
            # Insert documents
            result = collection.insert_many(records)
            logger.info(f"✅ Successfully loaded {len(result.inserted_ids)} documents into MongoDB")
            logger.info(f"   Database: {self.db.name}")
            logger.info(f"   Collection: {collection_name}")
            logger.info(f"   Inserted IDs: {len(result.inserted_ids)}")
            
            # Verify insertion
            count = collection.count_documents({})
            logger.info(f"   Total documents in collection: {count}")
            
        except Exception as e:
            logger.error(f"❌ Failed to load data into MongoDB: {e}")
            logger.error(f"   Database: {self.db.name if self.db else 'Unknown'}")
            logger.error(f"   Collection: {collection_name}")
            import traceback
            logger.error(traceback.format_exc())
            raise  # Re-raise to see the error
    
    def query(self, query: str) -> List[Dict[str, Any]]:
        """Execute MongoDB query (expects JSON string)."""
        if not self.client:
            self.connect()
        
        if not self.client:
            return []
        
        try:
            query_dict = json.loads(query) if isinstance(query, str) else query
            collection_name = query_dict.get("collection", "data_collection")
            filter_dict = query_dict.get("filter", {})
            projection = query_dict.get("projection", {})
            
            collection = self.db[collection_name]
            results = list(collection.find(filter_dict, projection))
            
            # Convert ObjectId to string for JSON serialization
            for result in results:
                if "_id" in result:
                    result["_id"] = str(result["_id"])
            
            return results
        except Exception as e:
            logger.error(f"MongoDB query execution failed: {e}")
            return []


class Neo4jLoader(DatabaseLoader):
    """Neo4j graph database loader."""
    
    def __init__(self, connection_config: Dict[str, Any]):
        super().__init__(connection_config)
        self.driver = None
    
    def connect(self):
        """Connect to Neo4j."""
        try:
            from neo4j import GraphDatabase
            uri = f"bolt://{self.connection_config.get('host', 'localhost')}:{self.connection_config.get('port', 7687)}"
            self.driver = GraphDatabase.driver(
                uri,
                auth=(
                    self.connection_config.get("user", "neo4j"),
                    self.connection_config.get("password", "")
                )
            )
            logger.info("Connected to Neo4j")
        except ImportError:
            logger.warning("neo4j driver not installed. Neo4j loading disabled.")
            self.driver = None
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {e}")
            self.driver = None
    
    def load_data(self, df: pd.DataFrame, label: str, schema: Dict[str, Any]):
        """Load DataFrame as Neo4j nodes."""
        if not self.driver:
            self.connect()
        
        if not self.driver:
            logger.error("No Neo4j connection available")
            return
        
        try:
            with self.driver.session() as session:
                for _, row in df.iterrows():
                    props = {}
                    for col in df.columns:
                        value = row[col]
                        if pd.notna(value):
                            props[col] = value
                    
                    query = f"CREATE (n:{label} $props)"
                    session.run(query, props=props)
            
            logger.info(f"Loaded {len(df)} nodes into Neo4j with label {label}")
        except Exception as e:
            logger.error(f"Failed to load data into Neo4j: {e}")
    
    def query(self, query: str) -> List[Dict[str, Any]]:
        """Execute Cypher query."""
        if not self.driver:
            self.connect()
        
        if not self.driver:
            return []
        
        try:
            with self.driver.session() as session:
                result = session.run(query)
                records = []
                for record in result:
                    records.append(dict(record))
                return records
        except Exception as e:
            logger.error(f"Neo4j query execution failed: {e}")
            return []
    
    def close(self):
        """Close Neo4j driver."""
        if self.driver:
            self.driver.close()


def get_loader(db_type: str, connection_config: Dict[str, Any]) -> DatabaseLoader:
    """
    Factory function to get appropriate database loader.
    
    Args:
        db_type: Database type ('postgresql', 'mongodb', 'neo4j')
        connection_config: Connection configuration
    
    Returns:
        DatabaseLoader instance
    """
    if db_type == "postgresql":
        return PostgreSQLLoader(connection_config)
    elif db_type == "mongodb":
        return MongoLoader(connection_config)
    elif db_type == "neo4j":
        return Neo4jLoader(connection_config)
    else:
        raise ValueError(f"Unsupported database type: {db_type}")


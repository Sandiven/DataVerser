"""
Schema evolution tracking and migration management.

Handles schema versioning, change detection, and backward compatibility.
"""

import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
import json

logger = logging.getLogger(__name__)


class SchemaEvolution:
    """Manages schema evolution and history."""
    
    def __init__(self, storage_path: str = "schemas"):
        """
        Initialize schema evolution tracker.
        
        Args:
            storage_path: Path to store schema history files
        """
        import os
        self.storage_path = storage_path
        os.makedirs(storage_path, exist_ok=True)
        self.schemas: Dict[str, List[Dict[str, Any]]] = {}  # source_id -> list of schemas
    
    def add_schema(self, source_id: str, schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add a new schema version and detect changes.
        
        Args:
            source_id: Source identifier
            schema: New schema dictionary
        
        Returns:
            Schema with migration notes added
        """
        if source_id not in self.schemas:
            self.schemas[source_id] = []
        
        previous_schema = self.schemas[source_id][-1] if self.schemas[source_id] else None
        
        # Detect changes
        if previous_schema:
            changes = self._detect_changes(previous_schema, schema)
            schema["migration_notes"] = self._generate_migration_notes(changes)
            schema["previous_schema_id"] = previous_schema["schema_id"]
        else:
            schema["migration_notes"] = "Initial schema creation"
            schema["previous_schema_id"] = None
        
        # Add version number
        schema["version"] = len(self.schemas[source_id]) + 1
        
        # Store schema
        self.schemas[source_id].append(schema)
        
        # Persist to disk
        self._save_schema_history(source_id)
        
        return schema
    
    def get_schema(self, source_id: str, version: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """
        Get schema for source_id, optionally by version.
        
        Args:
            source_id: Source identifier
            version: Schema version (None for latest)
        
        Returns:
            Schema dictionary or None
        """
        if source_id not in self.schemas:
            self._load_schema_history(source_id)
        
        if not self.schemas.get(source_id):
            return None
        
        if version is None:
            return self.schemas[source_id][-1]
        
        if 1 <= version <= len(self.schemas[source_id]):
            return self.schemas[source_id][version - 1]
        
        return None
    
    def get_schema_history(self, source_id: str) -> List[Dict[str, Any]]:
        """
        Get full schema history for source_id.
        
        Args:
            source_id: Source identifier
        
        Returns:
            List of schema dictionaries with diffs
        """
        if source_id not in self.schemas:
            self._load_schema_history(source_id)
        
        schemas = self.schemas.get(source_id, [])
        
        # Add diffs between versions
        result = []
        for i, schema in enumerate(schemas):
            schema_copy = schema.copy()
            if i > 0:
                prev_schema = schemas[i - 1]
                changes = self._detect_changes(prev_schema, schema)
                schema_copy["changes"] = changes
            else:
                schema_copy["changes"] = {"added": schema["fields"], "removed": [], "modified": []}
            result.append(schema_copy)
        
        return result
    
    def _field_similarity(self, field1_name: str, field2_name: str) -> float:
        """
        Calculate similarity score between two field names.
        Returns float between 0 and 1.
        """
        name1 = field1_name.lower()
        name2 = field2_name.lower()
        
        # Exact match
        if name1 == name2:
            return 1.0
        
        # Check if one contains the other (e.g., "price_usd" vs "price")
        if name1 in name2 or name2 in name1:
            return 0.8
        
        # Check for common suffixes/prefixes removed
        # e.g., "price_usd" -> "price", "usd_price" -> "price"
        name1_parts = set(name1.split('_'))
        name2_parts = set(name2.split('_'))
        
        if name1_parts.intersection(name2_parts):
            intersection = len(name1_parts.intersection(name2_parts))
            union = len(name1_parts.union(name2_parts))
            return intersection / union if union > 0 else 0.0
        
        # Check character-level similarity (simple Jaccard)
        set1 = set(name1)
        set2 = set(name2)
        intersection = len(set1.intersection(set2))
        union = len(set1.union(set2))
        return intersection / union if union > 0 else 0.0
    
    def _detect_semantic_renames(
        self,
        removed_fields: List[Dict[str, Any]],
        added_fields: List[Dict[str, Any]],
        threshold: float = 0.6
    ) -> List[Dict[str, Any]]:
        """
        Detect potential semantic renames by comparing removed and added fields.
        Returns list of rename mappings.
        """
        renames = []
        used_added = set()
        
        for removed_field in removed_fields:
            best_match = None
            best_score = 0.0
            removed_name = removed_field["name"]
            
            for added_field in added_fields:
                added_name = added_field["name"]
                if added_name in used_added:
                    continue
                
                # Check type compatibility
                if removed_field.get("type") != added_field.get("type"):
                    continue
                
                score = self._field_similarity(removed_name, added_name)
                if score > best_score and score >= threshold:
                    best_score = score
                    best_match = added_field
            
            if best_match:
                renames.append({
                    "old_field": removed_field,
                    "new_field": best_match,
                    "confidence": best_score
                })
                used_added.add(best_match["name"])
        
        return renames
    
    def _detect_changes(
        self,
        old_schema: Dict[str, Any],
        new_schema: Dict[str, Any]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Detect changes between two schemas, including semantic renames.
        
        Returns:
            Dict with 'added', 'removed', 'modified', 'renamed' lists
        """
        old_fields = {f["name"]: f for f in old_schema.get("fields", [])}
        new_fields = {f["name"]: f for f in new_schema.get("fields", [])}
        
        added = [new_fields[name] for name in new_fields if name not in old_fields]
        removed = [old_fields[name] for name in old_fields if name not in new_fields]
        
        modified = []
        for name in old_fields:
            if name in new_fields:
                old_field = old_fields[name]
                new_field = new_fields[name]
                if old_field["type"] != new_field["type"] or old_field["nullable"] != new_field["nullable"]:
                    modified.append({
                        "field": name,
                        "old": old_field,
                        "new": new_field
                    })
        
        # Detect semantic renames
        renames = self._detect_semantic_renames(removed, added)
        
        # Remove renamed fields from added/removed lists
        renamed_old_names = {r["old_field"]["name"] for r in renames}
        renamed_new_names = {r["new_field"]["name"] for r in renames}
        
        added = [f for f in added if f["name"] not in renamed_new_names]
        removed = [f for f in removed if f["name"] not in renamed_old_names]
        
        return {
            "added": added,
            "removed": removed,
            "modified": modified,
            "renamed": renames
        }
    
    def _generate_migration_notes(self, changes: Dict[str, List[Dict[str, Any]]]) -> str:
        """Generate human-readable migration notes."""
        notes = []
        
        if changes.get("renamed"):
            renames = []
            for r in changes["renamed"]:
                old_name = r["old_field"]["name"]
                new_name = r["new_field"]["name"]
                confidence = r.get("confidence", 0.0)
                renames.append(f"{old_name} -> {new_name} (confidence: {confidence:.2f})")
            notes.append(f"Renamed {len(changes['renamed'])} field(s): {', '.join(renames)}")
        
        if changes.get("added"):
            notes.append(f"Added {len(changes['added'])} field(s): {', '.join(f['name'] for f in changes['added'])}")
        
        if changes.get("removed"):
            notes.append(f"Removed {len(changes['removed'])} field(s): {', '.join(f['name'] for f in changes['removed'])}")
        
        if changes.get("modified"):
            mods = []
            for mod in changes["modified"]:
                old_type = mod["old"]["type"]
                new_type = mod["new"]["type"]
                mods.append(f"{mod['field']} ({old_type} -> {new_type})")
            notes.append(f"Modified {len(changes['modified'])} field(s): {', '.join(mods)}")
        
        if not notes:
            return "No schema changes detected"
        
        return "; ".join(notes)
    
    def _save_schema_history(self, source_id: str):
        """Save schema history to disk."""
        file_path = f"{self.storage_path}/{source_id}_history.json"
        try:
            with open(file_path, 'w') as f:
                json.dump(self.schemas[source_id], f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Failed to save schema history for {source_id}: {e}")
    
    def _load_schema_history(self, source_id: str):
        """Load schema history from disk."""
        file_path = f"{self.storage_path}/{source_id}_history.json"
        try:
            with open(file_path, 'r') as f:
                self.schemas[source_id] = json.load(f)
        except FileNotFoundError:
            self.schemas[source_id] = []
        except Exception as e:
            logger.error(f"Failed to load schema history for {source_id}: {e}")
            self.schemas[source_id] = []
    
    def get_migration_strategy(
        self,
        source_id: str,
        from_version: int,
        to_version: int
    ) -> Dict[str, Any]:
        """
        Generate migration strategy between two schema versions.
        
        Returns:
            Migration plan with SQL/NoSQL migration scripts
        """
        old_schema = self.get_schema(source_id, from_version)
        new_schema = self.get_schema(source_id, to_version)
        
        if not old_schema or not new_schema:
            return {"error": "Invalid schema versions"}
        
        changes = self._detect_changes(old_schema, new_schema)
        
        strategy = {
            "from_version": from_version,
            "to_version": to_version,
            "changes": changes,
            "migration_scripts": {}
        }
        
        # Generate PostgreSQL migration
        strategy["migration_scripts"]["postgresql"] = self._generate_postgresql_migration(
            old_schema, new_schema, changes
        )
        
        # Generate MongoDB migration
        strategy["migration_scripts"]["mongodb"] = self._generate_mongodb_migration(
            old_schema, new_schema, changes
        )
        
        return strategy
    
    def _generate_postgresql_migration(
        self,
        old_schema: Dict[str, Any],
        new_schema: Dict[str, Any],
        changes: Dict[str, List[Dict[str, Any]]]
    ) -> List[str]:
        """Generate PostgreSQL ALTER TABLE statements."""
        statements = []
        table_name = "data_table"  # Would be configurable
        
        for field in changes["added"]:
            pg_type = self._pandas_to_postgresql_type(field["type"])
            nullable = "NULL" if field["nullable"] else "NOT NULL"
            statements.append(
                f"ALTER TABLE {table_name} ADD COLUMN {field['name']} {pg_type} {nullable};"
            )
        
        for field in changes["removed"]:
            statements.append(
                f"ALTER TABLE {table_name} DROP COLUMN {field['name']};"
            )
        
        for mod in changes["modified"]:
            field_name = mod["field"]
            new_type = self._pandas_to_postgresql_type(mod["new"]["type"])
            statements.append(
                f"ALTER TABLE {table_name} ALTER COLUMN {field_name} TYPE {new_type};"
            )
            if mod["old"]["nullable"] != mod["new"]["nullable"]:
                nullable = "DROP NOT NULL" if mod["new"]["nullable"] else "SET NOT NULL"
                statements.append(
                    f"ALTER TABLE {table_name} ALTER COLUMN {field_name} {nullable};"
                )
        
        return statements
    
    def _generate_mongodb_migration(
        self,
        old_schema: Dict[str, Any],
        new_schema: Dict[str, Any],
        changes: Dict[str, List[Dict[str, Any]]]
    ) -> List[str]:
        """Generate MongoDB update operations."""
        operations = []
        collection_name = "data_collection"  # Would be configurable
        
        for field in changes["added"]:
            operations.append(
                f"db.{collection_name}.updateMany({{}}, {{$set: {{'{field['name']}': null}}}});"
            )
        
        for field in changes["removed"]:
            operations.append(
                f"db.{collection_name}.updateMany({{}}, {{$unset: {{'{field['name']}': ''}}}});"
            )
        
        # Type changes in MongoDB are handled at application level
        for mod in changes["modified"]:
            operations.append(
                f"// Field {mod['field']} type changed from {mod['old']['type']} to {mod['new']['type']}"
            )
            operations.append(
                f"// Manual data transformation may be required"
            )
        
        return operations
    
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


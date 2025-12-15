"""
Schema synchronization and management handler
"""
import logging
from typing import Dict, Optional

from db_sync.connectors.base import BaseConnector
from db_sync.models.schema import TableSchema, ColumnDefinition
from db_sync.utils.schema_converter import SchemaConverter

logger = logging.getLogger(__name__)


class SchemaHandler:
    """Manages schema synchronization between source and target databases"""

    def __init__(self, source_connector: BaseConnector, target_connector: BaseConnector,
                 auto_detect_changes: bool = True):
        self.source = source_connector
        self.target = target_connector
        self.auto_detect_changes = auto_detect_changes
        self.schema_cache: Dict[str, TableSchema] = {}
        self.converter = SchemaConverter()

    def get_or_sync_schema(self, table_name: str) -> TableSchema:
        """
        Get schema for a table, fetching from cache or syncing if needed
        
        Args:
            table_name: Name of the table
            
        Returns:
            TableSchema object
        """
        if table_name not in self.schema_cache:
            schema = self.source.get_table_schema(table_name)
            self.schema_cache[table_name] = schema
            
            # Sync schema to target if it doesn't exist
            if not self.target.table_exists(table_name):
                self.sync_table_schema(table_name)
        
        return self.schema_cache[table_name]

    def sync_table_schema(self, table_name: str) -> None:
        """
        Synchronize table schema from source to target
        
        Args:
            table_name: Name of the table to sync
        """
        logger.info(f"Syncing schema for table: {table_name}")
        
        # Get source schema
        source_schema = self.source.get_table_schema(table_name)
        
        # Convert schema if needed (for different database types)
        target_schema = self.converter.convert_schema(
            source_schema,
            self.source.config['type'],
            self.target.config['type']
        )
        
        # Create table in target if it doesn't exist
        if not self.target.table_exists(table_name):
            self.target.create_table(target_schema)
            logger.info(f"Created table {table_name} in target database")
        else:
            # Check for schema differences and apply changes
            if self.auto_detect_changes:
                self._sync_schema_changes(table_name, target_schema)
        
        # Update cache
        self.schema_cache[table_name] = source_schema

    def _sync_schema_changes(self, table_name: str, new_schema: TableSchema) -> None:
        """
        Detect and apply schema changes to existing table
        
        Args:
            table_name: Name of the table
            new_schema: New schema definition
        """
        try:
            current_schema = self.target.get_table_schema(table_name)
            current_columns = {col.name: col for col in current_schema.columns}
            new_columns = {col.name: col for col in new_schema.columns}
            
            # Find new columns
            for col_name, col_def in new_columns.items():
                if col_name not in current_columns:
                    logger.info(f"Adding new column {col_name} to table {table_name}")
                    self.target.alter_table_add_column(table_name, col_def)
            
            # Note: Column deletions and type changes are not automatically handled
            # as they can be destructive operations
            
        except Exception as e:
            logger.error(f"Error syncing schema changes for {table_name}: {e}")

    def sync_all_schemas(self) -> None:
        """Synchronize schemas for all tables from source to target"""
        logger.info("Starting full schema synchronization")
        
        tables = self.source.get_all_tables()
        logger.info(f"Found {len(tables)} tables to sync")
        
        for table_name in tables:
            try:
                self.sync_table_schema(table_name)
            except Exception as e:
                logger.error(f"Failed to sync schema for {table_name}: {e}")
                raise

    def validate_schema(self, table_name: str) -> bool:
        """
        Validate that table schema matches between source and target
        
        Args:
            table_name: Name of the table to validate
            
        Returns:
            True if schemas match, False otherwise
        """
        try:
            source_schema = self.source.get_table_schema(table_name)
            target_schema = self.target.get_table_schema(table_name)
            
            source_cols = {col.name for col in source_schema.columns}
            target_cols = {col.name for col in target_schema.columns}
            
            if source_cols != target_cols:
                logger.warning(f"Schema mismatch for {table_name}")
                logger.warning(f"Missing in target: {source_cols - target_cols}")
                logger.warning(f"Extra in target: {target_cols - source_cols}")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating schema for {table_name}: {e}")
            return False

    def clear_cache(self) -> None:
        """Clear the schema cache"""
        self.schema_cache.clear()
        logger.debug("Schema cache cleared")
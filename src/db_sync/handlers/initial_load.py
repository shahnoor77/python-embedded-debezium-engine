"""
Initial data load handler for full table synchronization
"""
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Set

from db_sync.connectors.base import BaseConnector
from db_sync.handlers.schema_handler import SchemaHandler
from db_sync.core.config import InitialLoadConfig

logger = logging.getLogger(__name__)


class InitialLoadHandler:
    """Handles initial full data load from source to target"""

    def __init__(self, source_connector: BaseConnector, target_connector: BaseConnector,
                 schema_handler: SchemaHandler, config: InitialLoadConfig):
        self.source = source_connector
        self.target = target_connector
        self.schema_handler = schema_handler
        self.config = config
        self.completed_tables: Set[str] = set()
        self.failed_tables: Set[str] = set()

    def perform_initial_load(self) -> None:
        """
        Perform initial full data load for all tables
        """
        logger.info("Starting initial data load")
        
        # Get list of tables to sync
        all_tables = self.source.get_all_tables()
        tables_to_sync = self._filter_tables(all_tables)
        
        logger.info(f"Found {len(tables_to_sync)} tables to load")
        
        if not tables_to_sync:
            logger.warning("No tables to load")
            return
        
        # First, sync all schemas
        logger.info("Syncing table schemas...")
        for table_name in tables_to_sync:
            try:
                self.schema_handler.sync_table_schema(table_name)
            except Exception as e:
                logger.error(f"Failed to sync schema for {table_name}: {e}")
                self.failed_tables.add(table_name)
        
        # Remove failed tables from sync list
        tables_to_sync = [t for t in tables_to_sync if t not in self.failed_tables]
        
        # Load data in parallel
        if self.config.parallel_tables > 1:
            self._load_tables_parallel(tables_to_sync)
        else:
            self._load_tables_sequential(tables_to_sync)
        
        # Report results
        logger.info(f"Initial load completed: {len(self.completed_tables)} succeeded, "
                    f"{len(self.failed_tables)} failed")
        
        if self.failed_tables:
            logger.error(f"Failed tables: {', '.join(self.failed_tables)}")

    def _filter_tables(self, all_tables: List[str]) -> List[str]:
        """
        Filter tables based on include/exclude lists
        
        Args:
            all_tables: List of all table names
            
        Returns:
            Filtered list of table names
        """
        filtered = all_tables
        
        # Apply include filter
        if self.config.include_tables:
            filtered = [t for t in filtered if t in self.config.include_tables]
        
        # Apply exclude filter
        if self.config.exclude_tables:
            filtered = [t for t in filtered if t not in self.config.exclude_tables]
        
        return filtered

    def _load_tables_sequential(self, tables: List[str]) -> None:
        """
        Load tables sequentially
        
        Args:
            tables: List of table names to load
        """
        for table_name in tables:
            self._load_table(table_name)

    def _load_tables_parallel(self, tables: List[str]) -> None:
        """
        Load tables in parallel
        
        Args:
            tables: List of table names to load
        """
        with ThreadPoolExecutor(max_workers=self.config.parallel_tables) as executor:
            future_to_table = {
                executor.submit(self._load_table, table): table
                for table in tables
            }
            
            for future in as_completed(future_to_table):
                table_name = future_to_table[future]
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error loading table {table_name}: {e}")

    def _load_table(self, table_name: str) -> None:
        """
        Load data for a single table. **Uses an isolated connection for thread safety.**
        
        Args:
            table_name: Name of the table to load
        """
        target_conn = None
        try:
            logger.info(f"Loading table: {table_name}")
            
            # Get row count (uses the shared self.source connection, which is fine for reads)
            total_rows = self.source.get_row_count(table_name)
            logger.info(f"Table {table_name} has {total_rows} rows")
            
            if total_rows == 0:
                logger.info(f"Table {table_name} is empty, skipping data load")
                self.completed_tables.add(table_name)
                return
            
            # --- START: Obtain isolated connection for this thread ---
            # self.target.connect() must return a new, isolated connector instance.
            target_conn = self.target.connect() 
            # --- END  ---

            # Load data in batches
            rows_loaded = 0
            for batch in self.source.fetch_all_rows(table_name, self.config.batch_size):
                try:
                    # Use the isolated connection for transactions
                    target_conn.begin_transaction()
                    inserted = target_conn.insert_batch(table_name, batch)
                    target_conn.commit_transaction()
                    
                    rows_loaded += inserted
                    
                    if rows_loaded % 10000 == 0:
                        progress = (rows_loaded / total_rows) * 100
                        logger.info(f"Progress for {table_name}: {rows_loaded}/{total_rows} "
                                    f"({progress:.1f}%)")
                
                except Exception as e:
                    logger.error(f"Error inserting batch for {table_name}: {e}")
                    target_conn.rollback_transaction()
                    raise
            
            logger.info(f"Successfully loaded {rows_loaded} rows into {table_name}")
            self.completed_tables.add(table_name)
            
        except Exception as e:
            logger.error(f"Failed to load table {table_name}: {e}")
            self.failed_tables.add(table_name)
            raise
            
        finally:
            # --- Ensure the isolated connection is closed ---
            if target_conn:
                target_conn.disconnect()
            


    def is_initial_load_needed(self) -> bool:
        """
        Check if initial load is needed
        
        Returns:
            True if initial load should be performed
        """
        if not self.config.enabled:
            return False
        
        # Check if any source tables don't exist in target
        source_tables = set(self.source.get_all_tables())
        target_tables = set(self.target.get_all_tables())
        
        missing_tables = source_tables - target_tables
        
        if missing_tables:
            logger.info(f"Found {len(missing_tables)} tables missing in target")
            return True
        
        # Check if any existing tables are empty in target
        for table_name in source_tables:
            if table_name in target_tables:
                target_count = self.target.get_row_count(table_name)
                if target_count == 0:
                    source_count = self.source.get_row_count(table_name)
                    if source_count > 0:
                        logger.info(f"Table {table_name} is empty in target but has data in source")
                        return True
        
        logger.info("Initial load not needed, all tables exist and have data")
        return False
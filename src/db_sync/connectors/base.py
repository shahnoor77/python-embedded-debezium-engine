"""
Base connector interface for database operations
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

from db_sync.models.schema import TableSchema, ColumnDefinition


class BaseConnector(ABC):
    """Abstract base class for database connectors"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = None

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the database"""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close database connection"""
        pass

    @abstractmethod
    def get_all_tables(self) -> List[str]:
        """
        Retrieve list of all tables in the database
        
        Returns:
            List of table names
        """
        pass

    @abstractmethod
    def get_table_schema(self, table_name: str) -> TableSchema:
        """
        Get schema information for a specific table
        
        Args:
            table_name: Name of the table
            
        Returns:
            TableSchema object containing table metadata
        """
        pass

    @abstractmethod
    def get_primary_keys(self, table_name: str) -> List[str]:
        """
        Get primary key columns for a table
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of primary key column names
        """
        pass

    @abstractmethod
    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database
        
        Args:
            table_name: Name of the table
            
        Returns:
            True if table exists, False otherwise
        """
        pass

    @abstractmethod
    def create_table(self, schema: TableSchema) -> None:
        """
        Create a new table based on schema definition
        
        Args:
            schema: TableSchema object defining the table structure
        """
        pass

    @abstractmethod
    def alter_table_add_column(self, table_name: str, column: ColumnDefinition) -> None:
        """
        Add a new column to an existing table
        
        Args:
            table_name: Name of the table
            column: Column definition
        """
        pass

    @abstractmethod
    def insert_batch(self, table_name: str, rows: List[Dict[str, Any]]) -> int:
        """
        Insert multiple rows into a table
        
        Args:
            table_name: Name of the table
            rows: List of row dictionaries
            
        Returns:
            Number of rows inserted
        """
        pass

    @abstractmethod
    def update_row(self, table_name: str, primary_keys: Dict[str, Any], 
                   values: Dict[str, Any]) -> None:
        """
        Update a single row in a table
        
        Args:
            table_name: Name of the table
            primary_keys: Dictionary of primary key column names and values
            values: Dictionary of column names and new values
        """
        pass

    @abstractmethod
    def delete_row(self, table_name: str, primary_keys: Dict[str, Any]) -> None:
        """
        Delete a single row from a table
        
        Args:
            table_name: Name of the table
            primary_keys: Dictionary of primary key column names and values
        """
        pass

    @abstractmethod
    def fetch_all_rows(self, table_name: str, batch_size: int = 1000) -> List[Dict[str, Any]]:
        """
        Fetch all rows from a table in batches
        
        Args:
            table_name: Name of the table
            batch_size: Number of rows per batch
            
        Yields:
            Batches of rows as dictionaries
        """
        pass

    @abstractmethod
    def get_row_count(self, table_name: str) -> int:
        """
        Get the total number of rows in a table
        
        Args:
            table_name: Name of the table
            
        Returns:
            Row count
        """
        pass

    @abstractmethod
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> Any:
        """
        Execute a raw SQL query
        
        Args:
            query: SQL query string
            params: Optional query parameters
            
        Returns:
            Query result
        """
        pass

    @abstractmethod
    def begin_transaction(self) -> None:
        """Begin a database transaction"""
        pass

    @abstractmethod
    def commit_transaction(self) -> None:
        """Commit the current transaction"""
        pass

    @abstractmethod
    def rollback_transaction(self) -> None:
        """Rollback the current transaction"""
        pass

    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        if exc_type is not None:
            self.rollback_transaction()
        self.disconnect()
        return False
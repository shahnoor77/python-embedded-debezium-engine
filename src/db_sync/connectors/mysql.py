"""
MySQL connector implementation
"""
import logging
from typing import Any, Dict, List, Optional, Tuple

import mysql.connector
from mysql.connector import Error as MySQLError

from db_sync.connectors.base import BaseConnector
from db_sync.models.schema import ColumnDefinition, TableSchema

logger = logging.getLogger(__name__)


class MySQLConnector(BaseConnector):
    """MySQL database connector"""

    # NOTE: TYPE_MAPPING is generally unused in this class, but kept for context/completeness.
    TYPE_MAPPING = {
        'BIGINT': 'BIGINT', 'INTEGER': 'INT', 'INT': 'INT', 'SMALLINT': 'SMALLINT',
        'TINYINT': 'TINYINT', 'DECIMAL': 'DECIMAL', 'NUMERIC': 'DECIMAL',
        'FLOAT': 'FLOAT', 'DOUBLE': 'DOUBLE', 'REAL': 'FLOAT',
        'VARCHAR': 'VARCHAR', 'CHAR': 'CHAR', 'TEXT': 'TEXT',
        'LONGTEXT': 'LONGTEXT', 'MEDIUMTEXT': 'MEDIUMTEXT', 'BOOLEAN': 'TINYINT(1)',
        'DATE': 'DATE', 'DATETIME': 'DATETIME', 'TIMESTAMP': 'TIMESTAMP',
        'TIME': 'TIME', 'JSON': 'JSON', 'BLOB': 'BLOB', 
        'BINARY': 'BINARY', 'VARBINARY': 'VARBINARY',
    }

    def connect(self) -> 'MySQLConnector': # <-- Changed to return a new instance
        """
        Establish connection to MySQL. 
        Returns a new connector instance with an isolated connection for thread safety.
        """
        try:
            # 1. Create a NEW connection every time this is called
            connection = mysql.connector.connect(
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                user=self.config['username'],
                password=self.config['password'],
                autocommit=False
            )
            
            # 2. Create a TEMPORARY connector instance with the new connection
            temp_connector = MySQLConnector(self.config)
            temp_connector.connection = connection
            if self.connection is None:
                self.connection = connection
            logger.info(f"Connected to MySQL at {self.config['host']}:{self.config['port']}")
            return temp_connector # <-- RETURN THE TEMPORARY, ISOLATED CONNECTOR INSTANCE
            
        except MySQLError as e:
            logger.error(f"Failed to connect to MySQL: {e}")
            raise

    def disconnect(self) -> None:
        """Close MySQL connection"""
        if hasattr(self, 'connection') and self.connection:
            self.connection.close()
            # Deliberately do not clear self.connection to avoid race conditions on the main object
            logger.info("Disconnected from MySQL")

    def get_all_tables(self) -> List[str]:
        """Retrieve all tables from the database"""
        query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, (self.config['database'],))
            return [row[0] for row in cursor.fetchall()]
        finally:
            cursor.close()

    def get_table_schema(self, table_name: str) -> TableSchema:
        """Get schema information for a table"""
        query = """
            SELECT 
                column_name,
                data_type,
                column_type,
                is_nullable,
                column_default,
                column_key,
                extra
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        
        cursor = self.connection.cursor(dictionary=True)
        try:
            cursor.execute(query, (self.config['database'], table_name))
            columns = []
            primary_keys = []
            
            for row in cursor.fetchall():
                # Use column_type for full type info (e.g., VARCHAR(255))
                data_type = row['column_type'].upper()
                
                column = ColumnDefinition(
                    name=row['column_name'],
                    data_type=data_type,
                    nullable=row['is_nullable'] == 'YES',
                    default=row['column_default']
                )
                
                if row['column_key'] == 'PRI':
                    primary_keys.append(row['column_name'])
                    column.is_primary_key = True
                
                columns.append(column)
            
            return TableSchema(
                name=table_name,
                columns=columns,
                primary_keys=primary_keys
            )
        finally:
            cursor.close()

    def get_primary_keys(self, table_name: str) -> List[str]:
        """Get primary key columns for a table"""
        query = """
            SELECT column_name
            FROM information_schema.key_column_usage
            WHERE table_schema = %s
            AND table_name = %s
            AND constraint_name = 'PRIMARY'
            ORDER BY ordinal_position
        """
        
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, (self.config['database'], table_name))
            return [row[0] for row in cursor.fetchall()]
        finally:
            cursor.close()

    def table_exists(self, table_name: str) -> bool:
        """Check if table exists"""
        query = """
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = %s 
            AND table_name = %s
        """
        
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, (self.config['database'], table_name))
            return cursor.fetchone()[0] > 0
        finally:
            cursor.close()

    def create_table(self, schema: TableSchema) -> None:
        """
        Create a new table based on schema definition (Fix 1: Add AUTO_INCREMENT)
        """
        columns_sql = []
        for col in schema.columns:
            col_def = f"`{col.name}` {col.data_type}"
            
            # Check if column is a primary key and an integer type for AUTO_INCREMENT
            is_autoincrement = col.is_primary_key and col.data_type.upper().split('(')[0] in ('INT', 'BIGINT', 'SMALLINT')

            if is_autoincrement:
                # Must be NOT NULL and AUTO_INCREMENT
                col_def += " NOT NULL AUTO_INCREMENT"
                # Do NOT add a default
            else:
                if not col.nullable:
                    col_def += " NOT NULL"
                if col.default is not None:
                    # Note: schema_converter should have stripped nextval from Postgres columns
                    col_def += f" DEFAULT {col.default}"
                
            columns_sql.append(col_def)
        
        if schema.primary_keys:
            pk_constraint = f"PRIMARY KEY ({', '.join([f'`{pk}`' for pk in schema.primary_keys])})"
            columns_sql.append(pk_constraint)
        
        query = f"""
            CREATE TABLE `{schema.name}` (
                {', '.join(columns_sql)}
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
            logger.info(f"Created table {schema.name}")
        finally:
            cursor.close()

    def alter_table_add_column(self, table_name: str, column: ColumnDefinition) -> None:
        """
        Add a column to an existing table (Fix 2: Ensure abstract method satisfaction)
        """
        col_def = f"{column.data_type}"
        if not column.nullable:
            col_def += " NOT NULL"
        if column.default is not None:
            col_def += f" DEFAULT {column.default}"
        
        query = f"ALTER TABLE `{table_name}` ADD COLUMN `{column.name}` {col_def}"
        
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
            logger.info(f"Added column {column.name} to {table_name}")
        finally:
            cursor.close()

    def insert_batch(self, table_name: str, rows: List[Dict[str, Any]]) -> int:
        """Insert multiple rows"""
        if not rows:
            return 0
        
        columns = list(rows[0].keys())
        placeholders = ', '.join(['%s'] * len(columns))
        columns_sql = ', '.join([f'`{col}`' for col in columns])
        
        # NOTE: Using INSERT IGNORE might be safer for idempotency in CDC, 
        # but for initial load, a straight INSERT is fine and faster.
        query = f"INSERT INTO `{table_name}` ({columns_sql}) VALUES ({placeholders})"
        
        values = [[row.get(col) for col in columns] for row in rows]
        
        cursor = self.connection.cursor()
        try:
            cursor.executemany(query, values)
            # Removed explicit commit here, as transaction is now managed by the handler:
            # self.connection.commit() 
            return cursor.rowcount # Use rowcount as a reliable way to get inserted rows
        finally:
            cursor.close()

    def update_row(self, table_name: str, primary_keys: Dict[str, Any], 
                   values: Dict[str, Any]) -> None:
        """Update a single row"""
        set_clause = ', '.join([f"`{k}` = %s" for k in values.keys()])
        where_clause = ' AND '.join([f"`{k}` = %s" for k in primary_keys.keys()])
        
        query = f"UPDATE `{table_name}` SET {set_clause} WHERE {where_clause}"
        params = list(values.values()) + list(primary_keys.values())
        
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params)
            self.connection.commit()
        finally:
            cursor.close()

    def delete_row(self, table_name: str, primary_keys: Dict[str, Any]) -> None:
        """Delete a single row"""
        where_clause = ' AND '.join([f"`{k}` = %s" for k in primary_keys.keys()])
        query = f"DELETE FROM `{table_name}` WHERE {where_clause}"
        
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, list(primary_keys.values()))
            self.connection.commit()
        finally:
            cursor.close()

    def fetch_all_rows(self, table_name: str, batch_size: int = 1000) -> List[Dict[str, Any]]:
        """
        Fetch all rows in batches (Fix 3: Use generator for expected behavior)
        """
        query = f"SELECT * FROM `{table_name}`"
        
        # Use server-side cursor equivalent (default for large queries in some drivers)
        # MySQL Connector/Python does not expose an explicit server-side cursor API like psycopg2, 
        # so we rely on fetchmany being efficient enough, or require a different driver.
        cursor = self.connection.cursor(dictionary=True)
        try:
            cursor.execute(query)
            
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                yield rows # Use yield to return batches
        finally:
            cursor.close()

    def get_row_count(self, table_name: str) -> int:
        """Get row count"""
        query = f"SELECT COUNT(*) FROM `{table_name}`"
        
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            return cursor.fetchone()[0]
        finally:
            cursor.close()

    def execute_query(self, query: str, params: Optional[Tuple] = None) -> Any:
        """Execute raw query"""
        cursor = self.connection.cursor(dictionary=True)
        try:
            cursor.execute(query, params or ())
            try:
                # Attempt to fetch results only if it was a SELECT query
                return cursor.fetchall()
            except MySQLError:
                return None
        finally:
            cursor.close()

    def begin_transaction(self) -> None:
        """Begin transaction"""
        self.connection.start_transaction()

    def commit_transaction(self) -> None:
        """Commit transaction"""
        self.connection.commit()

    def rollback_transaction(self) -> None:
        """Rollback transaction"""
        self.connection.rollback()
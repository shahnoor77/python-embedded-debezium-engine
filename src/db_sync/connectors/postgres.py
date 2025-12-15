"""
PostgreSQL connector implementation
"""
import logging
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor, execute_batch
import threading
from db_sync.connectors.base import BaseConnector
from db_sync.models.schema import ColumnDefinition, TableSchema

logger = logging.getLogger(__name__)


class PostgreSQLConnector(BaseConnector):
    """PostgreSQL database connector"""

    TYPE_MAPPING = {
        'bigint': 'BIGINT',
        'integer': 'INTEGER',
        'smallint': 'SMALLINT',
        'numeric': 'NUMERIC',
        'real': 'REAL',
        'double precision': 'DOUBLE PRECISION',
        'character varying': 'VARCHAR',
        'character': 'CHAR',
        'text': 'TEXT',
        'boolean': 'BOOLEAN',
        'date': 'DATE',
        'timestamp without time zone': 'TIMESTAMP',
        'timestamp with time zone': 'TIMESTAMPTZ',
        'time without time zone': 'TIME',
        'json': 'JSON',
        'jsonb': 'JSONB',
        'uuid': 'UUID',
        'bytea': 'BYTEA',
    }
    def __init__(self, config:Dict[str,Any]):
        super().__init__(config)
        self.connection = None

    def connect(self) -> 'PostgreSQLConnector':  # <-- CRITICAL FIX: Return type changed to 'PostgreSQLConnector'
        """
        Establish connection to PostgreSQL. 
        Returns a new connector instance with an isolated connection for thread safety.
        """
        try:
            # 1. Create a NEW connection every time this is called
            connection = psycopg2.connect(
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                user=self.config['username'],
                password=self.config['password'],
                sslmode=self.config.get('ssl_mode', 'prefer')
            )
            connection.autocommit = False
            
            # 2. Create a TEMPORARY connector instance with the new connection
            # This allows the InitialLoadHandler to call methods like insert_batch 
            # and commit_transaction on a thread-local object.
            temp_connector = PostgreSQLConnector(self.config)
            temp_connector.connection = connection
            if self.connection is None:
                self.connection = connection
                
            logger.info(f"Connected to PostgreSQL at {self.config['host']}:{self.config['port']}")
            return temp_connector # <-- RETURN THE ISOLATED CONNECTOR INSTANCE
            
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def disconnect(self) -> None:
        """Close PostgreSQL connection"""
        if hasattr(self, 'connection') and self.connection:
            self.connection.close()
            logger.info("Disconnected from PostgreSQL")

    def get_all_tables(self) -> List[str]:
        """Retrieve all tables from the database"""
        schema = self.config.get('schema', 'public')
        query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query, (schema,))
            return [row[0] for row in cursor.fetchall()]

    def get_table_schema(self, table_name: str) -> TableSchema:
        """Get schema information for a table"""
        schema = self.config.get('schema', 'public')
        
        query = """
            SELECT 
                column_name,
                data_type,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (schema, table_name))
            columns = []
            
            for row in cursor.fetchall():
                data_type = row['data_type']
                if row['character_maximum_length']:
                    data_type = f"{data_type}({row['character_maximum_length']})"
                elif row['numeric_precision'] and row['numeric_scale']:
                    data_type = f"{data_type}({row['numeric_precision']},{row['numeric_scale']})"
                
                columns.append(ColumnDefinition(
                    name=row['column_name'],
                    data_type=data_type,
                    nullable=row['is_nullable'] == 'YES',
                    default=row['column_default']
                ))
        
        primary_keys = self.get_primary_keys(table_name)
        
        return TableSchema(
            name=table_name,
            columns=columns,
            primary_keys=primary_keys
        )

    def get_primary_keys(self, table_name: str) -> List[str]:
        """Get primary key columns"""
        schema = self.config.get('schema', 'public')
        
        query = """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass
            AND i.indisprimary
        """
        
        full_table_name = f"{schema}.{table_name}"
        
        with self.connection.cursor() as cursor:
            cursor.execute(query, (full_table_name,))
            return [row[0] for row in cursor.fetchall()]

    def table_exists(self, table_name: str) -> bool:
        """Check if table exists"""
        schema = self.config.get('schema', 'public')
        
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_name = %s
            )
        """
        
        with self.connection.cursor() as cursor:
            cursor.execute(query, (schema, table_name))
            return cursor.fetchone()[0]

    def create_table(self, schema: TableSchema) -> None:
        """Create a new table"""
        schema_name = self.config.get('schema', 'public')
        
        columns_sql = []
        for col in schema.columns:
            col_def = f"{col.name} {col.data_type}"
            if not col.nullable:
                col_def += " NOT NULL"
            if col.default:
                col_def += f" DEFAULT {col.default}"
            columns_sql.append(col_def)
        
        if schema.primary_keys:
            pk_constraint = f"PRIMARY KEY ({', '.join(schema.primary_keys)})"
            columns_sql.append(pk_constraint)
        
        query = f"""
            CREATE TABLE {schema_name}.{schema.name} (
                {', '.join(columns_sql)}
            )
        """
        
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            self.connection.commit()
        
        logger.info(f"Created table {schema_name}.{schema.name}")

    def alter_table_add_column(self, table_name: str, column: ColumnDefinition) -> None:
        """Add a column to an existing table"""
        schema = self.config.get('schema', 'public')
        
        col_def = f"{column.data_type}"
        if not column.nullable:
            col_def += " NOT NULL"
        if column.default:
            col_def += f" DEFAULT {column.default}"
        
        query = f"""
            ALTER TABLE {schema}.{table_name}
            ADD COLUMN {column.name} {col_def}
        """
        
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            self.connection.commit()
        
        logger.info(f"Added column {column.name} to {schema}.{table_name}")

    def insert_batch(self, table_name: str, rows: List[Dict[str, Any]]) -> int:
        """Insert multiple rows"""
        if not rows:
            return 0
        
        schema = self.config.get('schema', 'public')
        columns = list(rows[0].keys())
        
        query = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
            sql.Identifier(schema),
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(columns))
        )
        
        values = [[row[col] for col in columns] for row in rows]
        
        with self.connection.cursor() as cursor:
            execute_batch(cursor, query, values)
            # Removed explicit commit: transaction managed by handler
        
        return len(rows)

    def update_row(self, table_name: str, primary_keys: Dict[str, Any], 
                   values: Dict[str, Any]) -> None:
        """Update a single row"""
        schema = self.config.get('schema', 'public')
        
        set_clause = ', '.join([f"{k} = %s" for k in values.keys()])
        where_clause = ' AND '.join([f"{k} = %s" for k in primary_keys.keys()])
        
        query = f"""
            UPDATE {schema}.{table_name}
            SET {set_clause}
            WHERE {where_clause}
        """
        
        params = list(values.values()) + list(primary_keys.values())
        
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)
            self.connection.commit()

    def delete_row(self, table_name: str, primary_keys: Dict[str, Any]) -> None:
        """Delete a single row"""
        schema = self.config.get('schema', 'public')
        
        where_clause = ' AND '.join([f"{k} = %s" for k in primary_keys.keys()])
        query = f"DELETE FROM {schema}.{table_name} WHERE {where_clause}"
        
        with self.connection.cursor() as cursor:
            cursor.execute(query, list(primary_keys.values()))
            self.connection.commit()

    def fetch_all_rows(self, table_name: str, batch_size: int = 1000) -> List[Dict[str, Any]]:
        """Fetch all rows in batches using a server-side cursor (named cursor)"""
        schema = self.config.get('schema', 'public')
        query = f"SELECT * FROM {schema}.{table_name}"
        
        # Use a unique cursor name to ensure a distinct server-side cursor is created for this thread.
        # This is critical for reading large datasets concurrently.
        unique_cursor_name = f"fetch_cursor_{table_name}_{threading.get_ident()}"
        with self.connection.cursor(cursor_factory=RealDictCursor, name=unique_cursor_name) as cursor:
            cursor.itersize = batch_size
            cursor.execute(query)
            
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                yield [dict(row) for row in rows]

    def get_row_count(self, table_name: str) -> int:
        """Get row count"""
        schema = self.config.get('schema', 'public')
        query = f"SELECT COUNT(*) FROM {schema}.{table_name}"
        
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchone()[0]

    def execute_query(self, query: str, params: Optional[Tuple] = None) -> Any:
        """Execute raw query"""
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            try:
                return cursor.fetchall()
            except psycopg2.ProgrammingError:
                return None

    def begin_transaction(self) -> None:
        """Begin transaction"""
        # PostgreSQL connection is already in transaction mode (autocommit=False)
        # We only need to ensure no prior transaction is pending.
        pass

    def commit_transaction(self) -> None:
        """Commit transaction"""
        self.connection.commit()

    def rollback_transaction(self) -> None:
        """Rollback transaction"""
        self.connection.rollback()
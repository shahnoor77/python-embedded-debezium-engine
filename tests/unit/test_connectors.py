"""
Unit tests for database connectors
"""
import pytest
from unittest.mock import Mock, patch, MagicMock

from db_sync.connectors.factory import ConnectorFactory
from db_sync.connectors.postgres import PostgreSQLConnector
from db_sync.connectors.mysql import MySQLConnector
from db_sync.models.schema import TableSchema, ColumnDefinition


class TestConnectorFactory:
    """Test ConnectorFactory"""
    
    def test_create_postgres_connector(self, sample_postgres_config):
        """Test creating PostgreSQL connector"""
        connector = ConnectorFactory.create_connector(
            'postgresql',
            sample_postgres_config.dict()
        )
        assert isinstance(connector, PostgreSQLConnector)
    
    def test_create_mysql_connector(self, sample_mysql_config):
        """Test creating MySQL connector"""
        connector = ConnectorFactory.create_connector(
            'mysql',
            sample_mysql_config.dict()
        )
        assert isinstance(connector, MySQLConnector)
    
    def test_unsupported_database_type(self):
        """Test unsupported database type raises error"""
        with pytest.raises(ValueError, match="Unsupported database type"):
            ConnectorFactory.create_connector('mongodb', {})
    
    def test_get_supported_types(self):
        """Test getting supported database types"""
        types = ConnectorFactory.get_supported_types()
        assert 'postgresql' in types
        assert 'mysql' in types


class TestPostgreSQLConnector:
    """Test PostgreSQL connector"""
    
    @patch('db_sync.connectors.postgres.psycopg2.connect')
    def test_connect(self, mock_connect, sample_postgres_config):
        """Test connection to PostgreSQL"""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        connector = PostgreSQLConnector(sample_postgres_config.dict())
        connector.connect()
        
        assert connector.connection == mock_conn
        mock_connect.assert_called_once()
    
    @patch('db_sync.connectors.postgres.psycopg2.connect')
    def test_get_all_tables(self, mock_connect, sample_postgres_config):
        """Test getting all tables"""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [('users',), ('orders',)]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        connector = PostgreSQLConnector(sample_postgres_config.dict())
        connector.connect()
        tables = connector.get_all_tables()
        
        assert tables == ['users', 'orders']
    
    @patch('db_sync.connectors.postgres.psycopg2.connect')
    def test_table_exists(self, mock_connect, sample_postgres_config):
        """Test checking if table exists"""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (True,)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        connector = PostgreSQLConnector(sample_postgres_config.dict())
        connector.connect()
        exists = connector.table_exists('users')
        
        assert exists is True


class TestMySQLConnector:
    """Test MySQL connector"""
    
    @patch('db_sync.connectors.mysql.mysql.connector.connect')
    def test_connect(self, mock_connect, sample_mysql_config):
        """Test connection to MySQL"""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        connector = MySQLConnector(sample_mysql_config.dict())
        connector.connect()
        
        assert connector.connection == mock_conn
        mock_connect.assert_called_once()
    
    @patch('db_sync.connectors.mysql.mysql.connector.connect')
    def test_insert_batch(self, mock_connect, sample_mysql_config):
        """Test batch insert"""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        connector = MySQLConnector(sample_mysql_config.dict())
        connector.connect()
        
        rows = [
            {'id': 1, 'name': 'Alice'},
            {'id': 2, 'name': 'Bob'}
        ]
        
        result = connector.insert_batch('users', rows)
        
        assert result == 2
        mock_cursor.executemany.assert_called_once()
    
    @patch('db_sync.connectors.mysql.mysql.connector.connect')
    def test_create_table(self, mock_connect, sample_mysql_config, sample_table_schema):
        """Test table creation"""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        connector = MySQLConnector(sample_mysql_config.dict())
        connector.connect()
        connector.create_table(sample_table_schema)
        
        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args[0][0]
        assert 'CREATE TABLE' in call_args
        assert 'test_table' in call_args
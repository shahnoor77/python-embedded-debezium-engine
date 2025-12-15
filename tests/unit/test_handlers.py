"""
Unit tests for handlers
"""
import pytest
from unittest.mock import Mock, MagicMock, patch

from db_sync.handlers.cdc_handler import CDCHandler
from db_sync.handlers.schema_handler import SchemaHandler
from db_sync.handlers.initial_load import InitialLoadHandler
from db_sync.models.change_event import ChangeEvent, OperationType
from db_sync.models.schema import TableSchema, ColumnDefinition


class TestCDCHandler:
    """Test CDC Handler"""
    
    def test_process_insert_event(self, mock_mysql_connector, sample_table_schema):
        """Test processing INSERT event"""
        schema_handler = Mock()
        schema_handler.get_or_sync_schema.return_value = sample_table_schema
        
        handler = CDCHandler(
            target_connector=mock_mysql_connector,
            schema_handler=schema_handler,
            apply_deletes=True
        )
        
        event = ChangeEvent(
            operation=OperationType.CREATE,
            table_name='test_table',
            after={'id': 1, 'name': 'Test'},
            before=None
        )
        
        handler.process_event(event)
        
        mock_mysql_connector.insert_batch.assert_called_once()
        assert handler.get_statistics()['inserts'] == 1
    
    def test_process_update_event(self, mock_mysql_connector, sample_table_schema):
        """Test processing UPDATE event"""
        schema_handler = Mock()
        schema_handler.get_or_sync_schema.return_value = sample_table_schema
        
        handler = CDCHandler(
            target_connector=mock_mysql_connector,
            schema_handler=schema_handler
        )
        
        event = ChangeEvent(
            operation=OperationType.UPDATE,
            table_name='test_table',
            before={'id': 1, 'name': 'Old'},
            after={'id': 1, 'name': 'New'}
        )
        
        handler.process_event(event)
        
        mock_mysql_connector.update_row.assert_called_once()
        assert handler.get_statistics()['updates'] == 1
    
    def test_process_delete_event(self, mock_mysql_connector, sample_table_schema):
        """Test processing DELETE event"""
        schema_handler = Mock()
        schema_handler.get_or_sync_schema.return_value = sample_table_schema
        
        handler = CDCHandler(
            target_connector=mock_mysql_connector,
            schema_handler=schema_handler,
            apply_deletes=True
        )
        
        event = ChangeEvent(
            operation=OperationType.DELETE,
            table_name='test_table',
            before={'id': 1, 'name': 'Test'},
            after=None
        )
        
        handler.process_event(event)
        
        mock_mysql_connector.delete_row.assert_called_once()
        assert handler.get_statistics()['deletes'] == 1
    
    def test_delete_disabled(self, mock_mysql_connector, sample_table_schema):
        """Test delete operations when disabled"""
        schema_handler = Mock()
        schema_handler.get_or_sync_schema.return_value = sample_table_schema
        
        handler = CDCHandler(
            target_connector=mock_mysql_connector,
            schema_handler=schema_handler,
            apply_deletes=False
        )
        
        event = ChangeEvent(
            operation=OperationType.DELETE,
            table_name='test_table',
            before={'id': 1, 'name': 'Test'},
            after=None
        )
        
        handler.process_event(event)
        
        mock_mysql_connector.delete_row.assert_not_called()
        assert handler.get_statistics()['deletes'] == 0


class TestSchemaHandler:
    """Test Schema Handler"""
    
    def test_sync_table_schema(self, mock_postgres_connector, mock_mysql_connector, 
                               sample_table_schema):
        """Test syncing table schema"""
        mock_postgres_connector.get_table_schema.return_value = sample_table_schema
        mock_mysql_connector.table_exists.return_value = False
        
        handler = SchemaHandler(
            source_connector=mock_postgres_connector,
            target_connector=mock_mysql_connector
        )
        
        handler.sync_table_schema('test_table')
        
        mock_postgres_connector.get_table_schema.assert_called_once_with('test_table')
        mock_mysql_connector.create_table.assert_called_once()
    
    def test_get_or_sync_schema_cached(self, mock_postgres_connector, 
                                      mock_mysql_connector, sample_table_schema):
        """Test getting schema from cache"""
        mock_postgres_connector.get_table_schema.return_value = sample_table_schema
        
        handler = SchemaHandler(
            source_connector=mock_postgres_connector,
            target_connector=mock_mysql_connector
        )
        
        # First call - should fetch and cache
        schema1 = handler.get_or_sync_schema('test_table')
        # Second call - should use cache
        schema2 = handler.get_or_sync_schema('test_table')
        
        assert schema1 == schema2
        # Should only call once (cached)
        assert mock_postgres_connector.get_table_schema.call_count == 1
    
    def test_sync_all_schemas(self, mock_postgres_connector, mock_mysql_connector,
                             sample_table_schema):
        """Test syncing all schemas"""
        mock_postgres_connector.get_all_tables.return_value = ['table1', 'table2']
        mock_postgres_connector.get_table_schema.return_value = sample_table_schema
        mock_mysql_connector.table_exists.return_value = False
        
        handler = SchemaHandler(
            source_connector=mock_postgres_connector,
            target_connector=mock_mysql_connector
        )
        
        handler.sync_all_schemas()
        
        assert mock_postgres_connector.get_table_schema.call_count == 2
        assert mock_mysql_connector.create_table.call_count == 2


class TestInitialLoadHandler:
    """Test Initial Load Handler"""
    
    def test_is_initial_load_needed(self, mock_postgres_connector, mock_mysql_connector,
                                    sample_sync_config):
        """Test checking if initial load is needed"""
        mock_postgres_connector.get_all_tables.return_value = ['users', 'orders']
        mock_mysql_connector.get_all_tables.return_value = []  # Empty target
        
        schema_handler = SchemaHandler(
            source_connector=mock_postgres_connector,
            target_connector=mock_mysql_connector
        )
        
        handler = InitialLoadHandler(
            source_connector=mock_postgres_connector,
            target_connector=mock_mysql_connector,
            schema_handler=schema_handler,
            config=sample_sync_config.initial_load
        )
        
        assert handler.is_initial_load_needed() is True
    
    def test_filter_tables_include(self, mock_postgres_connector, mock_mysql_connector,
                                   sample_sync_config):
        """Test filtering tables with include list"""
        schema_handler = Mock()
        
        # Set include list
        sample_sync_config.initial_load.include_tables = ['users', 'orders']
        
        handler = InitialLoadHandler(
            source_connector=mock_postgres_connector,
            target_connector=mock_mysql_connector,
            schema_handler=schema_handler,
            config=sample_sync_config.initial_load
        )
        
        all_tables = ['users', 'orders', 'logs', 'temp']
        filtered = handler._filter_tables(all_tables)
        
        assert set(filtered) == {'users', 'orders'}
    
    def test_filter_tables_exclude(self, mock_postgres_connector, mock_mysql_connector,
                                   sample_sync_config):
        """Test filtering tables with exclude list"""
        schema_handler = Mock()
        
        # Set exclude list
        sample_sync_config.initial_load.exclude_tables = ['logs', 'temp']
        
        handler = InitialLoadHandler(
            source_connector=mock_postgres_connector,
            target_connector=mock_mysql_connector,
            schema_handler=schema_handler,
            config=sample_sync_config.initial_load
        )
        
        all_tables = ['users', 'orders', 'logs', 'temp']
        filtered = handler._filter_tables(all_tables)
        
        assert set(filtered) == {'users', 'orders'}
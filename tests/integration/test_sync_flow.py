"""
Integration tests for end-to-end sync flow
These tests require actual databases running
"""
import pytest
import time
from unittest.mock import Mock, patch

from db_sync.core.engine import CDCEngine
from db_sync.handlers.initial_load import InitialLoadHandler
from db_sync.handlers.schema_handler import SchemaHandler


# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


@pytest.mark.skip(reason="Requires running databases")
class TestEndToEndSync:
    """
    End-to-end integration tests
    These require PostgreSQL and MySQL to be running
    """
    
    def test_initial_load_flow(self, sample_full_config, 
                               mock_postgres_connector, mock_mysql_connector):
        """Test complete initial load flow"""
        # Setup
        schema_handler = SchemaHandler(
            source_connector=mock_postgres_connector,
            target_connector=mock_mysql_connector
        )
        
        initial_load_handler = InitialLoadHandler(
            source_connector=mock_postgres_connector,
            target_connector=mock_mysql_connector,
            schema_handler=schema_handler,
            config=sample_full_config.sync.initial_load
        )
        
        # Mock data
        mock_postgres_connector.get_all_tables.return_value = ['users', 'orders']
        mock_postgres_connector.get_row_count.return_value = 100
        mock_postgres_connector.fetch_all_rows.return_value = iter([
            [{'id': i, 'name': f'User {i}'} for i in range(1, 51)],
            [{'id': i, 'name': f'User {i}'} for i in range(51, 101)]
        ])
        
        mock_mysql_connector.get_all_tables.return_value = []
        
        # Execute
        if initial_load_handler.is_initial_load_needed():
            initial_load_handler.perform_initial_load()
        
        # Verify
        assert len(initial_load_handler.completed_tables) > 0
        assert mock_mysql_connector.create_table.called
        assert mock_mysql_connector.insert_batch.called
    
    def test_cdc_event_processing(self, sample_change_event_insert,
                                  mock_mysql_connector):
        """Test CDC event processing"""
        from db_sync.handlers.cdc_handler import CDCHandler
        from db_sync.models.change_event import ChangeEvent
        
        schema_handler = Mock()
        schema_handler.get_or_sync_schema.return_value = Mock(primary_keys=['id'])
        
        handler = CDCHandler(
            target_connector=mock_mysql_connector,
            schema_handler=schema_handler
        )
        
        # Process insert event
        event = ChangeEvent.from_debezium_message(sample_change_event_insert)
        handler.process_event(event)
        
        # Verify
        assert handler.get_statistics()['inserts'] == 1
        mock_mysql_connector.insert_batch.assert_called_once()


@pytest.mark.skip(reason="Requires running databases")
class TestSchemaEvolution:
    """Test schema evolution and changes"""
    
    def test_add_column_during_sync(self, mock_postgres_connector, 
                                    mock_mysql_connector, sample_table_schema):
        """Test adding new column during sync"""
        # Initial schema
        mock_postgres_connector.get_table_schema.return_value = sample_table_schema
        
        schema_handler = SchemaHandler(
            source_connector=mock_postgres_connector,
            target_connector=mock_mysql_connector,
            auto_detect_changes=True
        )
        
        # Sync initial schema
        schema_handler.sync_table_schema('test_table')
        
        # Simulate new column added
        from db_sync.models.schema import ColumnDefinition
        
        new_column = ColumnDefinition(
            name='status',
            data_type='VARCHAR(50)',
            nullable=True
        )
        sample_table_schema.add_column(new_column)
        
        # Re-sync
        schema_handler.clear_cache()
        schema_handler.sync_table_schema('test_table')
        
        # Verify alter table was called
        assert mock_mysql_connector.alter_table_add_column.called


class TestConfigValidation:
    """Test configuration validation"""
    
    def test_valid_config(self, sample_full_config):
        """Test valid configuration"""
        assert sample_full_config.source.type == 'postgresql'
        assert sample_full_config.target.type == 'mysql'
        assert sample_full_config.sync.initial_load.enabled is True
    
    def test_connection_strings(self, sample_full_config):
        """Test connection string generation"""
        source_conn = sample_full_config.get_source_connection_string()
        target_conn = sample_full_config.get_target_connection_string()
        
        assert 'postgresql://' in source_conn
        assert 'mysql+mysqlconnector://' in target_conn
    
    def test_config_from_yaml(self, temp_dir):
        """Test loading config from YAML file"""
        from db_sync.core.config import Config
        
        config_file = temp_dir / 'config.yaml'
        config_content = """
source:
  type: postgresql
  host: localhost
  port: 5432
  database: testdb
  username: user
  password: pass

target:
  type: mysql
  host: localhost
  port: 3306
  database: testdb
  username: user
  password: pass

kafka:
  bootstrap_servers: localhost:9092
  group_id: test

debezium:
  connector_class: io.debezium.connector.postgresql.PostgresConnector
  server_name: test

sync:
  initial_load:
    enabled: true
    batch_size: 1000
    parallel_tables: 4
  cdc:
    enabled: true
    auto_create_tables: true
    auto_detect_schema_changes: true
    apply_deletes: true

monitoring:
  enable_metrics: true

logging:
  level: INFO

performance:
  max_workers: 10

state:
  storage_path: /tmp/state
  offset_storage_path: /tmp/offsets
"""
        
        config_file.write_text(config_content)
        
        config = Config.from_yaml(str(config_file))
        
        assert config.source.host == 'localhost'
        assert config.target.port == 3306
        assert config.sync.initial_load.batch_size == 1000
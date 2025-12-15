"""
Pytest configuration and shared fixtures
"""
import pytest
from unittest.mock import Mock, MagicMock
from pathlib import Path
import tempfile

from db_sync.core.config import (
    Config, DatabaseConfig, KafkaConfig, DebeziumConfig,
    InitialLoadConfig, CDCConfig, SyncConfig, MonitoringConfig,
    LoggingConfig, PerformanceConfig, StateConfig
)
from db_sync.models.schema import TableSchema, ColumnDefinition


@pytest.fixture
def temp_dir():
    """Create temporary directory for tests"""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_postgres_config():
    """Sample PostgreSQL configuration"""
    return DatabaseConfig(
        type="postgresql",
        host="localhost",
        port=5432,
        database="testdb",
        username="testuser",
        password="testpass",
        schema="public"
    )


@pytest.fixture
def sample_mysql_config():
    """Sample MySQL configuration"""
    return DatabaseConfig(
        type="mysql",
        host="localhost",
        port=3306,
        database="testdb",
        username="testuser",
        password="testpass"
    )


@pytest.fixture
def sample_debezium_config():
    """Sample Debezium configuration"""
    return DebeziumConfig(
        connector_class="io.debezium.connector.postgresql.PostgresConnector",
        server_name="testserver",
        topic_prefix="test",
        plugin_name="pgoutput",
        slot_name="test_slot",
        publication_name="test_pub",
        snapshot_mode="initial"
    )


@pytest.fixture
def sample_sync_config():
    """Sample sync configuration"""
    return SyncConfig(
        initial_load=InitialLoadConfig(
            enabled=True,
            batch_size=100,
            parallel_tables=2
        ),
        cdc=CDCConfig(
            enabled=True,
            auto_create_tables=True,
            auto_detect_schema_changes=True,
            apply_deletes=True
        )
    )


@pytest.fixture
def sample_full_config(sample_postgres_config, sample_mysql_config, 
                       sample_debezium_config, sample_sync_config, temp_dir):
    """Complete configuration for testing"""
    return Config(
        source=sample_postgres_config,
        target=sample_mysql_config,
        kafka=KafkaConfig(
            bootstrap_servers="localhost:9092",
            group_id="test-group"
        ),
        debezium=sample_debezium_config,
        sync=sample_sync_config,
        monitoring=MonitoringConfig(),
        logging=LoggingConfig(
            file=str(temp_dir / "test.log")
        ),
        performance=PerformanceConfig(),
        state=StateConfig(
            storage_path=str(temp_dir / "state"),
            offset_storage_path=str(temp_dir / "offsets")
        )
    )


@pytest.fixture
def sample_table_schema():
    """Sample table schema"""
    return TableSchema(
        name="test_table",
        columns=[
            ColumnDefinition(
                name="id",
                data_type="INTEGER",
                nullable=False,
                is_primary_key=True
            ),
            ColumnDefinition(
                name="name",
                data_type="VARCHAR(100)",
                nullable=False
            ),
            ColumnDefinition(
                name="email",
                data_type="VARCHAR(255)",
                nullable=True
            ),
            ColumnDefinition(
                name="created_at",
                data_type="TIMESTAMP",
                nullable=True
            )
        ],
        primary_keys=["id"]
    )


@pytest.fixture
def mock_postgres_connector():
    """Mock PostgreSQL connector"""
    connector = Mock()
    connector.config = {
        'type': 'postgresql',
        'host': 'localhost',
        'port': 5432,
        'database': 'testdb'
    }
    connector.connect = Mock()
    connector.disconnect = Mock()
    connector.get_all_tables = Mock(return_value=["table1", "table2"])
    connector.table_exists = Mock(return_value=True)
    connector.get_row_count = Mock(return_value=100)
    return connector


@pytest.fixture
def mock_mysql_connector():
    """Mock MySQL connector"""
    connector = Mock()
    connector.config = {
        'type': 'mysql',
        'host': 'localhost',
        'port': 3306,
        'database': 'testdb'
    }
    connector.connect = Mock()
    connector.disconnect = Mock()
    connector.table_exists = Mock(return_value=False)
    connector.create_table = Mock()
    connector.insert_batch = Mock(return_value=10)
    connector.update_row = Mock()
    connector.delete_row = Mock()
    return connector


@pytest.fixture
def sample_change_event_insert():
    """Sample INSERT change event"""
    return {
        'payload': {
            'op': 'c',
            'after': {
                'id': 1,
                'name': 'Test User',
                'email': 'test@example.com'
            },
            'source': {
                'table': 'users',
                'ts_ms': 1234567890000
            }
        }
    }


@pytest.fixture
def sample_change_event_update():
    """Sample UPDATE change event"""
    return {
        'payload': {
            'op': 'u',
            'before': {
                'id': 1,
                'name': 'Test User',
                'email': 'test@example.com'
            },
            'after': {
                'id': 1,
                'name': 'Updated User',
                'email': 'updated@example.com'
            },
            'source': {
                'table': 'users',
                'ts_ms': 1234567890000
            }
        }
    }


@pytest.fixture
def sample_change_event_delete():
    """Sample DELETE change event"""
    return {
        'payload': {
            'op': 'd',
            'before': {
                'id': 1,
                'name': 'Test User',
                'email': 'test@example.com'
            },
            'source': {
                'table': 'users',
                'ts_ms': 1234567890000
            }
        }
    }
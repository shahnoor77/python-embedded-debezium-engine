"""
Unit tests for data models
"""
import pytest
from datetime import datetime

from db_sync.models.schema import TableSchema, ColumnDefinition
from db_sync.models.change_event import ChangeEvent, OperationType


class TestTableSchema:
    """Test TableSchema model"""
    
    def test_create_schema(self):
        """Test creating table schema"""
        schema = TableSchema(
            name='users',
            columns=[
                ColumnDefinition(name='id', data_type='INTEGER', nullable=False),
                ColumnDefinition(name='name', data_type='VARCHAR(100)', nullable=False)
            ],
            primary_keys=['id']
        )
        
        assert schema.name == 'users'
        assert len(schema.columns) == 2
        assert schema.primary_keys == ['id']
    
    def test_get_column(self, sample_table_schema):
        """Test getting column by name"""
        column = sample_table_schema.get_column('name')
        
        assert column is not None
        assert column.name == 'name'
        assert column.data_type == 'VARCHAR(100)'
    
    def test_get_column_not_found(self, sample_table_schema):
        """Test getting non-existent column"""
        column = sample_table_schema.get_column('nonexistent')
        
        assert column is None
    
    def test_add_column(self, sample_table_schema):
        """Test adding new column"""
        initial_count = len(sample_table_schema.columns)
        
        new_column = ColumnDefinition(
            name='status',
            data_type='VARCHAR(50)',
            nullable=True
        )
        
        sample_table_schema.add_column(new_column)
        
        assert len(sample_table_schema.columns) == initial_count + 1
        assert sample_table_schema.get_column('status') is not None
    
    def test_to_dict(self, sample_table_schema):
        """Test converting schema to dictionary"""
        schema_dict = sample_table_schema.to_dict()
        
        assert schema_dict['name'] == 'test_table'
        assert len(schema_dict['columns']) == 4
        assert schema_dict['primary_keys'] == ['id']
    
    def test_primary_key_marking(self):
        """Test that primary key columns are marked correctly"""
        schema = TableSchema(
            name='test',
            columns=[
                ColumnDefinition(name='id', data_type='INTEGER'),
                ColumnDefinition(name='name', data_type='VARCHAR(100)')
            ],
            primary_keys=['id']
        )
        
        id_column = schema.get_column('id')
        name_column = schema.get_column('name')
        
        assert id_column.is_primary_key is True
        assert name_column.is_primary_key is False


class TestColumnDefinition:
    """Test ColumnDefinition model"""
    
    def test_create_column(self):
        """Test creating column definition"""
        column = ColumnDefinition(
            name='email',
            data_type='VARCHAR(255)',
            nullable=True,
            default=None
        )
        
        assert column.name == 'email'
        assert column.data_type == 'VARCHAR(255)'
        assert column.nullable is True
        assert column.is_primary_key is False
    
    def test_column_to_dict(self):
        """Test converting column to dictionary"""
        column = ColumnDefinition(
            name='age',
            data_type='INTEGER',
            nullable=False,
            default='0'
        )
        
        column_dict = column.to_dict()
        
        assert column_dict['name'] == 'age'
        assert column_dict['data_type'] == 'INTEGER'
        assert column_dict['nullable'] is False
        assert column_dict['default'] == '0'


class TestChangeEvent:
    """Test ChangeEvent model"""
    
    def test_create_insert_event(self):
        """Test creating INSERT event"""
        event = ChangeEvent(
            operation=OperationType.CREATE,
            table_name='users',
            after={'id': 1, 'name': 'Alice'},
            before=None
        )
        
        assert event.is_insert() is True
        assert event.is_update() is False
        assert event.is_delete() is False
        assert event.table_name == 'users'
    
    def test_create_update_event(self):
        """Test creating UPDATE event"""
        event = ChangeEvent(
            operation=OperationType.UPDATE,
            table_name='users',
            before={'id': 1, 'name': 'Alice'},
            after={'id': 1, 'name': 'Alice Smith'}
        )
        
        assert event.is_update() is True
        assert event.is_insert() is False
        assert event.is_delete() is False
    
    def test_create_delete_event(self):
        """Test creating DELETE event"""
        event = ChangeEvent(
            operation=OperationType.DELETE,
            table_name='users',
            before={'id': 1, 'name': 'Alice'},
            after=None
        )
        
        assert event.is_delete() is True
        assert event.is_insert() is False
        assert event.is_update() is False
    
    def test_from_debezium_insert(self, sample_change_event_insert):
        """Test parsing Debezium INSERT message"""
        event = ChangeEvent.from_debezium_message(sample_change_event_insert)
        
        assert event.operation == OperationType.CREATE
        assert event.table_name == 'users'
        assert event.after['id'] == 1
        assert event.after['name'] == 'Test User'
        assert event.before is None
    
    def test_from_debezium_update(self, sample_change_event_update):
        """Test parsing Debezium UPDATE message"""
        event = ChangeEvent.from_debezium_message(sample_change_event_update)
        
        assert event.operation == OperationType.UPDATE
        assert event.table_name == 'users'
        assert event.before['name'] == 'Test User'
        assert event.after['name'] == 'Updated User'
    
    def test_from_debezium_delete(self, sample_change_event_delete):
        """Test parsing Debezium DELETE message"""
        event = ChangeEvent.from_debezium_message(sample_change_event_delete)
        
        assert event.operation == OperationType.DELETE
        assert event.table_name == 'users'
        assert event.before['id'] == 1
        assert event.after is None
    
    def test_get_primary_key_values(self):
        """Test extracting primary key values"""
        event = ChangeEvent(
            operation=OperationType.UPDATE,
            table_name='users',
            before={'id': 1, 'name': 'Old'},
            after={'id': 1, 'name': 'New', 'email': 'new@example.com'}
        )
        
        pk_values = event.get_primary_key_values(['id'])
        
        assert pk_values == {'id': 1}
    
    def test_get_primary_key_values_composite(self):
        """Test extracting composite primary key values"""
        event = ChangeEvent(
            operation=OperationType.CREATE,
            table_name='order_items',
            after={'order_id': 1, 'item_id': 5, 'quantity': 3},
            before=None
        )
        
        pk_values = event.get_primary_key_values(['order_id', 'item_id'])
        
        assert pk_values == {'order_id': 1, 'item_id': 5}
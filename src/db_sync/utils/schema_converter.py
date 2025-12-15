"""
Schema conversion utilities for cross-database type mapping
"""
import logging
from typing import Dict

from db_sync.models.schema import TableSchema, ColumnDefinition

logger = logging.getLogger(__name__)


class SchemaConverter:
    """Converts schemas between different database types"""
    
    # Type mapping from source to target databases
    TYPE_MAPPINGS = {
        ('postgresql', 'postgresql'): {}, # No conversion needed
        ('postgresql', 'mysql'): {
            'BIGINT': 'BIGINT',
            'INTEGER': 'INT',
            'INT4': 'INT', 
            'SMALLINT': 'SMALLINT',
            # FIX 1: Explicitly map SERIAL/BIGSERIAL for MySQL's AUTO_INCREMENT setup
            'SERIAL': 'BIGINT', 
            'BIGSERIAL': 'BIGINT', 
            
            'NUMERIC': 'DECIMAL',
            'REAL': 'FLOAT',
            'DOUBLE PRECISION': 'DOUBLE',
            'VARCHAR': 'VARCHAR',
            'CHAR': 'CHAR',
            'TEXT': 'TEXT',
            'BOOLEAN': 'TINYINT(1)',
            'DATE': 'DATE',
            'TIMESTAMP': 'DATETIME',
            'TIMESTAMPTZ': 'DATETIME',
            'TIME': 'TIME',
            'JSON': 'JSON',
            'JSONB': 'JSON',
            'UUID': 'CHAR(36)',
            'BYTEA': 'BLOB',
        },
        ('mysql', 'postgresql'): {
            'BIGINT': 'BIGINT',
            'INT': 'INTEGER',
            'SMALLINT': 'SMALLINT',
            'DECIMAL': 'NUMERIC',
            'FLOAT': 'REAL',
            'DOUBLE': 'DOUBLE PRECISION',
            'VARCHAR': 'VARCHAR',
            'CHAR': 'CHAR',
            'TEXT': 'TEXT',
            'TINYINT(1)': 'BOOLEAN',
            'DATE': 'DATE',
            'DATETIME': 'TIMESTAMP',
            'TIME': 'TIME',
            'JSON': 'JSONB',
            'BLOB': 'BYTEA',
        },
    }

    def convert_schema(self, schema: TableSchema, source_type: str, 
                         target_type: str) -> TableSchema:
        """
        Convert schema from source database type to target database type
        """
        if source_type == target_type:
            return schema
        
        mapping_key = (source_type.lower(), target_type.lower())
        type_map = self.TYPE_MAPPINGS.get(mapping_key, {})
        
        if not type_map:
            logger.warning(
                f"No type mapping found for {source_type} -> {target_type}, "
                f"using source types as-is"
            )
            return schema
        
        converted_columns = []
        for col in schema.columns:
            converted_type = self._convert_type(col.data_type, type_map)
            
            converted_default = col.default
            
            # FIX 2: Clear PostgreSQL default sequence values for MySQL AUTO_INCREMENT
            is_postgres_source = source_type.lower() == 'postgresql'
            is_mysql_target = target_type.lower() == 'mysql'
            
            if is_postgres_source and is_mysql_target and col.is_primary_key:
                # If it's a primary key, clear the default if it contains 'nextval'
                if isinstance(col.default, str) and 'nextval' in col.default.lower():
                    converted_default = None

            converted_col = ColumnDefinition(
                name=col.name,
                data_type=converted_type,
                nullable=col.nullable,
                default=converted_default,
                is_primary_key=col.is_primary_key
            )
            converted_columns.append(converted_col)
        
        return TableSchema(
            name=schema.name,
            columns=converted_columns,
            primary_keys=schema.primary_keys,
            indexes=schema.indexes
        )

    def _convert_type(self, data_type: str, type_map: Dict[str, str]) -> str:
        """
        Convert a single data type using the type map, stripping invalid MySQL keywords.
        """
        
        # 1. Start with the full data type, normalized to uppercase
        normalized_type = data_type.upper().strip()
        
        # 2. Extract base type, stripping time zone qualifiers (FIX for MySQL syntax error 1064)
        base_type_lookup = normalized_type.replace(' WITHOUT TIME ZONE', '')
        base_type_lookup = base_type_lookup.replace(' WITH TIME ZONE', '')
        
        # 3. Strip parameters for the map lookup (e.g., VARCHAR(255) -> VARCHAR)
        # This handles the case where the type is e.g., TIMESTAMP(6) without time zone
        base_type = base_type_lookup.split('(')[0].strip()
        
        # 4. Perform type mapping
        converted_base = type_map.get(base_type, base_type)
        
        # 5. Re-apply length/precision parameters if they existed
        if '(' in normalized_type:
            # Grab the parameters part (e.g., "(255)" or "(6)")
            params = normalized_type[normalized_type.index('('):]
            return f"{converted_base}{params}"
        
        # 6. If no parameters, return only the converted base type (e.g., DATETIME)
        return converted_base
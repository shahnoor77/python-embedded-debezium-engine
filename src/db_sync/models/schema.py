"""
Data models for schema representation
"""
from dataclasses import dataclass, field
from typing import Any, List, Optional


@dataclass
class ColumnDefinition:
    """Column definition in a table"""
    name: str
    data_type: str
    nullable: bool = True
    default: Optional[Any] = None
    is_primary_key: bool = False

    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return {
            'name': self.name,
            'data_type': self.data_type,
            'nullable': self.nullable,
            'default': self.default,
            'is_primary_key': self.is_primary_key
        }


@dataclass
class TableSchema:
    """Table schema definition"""
    name: str
    columns: List[ColumnDefinition] = field(default_factory=list)
    primary_keys: List[str] = field(default_factory=list)
    indexes: List[dict] = field(default_factory=list)
    
    def __post_init__(self):
        """Mark primary key columns"""
        for col in self.columns:
            if col.name in self.primary_keys:
                col.is_primary_key = True

    def get_column(self, name: str) -> Optional[ColumnDefinition]:
        """Get column by name"""
        for col in self.columns:
            if col.name == name:
                return col
        return None

    def add_column(self, column: ColumnDefinition) -> None:
        """Add a new column"""
        if not self.get_column(column.name):
            self.columns.append(column)

    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return {
            'name': self.name,
            'columns': [col.to_dict() for col in self.columns],
            'primary_keys': self.primary_keys,
            'indexes': self.indexes
        }
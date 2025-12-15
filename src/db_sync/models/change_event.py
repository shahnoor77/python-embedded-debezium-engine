"""
CDC change event models
"""
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional


class OperationType(Enum):
    """CDC operation types"""
    CREATE = "c"  # Insert
    UPDATE = "u"
    DELETE = "d"
    READ = "r"   # Initial snapshot read


@dataclass
class ChangeEvent:
    """Represents a change event from CDC"""
    operation: OperationType
    table_name: str
    before: Optional[Dict[str, Any]] = None
    after: Optional[Dict[str, Any]] = None
    source_metadata: Optional[Dict[str, Any]] = None
    timestamp: Optional[datetime] = None
    transaction_id: Optional[str] = None

    @classmethod
    def from_debezium_message(cls, message: Dict[str, Any]) -> "ChangeEvent":
        """Parse a Debezium CDC message"""
        payload = message.get('payload', {})
        op = payload.get('op')
        
        # Map Debezium operation codes
        op_mapping = {
            'c': OperationType.CREATE,
            'u': OperationType.UPDATE,
            'd': OperationType.DELETE,
            'r': OperationType.READ
        }
        
        operation = op_mapping.get(op, OperationType.READ)
        
        # Extract source metadata
        source = payload.get('source', {})
        table_name = source.get('table', '')
        
        # Extract before and after states
        before = payload.get('before')
        after = payload.get('after')
        
        # Extract timestamp
        ts_ms = payload.get('ts_ms') or source.get('ts_ms')
        timestamp = datetime.fromtimestamp(ts_ms / 1000) if ts_ms else None
        
        return cls(
            operation=operation,
            table_name=table_name,
            before=before,
            after=after,
            source_metadata=source,
            timestamp=timestamp,
            transaction_id=payload.get('transaction', {}).get('id')
        )

    def get_primary_key_values(self, primary_keys: list) -> Dict[str, Any]:
        """Extract primary key values from the event"""
        data = self.after if self.after else self.before
        if not data:
            return {}
        
        return {pk: data.get(pk) for pk in primary_keys if pk in data}

    def is_insert(self) -> bool:
        """Check if event is an insert"""
        return self.operation == OperationType.CREATE

    def is_update(self) -> bool:
        """Check if event is an update"""
        return self.operation == OperationType.UPDATE

    def is_delete(self) -> bool:
        """Check if event is a delete"""
        return self.operation == OperationType.DELETE

    def is_snapshot(self) -> bool:
        """Check if event is from initial snapshot"""
        return self.operation == OperationType.READ
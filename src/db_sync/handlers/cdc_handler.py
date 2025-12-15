"""
CDC event handler for processing change events
"""
import logging
from typing import Dict, Any

from db_sync.connectors.base import BaseConnector
from db_sync.models.change_event import ChangeEvent
from db_sync.handlers.schema_handler import SchemaHandler
from db_sync.utils.retry import with_retry

logger = logging.getLogger(__name__)


class CDCHandler:
    """Handles CDC events and applies them to target database"""

    def __init__(self, target_connector: BaseConnector, schema_handler: SchemaHandler,
                 apply_deletes: bool = True, conflict_resolution: str = "source_wins"):
        self.target = target_connector
        self.schema_handler = schema_handler
        self.apply_deletes = apply_deletes
        self.conflict_resolution = conflict_resolution
        self.stats = {
            'inserts': 0,
            'updates': 0,
            'deletes': 0,
            'errors': 0
        }

    @with_retry(max_attempts=3, delay_seconds=5)
    def process_event(self, event: ChangeEvent) -> None:
        """
        Process a single CDC event
        
        Args:
            event: ChangeEvent to process
        """
        try:
            # Ensure table exists in target
            if not self.target.table_exists(event.table_name):
                logger.info(f"Table {event.table_name} not found in target, creating...")
                schema = self.schema_handler.get_or_sync_schema(event.table_name)
                self.target.create_table(schema)

            # Route to appropriate handler
            if event.is_insert():
                self._handle_insert(event)
            elif event.is_update():
                self._handle_update(event)
            elif event.is_delete():
                self._handle_delete(event)
            elif event.is_snapshot():
                self._handle_insert(event)  # Treat snapshot reads as inserts

        except Exception as e:
            logger.error(f"Error processing event for table {event.table_name}: {e}")
            self.stats['errors'] += 1
            raise

    def _handle_insert(self, event: ChangeEvent) -> None:
        """Handle insert operation"""
        if not event.after:
            logger.warning(f"Insert event has no 'after' data for table {event.table_name}")
            return

        try:
            self.target.insert_batch(event.table_name, [event.after])
            self.stats['inserts'] += 1
            logger.debug(f"Inserted row into {event.table_name}")
        except Exception as e:
            # If insert fails due to duplicate key, treat as update
            if "duplicate key" in str(e).lower() or "unique constraint" in str(e).lower():
                logger.info(f"Duplicate key on insert, converting to update for {event.table_name}")
                self._handle_update(event)
            else:
                raise

    def _handle_update(self, event: ChangeEvent) -> None:
        """Handle update operation"""
        if not event.after:
            logger.warning(f"Update event has no 'after' data for table {event.table_name}")
            return

        # Get primary keys for the table
        schema = self.schema_handler.get_or_sync_schema(event.table_name)
        pk_values = event.get_primary_key_values(schema.primary_keys)

        if not pk_values:
            logger.warning(f"No primary key values found for update on {event.table_name}")
            return

        try:
            # Check if row exists
            if self.conflict_resolution == "target_wins":
                # Skip update if target has row (target wins)
                logger.debug(f"Conflict resolution: target_wins, skipping update")
                return
            
            self.target.update_row(event.table_name, pk_values, event.after)
            self.stats['updates'] += 1
            logger.debug(f"Updated row in {event.table_name}")
        except Exception as e:
            # If update fails due to missing row, insert it
            if "no rows" in str(e).lower() or "not found" in str(e).lower():
                logger.info(f"Row not found on update, inserting for {event.table_name}")
                self.target.insert_batch(event.table_name, [event.after])
                self.stats['inserts'] += 1
            else:
                raise

    def _handle_delete(self, event: ChangeEvent) -> None:
        """Handle delete operation"""
        if not self.apply_deletes:
            logger.debug(f"Delete operations disabled, skipping delete for {event.table_name}")
            return

        if not event.before:
            logger.warning(f"Delete event has no 'before' data for table {event.table_name}")
            return

        # Get primary keys for the table
        schema = self.schema_handler.get_or_sync_schema(event.table_name)
        pk_values = event.get_primary_key_values(schema.primary_keys)

        if not pk_values:
            logger.warning(f"No primary key values found for delete on {event.table_name}")
            return

        try:
            self.target.delete_row(event.table_name, pk_values)
            self.stats['deletes'] += 1
            logger.debug(f"Deleted row from {event.table_name}")
        except Exception as e:
            logger.warning(f"Error deleting row from {event.table_name}: {e}")

    def get_statistics(self) -> Dict[str, int]:
        """Get processing statistics"""
        return self.stats.copy()

    def reset_statistics(self) -> None:
        """Reset statistics counters"""
        self.stats = {
            'inserts': 0,
            'updates': 0,
            'deletes': 0,
            'errors': 0
        }
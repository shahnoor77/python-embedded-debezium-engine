"""
Main CDC engine using Debezium Embedded Engine (No Kafka)
"""
import json
import logging
import signal
import sys
import time
from typing import Optional, Dict, Any

from db_sync.core.config import Config
from db_sync.core.debezium_engine import DebeziumEmbeddedEngine
from db_sync.connectors.factory import ConnectorFactory
from db_sync.handlers.cdc_handler import CDCHandler
from db_sync.handlers.schema_handler import SchemaHandler
from db_sync.handlers.initial_load import InitialLoadHandler
from db_sync.models.change_event import ChangeEvent

logger = logging.getLogger(__name__)


class CDCEngine:
    """Main CDC engine for database synchronization using Debezium Embedded"""

    def __init__(self, config: Config):
        self.config = config
        self.running = False
        self.debezium_engine: Optional[DebeziumEmbeddedEngine] = None
        
        # Initialize connectors
        self.source_connector = ConnectorFactory.create_connector(
            config.source.type,
            config.source.dict()
        )
        self.target_connector = ConnectorFactory.create_connector(
            config.target.type,
            config.target.dict()
        )
        
        # Initialize handlers
        self.schema_handler = SchemaHandler(
            self.source_connector,
            self.target_connector,
            auto_detect_changes=config.sync.cdc.auto_detect_schema_changes
        )
        
        self.cdc_handler = CDCHandler(
            self.target_connector,
            self.schema_handler,
            apply_deletes=config.sync.cdc.apply_deletes,
            conflict_resolution=config.sync.cdc.conflict_resolution
        )
        
        self.initial_load_handler = InitialLoadHandler(
            self.source_connector,
            self.target_connector,
            self.schema_handler,
            config.sync.initial_load
        )
        
        # Statistics
        self.stats = {
            'events_processed': 0,
            'last_stats_time': time.time()
        }
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def start(self) -> None:
        """Start the CDC engine"""
        logger.info("=" * 80)
        logger.info("Starting Debezium Embedded CDC Engine")
        logger.info("=" * 80)
        
        try:
            # Connect to databases
            logger.info("Connecting to source database...")
            self.source_connector.connect()
            
            logger.info("Connecting to target database...")
            self.target_connector.connect()
            
            # Perform initial load if needed
            if self.initial_load_handler.is_initial_load_needed():
                logger.info("Performing initial data load...")
                self.initial_load_handler.perform_initial_load()
                logger.info("Initial load completed")
            else:
                logger.info("Skipping initial load (data already exists)")
            
            # Start CDC if enabled
            if self.config.sync.cdc.enabled:
                logger.info("Starting CDC processing with Debezium Embedded Engine...")
                self._start_cdc()
            else:
                logger.info("CDC disabled, exiting")
                
        except Exception as e:
            logger.error(f"Error starting CDC engine: {e}")
            raise
        finally:
            self.stop()

    def _start_cdc(self) -> None:
        """Start CDC processing using Debezium Embedded Engine"""
        # Build Debezium configuration
        debezium_config = self._build_debezium_config()
        
        logger.info("Initializing Debezium Embedded Engine...")
        
        try:
            # Create and start Debezium engine
            self.debezium_engine = DebeziumEmbeddedEngine(
                config=debezium_config,
                change_consumer=self._process_change_record
            )
            
            self.debezium_engine.start()
            self.running = True
            
            logger.info("Debezium Embedded Engine started successfully")
            logger.info("Waiting for change events...")
            
            # Keep the main thread alive
            self._monitor_loop()
            
        except Exception as e:
            logger.error(f"Error in CDC processing: {e}")
            raise

    def _build_debezium_config(self) -> Dict[str, Any]:
        """
        Build Debezium configuration using required dot.notation keys.
        Supports snake_case to dot.notation conversion for user flexibility.
        """
        config = {}
        
        # 1. Process all keys from the 'debezium' block in config.yaml (Allows snake_case/dot.notation)
        for key, value in self.config.debezium.dict().items():
            if value is not None:
                
                # Convert snake_case to dot.notation if necessary
                if '_' in key:
                    processed_key = key.replace('_', '.')
                else:
                    processed_key = key
                
                config[processed_key] = value

        # 2. OVERRIDE/ENSURE critical database connection settings from the reliable 'source' block.
        # This guarantees the connection parameters are present and correct in dot.notation.
        config['database.hostname'] = self.config.source.host
        config['database.port'] = str(self.config.source.port)
        config['database.user'] = self.config.source.username
        config['database.password'] = self.config.source.password
        config['database.dbname'] = self.config.source.database
        
        # 3. Ensure other critical settings (using dot.notation)
        config['name'] = 'debezium-embedded-engine'
        config['connector.class'] = self.config.debezium.connector_class
        config['database.server.name'] = self.config.debezium.server_name
        config['topic.naming.strategy'] = 'io.debezium.schema.DefaultTopicNamingStrategy'
        # 4. PostgreSQL specific (using dot.notation)
        if self.config.source.type == 'postgresql':
            config['plugin.name'] = self.config.debezium.plugin_name
            config['slot.name'] = self.config.debezium.slot_name
            config['publication.name'] = self.config.debezium.publication_name
        
        logger.debug(f"Debezium configuration: {config}")
        
        return config

    def _process_change_record(self, record: Dict[str, Any]) -> None:
        """
        Process a change record from Debezium
        
        Args:
            record: Change record dictionary
        """
        try:
            # Extract value (the actual change data)
            value = record.get('value')
            if not value:
                return
            
            # Convert to ChangeEvent
            event = ChangeEvent.from_debezium_message({'payload': value})
            
            # Process the event
            self.cdc_handler.process_event(event)
            
            # Update statistics
            self.stats['events_processed'] += 1
            
            # Log statistics periodically
            current_time = time.time()
            if current_time - self.stats['last_stats_time'] > 60:  # Every minute
                stats = self.cdc_handler.get_statistics()
                logger.info(
                    f"CDC Statistics - Events: {self.stats['events_processed']}, "
                    f"Inserts: {stats['inserts']}, Updates: {stats['updates']}, "
                    f"Deletes: {stats['deletes']}, Errors: {stats['errors']}"
                )
                self.stats['last_stats_time'] = current_time
                
        except Exception as e:
            logger.error(f"Error processing change record: {e}")
            logger.debug(f"Record: {record}")

    def _monitor_loop(self) -> None:
        """Monitor loop to keep engine running"""
        while self.running:
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                logger.info("Received keyboard interrupt")
                break

    def stop(self) -> None:
        """Stop the CDC engine"""
        logger.info("Stopping CDC Engine")
        self.running = False
        
        # Stop Debezium engine
        if self.debezium_engine:
            logger.info("Stopping Debezium Embedded Engine")
            self.debezium_engine.stop()
        
        # Disconnect from databases
        logger.info("Disconnecting from databases")
        try:
            self.source_connector.disconnect()
        except Exception as e:
            logger.error(f"Error disconnecting from source: {e}")
        
        try:
            self.target_connector.disconnect()
        except Exception as e:
            logger.error(f"Error disconnecting from target: {e}")
        
        # Log final statistics
        stats = self.cdc_handler.get_statistics()
        logger.info("=" * 80)
        logger.info("Final Statistics")
        logger.info("=" * 80)
        logger.info(f"Total Events Processed: {self.stats['events_processed']}")
        logger.info(f"Inserts: {stats['inserts']}")
        logger.info(f"Updates: {stats['updates']}")
        logger.info(f"Deletes: {stats['deletes']}")
        logger.info(f"Errors: {stats['errors']}")
        logger.info("=" * 80)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False
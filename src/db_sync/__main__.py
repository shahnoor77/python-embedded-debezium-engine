"""
Main entry point for the DB Sync application
"""
import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv
from db_sync.core.config import Config
from db_sync.core.engine import CDCEngine
from db_sync.core.debezium_engine import DebeziumEmbeddedEngine
from db_sync.utils.logger import setup_logging

logger = logging.getLogger(__name__)
load_dotenv()

def main():
    """Main function"""
    try:
        # Get config file path
        config_file = os.getenv('CONFIG_FILE', '/app/config/config.yaml')

        if not Path(config_file).exists():
            print(f"Error: Configuration file not found: {config_file}")
            sys.exit(1)

        # Load configuration
        print(f"Loading configuration from {config_file}")
        config = Config.from_yaml(config_file)

        # Setup logging (Pydantic V2 uses .model_dump() instead of .dict())
        setup_logging(config.logging.model_dump())

        logger.info("=" * 80)
        logger.info("DB Sync - Enterprise Database Synchronization System")
        logger.info("=" * 80)
        logger.info(f"Source: {config.source.type} - {config.source.host}:{config.source.port}")
        logger.info(f"Target: {config.target.type} - {config.target.host}:{config.target.port}")
        logger.info("=" * 80)

        # Function to process changes from Debezium
        def process_change(change_record):
            logger.info(f"Received change: {change_record}")
            # Here you can integrate with your CDCEngine logic
            # Example: CDCEngine.apply_change(change_record)
            # For now, I just log the record

        # Start Debezium engine using context manager
        with DebeziumEmbeddedEngine(
            config=config.debezium.model_dump(),  # Pydantic V2
            change_consumer=process_change
        ) as engine:
            logger.info("Debezium engine is running, starting CDCEngine...")

            # Start CDC engine (optional: initial load etc.)
            cdc_engine = CDCEngine(config)
            cdc_engine.start()

            # Keep main thread alive while Debezium engine runs
            try:
                while True:
                    pass
            except KeyboardInterrupt:
                logger.info("Shutting down due to user interrupt")

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()

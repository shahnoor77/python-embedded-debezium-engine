"""
Debezium Embedded Engine wrapper using JPype
"""
import logging
import os
import queue
import threading
from pathlib import Path
from typing import Dict, Any, Callable, Union
import json 

import jpype

from jpype.types import *

logger = logging.getLogger(__name__)


class DebeziumEmbeddedEngine:
    """Wrapper for Debezium Embedded Engine"""

    def __init__(self, config: Dict[str, Any], change_consumer: Callable):
        """
        Initialize Debezium Embedded Engine
        """
        self.config = config
        self.change_consumer = change_consumer
        self.engine = None
        self.executor = None
        self.running = False
        self.change_queue = queue.Queue(maxsize=10000)

        # Initialize JVM (must happen before JPackage calls in start())
        self._init_jvm()

    def _init_jvm(self):
        """Initialize JVM with Debezium libraries"""

        if jpype.isJVMStarted():
            logger.info("JVM already started")
            return

        # Get Debezium lib path
        lib_path = Path("/app/debezium/lib")
        if not lib_path.exists():
            lib_path = Path("debezium/lib")

        if not lib_path.exists():
            raise RuntimeError(f"Debezium libraries not found at {lib_path}")

        # Build classpath
        jars = list(lib_path.glob("*.jar"))
        classpath = ":".join([str(jar) for jar in jars])

        logger.info(f"Starting JVM with {len(jars)} Debezium JARs")
        logger.info(f"Classpath: {classpath}")

        # Start JVM
        jpype.startJVM(
            "-Djava.class.path=" + classpath,
            convertStrings=True
        )

        logger.info("JVM started successfully")

    def start(self):
        """Start the Debezium engine"""
        logger.info("Starting Debezium Embedded Engine")

        try:
            # JPackage and JClass are imported here, ensuring the JVM is ready
            from jpype import JPackage, JClass

            java = JPackage('java')
            debezium = JPackage('io').debezium 

            # Java classes
            Properties = java.util.Properties
            Executors = java.util.concurrent.Executors
            EmbeddedEngine = debezium.embedded.EmbeddedEngine
            
            # Use the known working format class: Json
            FormatClass = debezium.engine.format.Json 

            # Create Java Properties
            props = Properties()

            # 1. Add provided config (Keys are assumed to be in dot.notation)
            for key, value in self.config.items():
                
                if value is not None:
                    # Convert all Python booleans/numbers to strings for Java properties
                    props.setProperty(str(key), str(value))
                else:
                    logger.warning(f"Skipping Debezium config property '{key}' with value 'None'")

            # 2. Add JsonConverter configs (Required for internal consistency)
            props.setProperty('key.converter', 'org.apache.kafka.connect.json.JsonConverter')
            props.setProperty('value.converter', 'org.apache.kafka.connect.json.JsonConverter')
            props.setProperty('key.converter.schemas.enable', 'true')
            props.setProperty('value.converter.schemas.enable', 'true')
            
            # 3. Set 'name' as a mandatory property if missing
            if not props.getProperty('name'):
                props.setProperty('name', 'debezium-embedded-engine')

            # Configure engine
            # Pass the JsonFormat class
            engine_builder = EmbeddedEngine.create(JClass(FormatClass)) 

            self.engine = (
                engine_builder
                .using(props)
                .notifying(self._create_change_consumer())
                .build()
            )

            # Executor thread
            self.executor = Executors.newSingleThreadExecutor()

            self.running = True
            self.executor.submit(self.engine)

            # Start Python-side consumer thread
            self.consumer_thread = threading.Thread(
                target=self._process_changes,
                daemon=True
            )
            self.consumer_thread.start()

            logger.info("Debezium Embedded Engine started successfully")

        except Exception as e:
            logger.error(f"Failed to start Debezium engine: {e}")
            logger.exception(e)
            raise 

    def _create_change_consumer(self):
        """Create a Java Consumer for the incoming change events"""
        from jpype import JPackage, JImplements, JOverride

        Consumer = JPackage('java').util.function.Consumer

        @JImplements(Consumer)
        class ChangeConsumer:
            def __init__(self, queue_ref):
                self.queue = queue_ref

            @JOverride
            def accept(self, event): 
                """Extract raw JSON string payload and metadata."""
                try:
                    #  Use event.value() to get the JSON string payload directly.
                    # This bypasses the structured Java object conversion failure.
                    json_payload = event.value()
                    
                    if json_payload is not None:
                        logger.info(f"Received JSON payload, length: {len(json_payload)}")
                        # Queue the raw string and metadata for Python processing
                        self.queue.put({
                            'payload': json_payload,
                            'destination': str(event.destination()) if event.destination() else None, 
                        })
                            
                except Exception as e:
                    logger.error(f"Error in change consumer: {e}") 
        
        # I removed the _convert_record and _java_to_python methods because the 
        # data is raw JSON string handled by Python's json library.
        # user can add them if there is another pattern used for data

        return ChangeConsumer(self.change_queue)

    def _process_changes(self):
        """Background thread to process queued change events"""
        logger.info("Change consumer thread started")

        while self.running:
            try:
                # Use a small timeout to allow for clean shutdown
                queued_item = self.change_queue.get(timeout=1.0) 
                
                if queued_item:
                    # CRITICAL: Parse the raw JSON string in Python
                    raw_payload = queued_item.get('payload')
                    
                    # The value contains both key and value embedded in the JSON structure
                    parsed_record = json.loads(raw_payload)
                    logger.info(f"Successfully parsed JSON and calling consumer for topic: {parsed_record.get('topic')}")
                    # Pass the fully parsed Python dict to the user's consumer
                    self.change_consumer(parsed_record)

            except queue.Empty:
                continue
            except json.JSONDecodeError as e:
                logger.error(f"Error decoding JSON payload: {e}")
            except Exception as e:
                logger.error(f"Error processing change: {e}")

    def stop(self):
        """Stop the Debezium engine"""
        logger.info("Stopping Debezium Embedded Engine")

        self.running = False

        if self.engine:
            try:
                self.engine.close()
            except Exception as e:
                logger.error(f"Error closing engine: {e}")

        if self.executor:
            try:
                self.executor.shutdown()
            except Exception as e:
                logger.error(f"Error shutting down executor: {e}")

        logger.info("Debezium Embedded Engine stopped")

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        return False
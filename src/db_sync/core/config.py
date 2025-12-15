"""
Configuration management for DB Sync system
"""
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings


class DatabaseConfig(BaseModel):
    """Database connection configuration"""
    type: str
    host: str
    port: int
    database: str
    username: str
    password: str
    ssl_mode: Optional[str] = "prefer"
    schema: Optional[str] = "public"
    server_id: Optional[int] = None
    replica_set: Optional[str] = None


class KafkaConfig(BaseModel):
    """Kafka configuration"""
    bootstrap_servers: str
    group_id: str
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False
    max_poll_records: int = 500


class DebeziumConfig(BaseModel):
    """Debezium connector configuration"""
    connector_class: str
    server_name: str
    topic_prefix: str
    slot_name: Optional[str] = "debezium_slot"
    plugin_name: Optional[str] = "pgoutput"
    publication_name: Optional[str] = "dbz_publication"
    snapshot_mode: str = "initial"
    snapshot_locking_mode: str = "minimal"
    offset_storage:str =  "org.apache.kafka.connect.storage.FileOffsetBackingStore"
    offset_storage_file_filename: str =  "offsets.dat"
    offset_flush_interval_ms:int =  10000
    schema_history_internal:str = "io.debezium.storage.file.history.FileSchemaHistory"
    schema_history_internal_file_filename:str = "schema-history.dat" # can directly load from config.yaml
    decimal_handling_mode:str = "double"
    time_precision_mode:str = "adaptive"
    include_schema_changes:bool= True
    topic_prefix:str = "dbsync"


class InitialLoadConfig(BaseModel):
    """Initial load configuration"""
    enabled: bool = True
    batch_size: int = 1000
    parallel_tables: int = 4
    exclude_tables: List[str] = Field(default_factory=list)
    include_tables: List[str] = Field(default_factory=list)


class CDCConfig(BaseModel):
    """CDC configuration"""
    enabled: bool = True
    auto_create_tables: bool = True
    auto_detect_schema_changes: bool = True
    apply_deletes: bool = True
    conflict_resolution: str = "source_wins"


class SyncConfig(BaseModel):
    """Synchronization configuration"""
    initial_load: InitialLoadConfig
    cdc: CDCConfig
    schema_mapping: Dict[str, str] = Field(default_factory=dict)
    column_mapping: Dict[str, Dict[str, str]] = Field(default_factory=dict)


class MonitoringConfig(BaseModel):
    """Monitoring configuration"""
    enable_metrics: bool = True
    metrics_port: int = 9090
    health_check_port: int = 8080


class LoggingConfig(BaseModel):
    """Logging configuration"""
    level: str = "INFO"
    format: str = "json"
    file: str = "/app/logs/db-sync.log"
    max_bytes: int = 10485760
    backup_count: int = 5


class PerformanceConfig(BaseModel):
    """Performance configuration"""
    max_workers: int = 10
    queue_size: int = 10000
    batch_timeout_seconds: int = 5
    connection_pool_size: int = 20
    retry_attempts: int = 3
    retry_delay_seconds: int = 5


class StateConfig(BaseModel):
    """State management configuration"""
    storage_path: str = "/app/data/state"
    checkpoint_interval_seconds: int = 60
    offset_storage_path: str = "/app/data/offsets"


class Config(BaseSettings):
    """Main configuration class"""
    source: DatabaseConfig
    target: DatabaseConfig
    kafka: KafkaConfig
    debezium: DebeziumConfig
    sync: SyncConfig
    monitoring: MonitoringConfig
    logging: LoggingConfig
    performance: PerformanceConfig
    state: StateConfig

    @classmethod
    def from_yaml(cls, config_path: str) -> "Config":
        """Load configuration from YAML file"""
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(path, "r") as f:
            config_dict = yaml.safe_load(f)
        
        return cls(**config_dict)

    def get_source_connection_string(self) -> str:
        """Generate source database connection string"""
        return self._build_connection_string(self.source)

    def get_target_connection_string(self) -> str:
        """Generate target database connection string"""
        return self._build_connection_string(self.target)

    @staticmethod
    def _build_connection_string(db_config: DatabaseConfig) -> str:
        """Build database connection string based on type"""
        if db_config.type == "postgresql":
            return (
                f"postgresql://{db_config.username}:{db_config.password}"
                f"@{db_config.host}:{db_config.port}/{db_config.database}"
            )
        elif db_config.type == "mysql":
            return (
                f"mysql+mysqlconnector://{db_config.username}:{db_config.password}"
                f"@{db_config.host}:{db_config.port}/{db_config.database}"
            )
        elif db_config.type == "mongodb":
            return (
                f"mongodb://{db_config.username}:{db_config.password}"
                f"@{db_config.host}:{db_config.port}/{db_config.database}"
            )
        else:
            raise ValueError(f"Unsupported database type: {db_config.type}")
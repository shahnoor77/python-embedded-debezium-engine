"""
Connector factory for creating database-specific connectors
"""
from typing import Dict, Any

from db_sync.connectors.base import BaseConnector
from db_sync.connectors.postgres import PostgreSQLConnector
from db_sync.connectors.mysql import MySQLConnector


class ConnectorFactory:
    """Factory for creating database connectors"""

    _connectors = {
        'postgresql': PostgreSQLConnector,
        'postgres': PostgreSQLConnector,
        'mysql': MySQLConnector,
        # Add more connectors as they are implemented
        # 'mongodb': MongoDBConnector,
    }

    @classmethod
    def create_connector(cls, db_type: str, config: Dict[str, Any]) -> BaseConnector:
        """
        Create a database connector based on type
        
        Args:
            db_type: Type of database (postgresql, mysql, mongodb, etc.)
            config: Database configuration dictionary
            
        Returns:
            Connector instance
            
        Raises:
            ValueError: If database type is not supported
        """
        db_type_lower = db_type.lower()
        
        if db_type_lower not in cls._connectors:
            raise ValueError(
                f"Unsupported database type: {db_type}. "
                f"Supported types: {', '.join(cls._connectors.keys())}"
            )
        
        connector_class = cls._connectors[db_type_lower]
        return connector_class(config)

    @classmethod
    def register_connector(cls, db_type: str, connector_class: type) -> None:
        """
        Register a new connector type
        
        Args:
            db_type: Type identifier for the database
            connector_class: Connector class to register
        """
        cls._connectors[db_type.lower()] = connector_class

    @classmethod
    def get_supported_types(cls) -> list:
        """Get list of supported database types"""
        return list(cls._connectors.keys())
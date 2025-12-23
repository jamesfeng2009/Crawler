"""
Database factory for creating appropriate database managers.
"""

from typing import Union
from config import DatabaseConfig
from memory_price_monitor.data.database import DatabaseManager
from memory_price_monitor.data.sqlite_database import SQLiteDatabaseManager
from memory_price_monitor.utils.errors import DatabaseError


class DatabaseFactory:
    """Factory class for creating database managers."""
    
    @staticmethod
    def create_database_manager(config: DatabaseConfig) -> Union[DatabaseManager, SQLiteDatabaseManager]:
        """
        Create appropriate database manager based on configuration.
        
        Args:
            config: Database configuration
            
        Returns:
            Database manager instance
            
        Raises:
            DatabaseError: If unsupported database type is specified
        """
        if config.db_type.lower() == "sqlite":
            return SQLiteDatabaseManager(config.sqlite_path)
        elif config.db_type.lower() == "postgresql":
            return DatabaseManager(config)
        else:
            raise DatabaseError(
                f"Unsupported database type: {config.db_type}",
                {"supported_types": ["sqlite", "postgresql"]}
            )
    
    @staticmethod
    def get_database_type(config: DatabaseConfig) -> str:
        """
        Get the database type from configuration.
        
        Args:
            config: Database configuration
            
        Returns:
            Database type string
        """
        return config.db_type.lower()
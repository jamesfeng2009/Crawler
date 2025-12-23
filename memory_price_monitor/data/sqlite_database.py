"""
SQLite database connection and management utilities.
"""

import sqlite3
from typing import Optional, Any, Dict, List
from contextlib import contextmanager
import logging
from pathlib import Path
import json

from memory_price_monitor.utils.errors import DatabaseError


logger = logging.getLogger(__name__)


class SQLiteDatabaseManager:
    """Manages SQLite database connections and operations."""
    
    def __init__(self, database_path: str = "data/memory_price_monitor.db"):
        """
        Initialize SQLite database manager.
        
        Args:
            database_path: Path to SQLite database file
        """
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        
    def initialize(self) -> None:
        """Initialize the database and create tables."""
        try:
            self.create_tables()
            logger.info(f"SQLite database initialized at {self.database_path}")
        except Exception as e:
            raise DatabaseError(
                "Failed to initialize SQLite database",
                {"error": str(e)}
            )
    
    @contextmanager
    def get_connection(self):
        """
        Get a database connection.
        
        Yields:
            SQLite database connection
        """
        conn = None
        try:
            conn = sqlite3.connect(
                str(self.database_path),
                timeout=30.0,
                check_same_thread=False
            )
            # Enable foreign key constraints
            conn.execute("PRAGMA foreign_keys = ON")
            # Use row factory for dict-like access
            conn.row_factory = sqlite3.Row
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            raise DatabaseError(
                "SQLite database operation failed",
                {"error": str(e)}
            )
        finally:
            if conn:
                conn.close()
    
    @contextmanager
    def get_cursor(self):
        """
        Get a database cursor with automatic connection management.
        
        Yields:
            SQLite database cursor
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                yield cursor
                conn.commit()
            except Exception as e:
                conn.rollback()
                raise e
            finally:
                cursor.close()
    
    def execute_query(
        self,
        query: str,
        params: Optional[tuple] = None,
        fetch: bool = True
    ) -> Optional[List[sqlite3.Row]]:
        """
        Execute a SQL query.
        
        Args:
            query: SQL query string
            params: Query parameters
            fetch: Whether to fetch results
            
        Returns:
            Query results if fetch=True, None otherwise
        """
        with self.get_cursor() as cursor:
            cursor.execute(query, params or ())
            if fetch:
                return cursor.fetchall()
            return None
    
    def execute_many(
        self,
        query: str,
        params_list: List[tuple]
    ) -> None:
        """
        Execute a query with multiple parameter sets.
        
        Args:
            query: SQL query string
            params_list: List of parameter tuples
        """
        with self.get_cursor() as cursor:
            cursor.executemany(query, params_list)
    
    def create_tables(self) -> None:
        """Create database tables if they don't exist."""
        
        # Products table
        create_products_table = """
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            product_id TEXT NOT NULL,
            brand TEXT NOT NULL,
            model TEXT NOT NULL,
            capacity TEXT NOT NULL,
            frequency TEXT,
            type TEXT NOT NULL,
            url TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source, product_id)
        );
        """
        
        # Price records table
        create_price_records_table = """
        CREATE TABLE IF NOT EXISTS price_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            current_price REAL NOT NULL CHECK (current_price > 0),
            original_price REAL,
            recorded_at TIMESTAMP NOT NULL,
            metadata TEXT,  -- JSON string
            FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
        );
        """
        
        # Crawl logs table
        create_crawl_logs_table = """
        CREATE TABLE IF NOT EXISTS crawl_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            status TEXT NOT NULL,
            products_found INTEGER,
            products_saved INTEGER,
            errors TEXT,
            started_at TIMESTAMP NOT NULL,
            completed_at TIMESTAMP,
            duration_seconds INTEGER
        );
        """
        
        # Create indexes
        create_indexes = [
            "CREATE INDEX IF NOT EXISTS idx_products_brand ON products(brand);",
            "CREATE INDEX IF NOT EXISTS idx_products_specs ON products(capacity, type);",
            "CREATE INDEX IF NOT EXISTS idx_products_source ON products(source);",
            "CREATE INDEX IF NOT EXISTS idx_products_created ON products(created_at);",
            "CREATE INDEX IF NOT EXISTS idx_price_records_product ON price_records(product_id);",
            "CREATE INDEX IF NOT EXISTS idx_price_records_time ON price_records(recorded_at);",
            "CREATE INDEX IF NOT EXISTS idx_price_records_product_time ON price_records(product_id, recorded_at);",
            "CREATE INDEX IF NOT EXISTS idx_price_records_price ON price_records(current_price);",
            "CREATE INDEX IF NOT EXISTS idx_crawl_logs_source ON crawl_logs(source);",
            "CREATE INDEX IF NOT EXISTS idx_crawl_logs_status ON crawl_logs(status);",
            "CREATE INDEX IF NOT EXISTS idx_crawl_logs_started ON crawl_logs(started_at);"
        ]
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Create tables
                cursor.execute(create_products_table)
                cursor.execute(create_price_records_table)
                cursor.execute(create_crawl_logs_table)
                
                # Create indexes
                for index_sql in create_indexes:
                    cursor.execute(index_sql)
                
                conn.commit()
                
            logger.info("SQLite database tables created successfully")
        except Exception as e:
            raise DatabaseError(
                "Failed to create SQLite database tables",
                {"error": str(e)}
            )
    
    @contextmanager
    def transaction(self):
        """
        Context manager for database transactions.
        
        Usage:
            with db_manager.transaction():
                # Database operations here
                # Will be committed automatically or rolled back on exception
        """
        with self.get_connection() as conn:
            try:
                yield conn
                conn.commit()
            except Exception as e:
                conn.rollback()
                raise e
    
    def execute_in_transaction(self, operations: List[tuple]) -> List[Any]:
        """
        Execute multiple operations in a single transaction.
        
        Args:
            operations: List of (query, params, fetch) tuples
            
        Returns:
            List of results for operations that fetch data
        """
        results = []
        
        with self.transaction() as conn:
            cursor = conn.cursor()
            try:
                for query, params, fetch in operations:
                    cursor.execute(query, params or ())
                    if fetch:
                        results.append(cursor.fetchall())
                    else:
                        results.append(None)
            finally:
                cursor.close()
        
        return results
    
    def get_connection_stats(self) -> Dict[str, Any]:
        """
        Get database statistics.
        
        Returns:
            Dictionary with database stats
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Get table counts
                cursor.execute("SELECT COUNT(*) FROM products")
                products_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM price_records")
                price_records_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM crawl_logs")
                crawl_logs_count = cursor.fetchone()[0]
                
                # Get database size
                cursor.execute("PRAGMA page_count")
                page_count = cursor.fetchone()[0]
                cursor.execute("PRAGMA page_size")
                page_size = cursor.fetchone()[0]
                db_size = page_count * page_size
                
                return {
                    "status": "active",
                    "database_path": str(self.database_path),
                    "database_size_bytes": db_size,
                    "products_count": products_count,
                    "price_records_count": price_records_count,
                    "crawl_logs_count": crawl_logs_count
                }
        except Exception as e:
            return {"status": "error", "error": str(e)}
    
    def health_check(self) -> bool:
        """
        Perform a health check on the database connection.
        
        Returns:
            True if database is healthy, False otherwise
        """
        try:
            result = self.execute_query("SELECT 1")
            return result is not None and len(result) > 0
        except Exception as e:
            logger.error(f"SQLite database health check failed: {e}")
            return False
    
    def vacuum(self) -> None:
        """
        Vacuum the database to reclaim space and optimize performance.
        """
        try:
            with self.get_connection() as conn:
                conn.execute("VACUUM")
            logger.info("Database vacuum completed successfully")
        except Exception as e:
            logger.error(f"Database vacuum failed: {e}")
            raise DatabaseError("Failed to vacuum database", {"error": str(e)})
    
    def backup(self, backup_path: str) -> None:
        """
        Create a backup of the database.
        
        Args:
            backup_path: Path for the backup file
        """
        try:
            backup_path = Path(backup_path)
            backup_path.parent.mkdir(parents=True, exist_ok=True)
            
            with self.get_connection() as source:
                with sqlite3.connect(str(backup_path)) as backup:
                    source.backup(backup)
            
            logger.info(f"Database backup created at {backup_path}")
        except Exception as e:
            logger.error(f"Database backup failed: {e}")
            raise DatabaseError("Failed to backup database", {"error": str(e)})
    def close(self) -> None:
        """Close database connections (no-op for SQLite as connections are per-operation)."""
        logger.info("SQLite database manager closed (connections are per-operation)")
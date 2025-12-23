"""
Database connection and management utilities.
"""

import psycopg2
from psycopg2 import pool, extras
from typing import Optional, Any, Dict, List
from contextlib import contextmanager
import logging

from config import DatabaseConfig
from memory_price_monitor.utils.errors import DatabaseError


logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages database connections and operations."""
    
    def __init__(self, config: DatabaseConfig):
        """
        Initialize database manager with configuration.
        
        Args:
            config: Database configuration
        """
        self.config = config
        self._pool: Optional[pool.SimpleConnectionPool] = None
        
    def initialize(self) -> None:
        """Initialize the connection pool."""
        try:
            self._pool = pool.SimpleConnectionPool(
                minconn=1,
                maxconn=self.config.pool_size,
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.username,
                password=self.config.password
            )
            logger.info("Database connection pool initialized")
        except Exception as e:
            raise DatabaseError(
                "Failed to initialize database connection pool",
                {"error": str(e)}
            )
    
    def close(self) -> None:
        """Close all connections in the pool."""
        if self._pool:
            self._pool.closeall()
            logger.info("Database connection pool closed")
    
    @contextmanager
    def get_connection(self):
        """
        Get a database connection from the pool.
        
        Yields:
            Database connection
        """
        if not self._pool:
            raise DatabaseError("Database pool not initialized")
        
        conn = None
        try:
            conn = self._pool.getconn()
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            raise DatabaseError(
                "Database operation failed",
                {"error": str(e)}
            )
        finally:
            if conn:
                self._pool.putconn(conn)
    
    @contextmanager
    def get_cursor(self, cursor_factory=None):
        """
        Get a database cursor with automatic connection management.
        
        Args:
            cursor_factory: Optional cursor factory (e.g., RealDictCursor)
            
        Yields:
            Database cursor
        """
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=cursor_factory)
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
    ) -> Optional[List[tuple]]:
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
            cursor.execute(query, params)
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
        create_products_table = """
        CREATE TABLE IF NOT EXISTS products (
            id SERIAL PRIMARY KEY,
            source VARCHAR(50) NOT NULL,
            product_id VARCHAR(255) NOT NULL,
            brand VARCHAR(100) NOT NULL,
            model VARCHAR(255) NOT NULL,
            capacity VARCHAR(50) NOT NULL,
            frequency VARCHAR(50),
            type VARCHAR(50) NOT NULL,
            url TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source, product_id)
        );
        
        CREATE INDEX IF NOT EXISTS idx_products_brand ON products(brand);
        CREATE INDEX IF NOT EXISTS idx_products_specs ON products(capacity, type);
        CREATE INDEX IF NOT EXISTS idx_products_source ON products(source);
        CREATE INDEX IF NOT EXISTS idx_products_created ON products(created_at);
        """
        
        create_price_records_table = """
        CREATE TABLE IF NOT EXISTS price_records (
            id SERIAL PRIMARY KEY,
            product_id INTEGER REFERENCES products(id) ON DELETE CASCADE,
            current_price DECIMAL(10, 2) NOT NULL,
            original_price DECIMAL(10, 2),
            recorded_at TIMESTAMP NOT NULL,
            metadata JSONB,
            CONSTRAINT price_positive CHECK (current_price > 0)
        );
        
        CREATE INDEX IF NOT EXISTS idx_price_records_product ON price_records(product_id);
        CREATE INDEX IF NOT EXISTS idx_price_records_time ON price_records(recorded_at);
        CREATE INDEX IF NOT EXISTS idx_price_records_product_time ON price_records(product_id, recorded_at);
        CREATE INDEX IF NOT EXISTS idx_price_records_price ON price_records(current_price);
        """
        
        create_crawl_logs_table = """
        CREATE TABLE IF NOT EXISTS crawl_logs (
            id SERIAL PRIMARY KEY,
            source VARCHAR(50) NOT NULL,
            status VARCHAR(20) NOT NULL,
            products_found INTEGER,
            products_saved INTEGER,
            errors TEXT,
            started_at TIMESTAMP NOT NULL,
            completed_at TIMESTAMP,
            duration_seconds INTEGER
        );
        
        CREATE INDEX IF NOT EXISTS idx_crawl_logs_source ON crawl_logs(source);
        CREATE INDEX IF NOT EXISTS idx_crawl_logs_status ON crawl_logs(status);
        CREATE INDEX IF NOT EXISTS idx_crawl_logs_started ON crawl_logs(started_at);
        """
        
        try:
            self.execute_query(create_products_table, fetch=False)
            self.execute_query(create_price_records_table, fetch=False)
            self.execute_query(create_crawl_logs_table, fetch=False)
            logger.info("Database tables created successfully")
        except Exception as e:
            raise DatabaseError(
                "Failed to create database tables",
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
                    cursor.execute(query, params)
                    if fetch:
                        results.append(cursor.fetchall())
                    else:
                        results.append(None)
            finally:
                cursor.close()
        
        return results
    
    def get_connection_stats(self) -> Dict[str, Any]:
        """
        Get connection pool statistics.
        
        Returns:
            Dictionary with connection pool stats
        """
        if not self._pool:
            return {"status": "not_initialized"}
        
        return {
            "status": "active",
            "minconn": self._pool.minconn,
            "maxconn": self._pool.maxconn,
            "closed": self._pool.closed
        }
    
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
            logger.error(f"Database health check failed: {e}")
            return False
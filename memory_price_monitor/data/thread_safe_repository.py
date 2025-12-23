"""
Thread-safe repository for concurrent crawler operations.
Extends the existing PriceRepository with thread-safety mechanisms.
"""

import threading
import queue
import time
from typing import List, Optional, Dict, Any, Union, Set
from datetime import datetime, date
from decimal import Decimal
from contextlib import contextmanager
import logging

from memory_price_monitor.data.models import StandardizedProduct, PriceRecord
from memory_price_monitor.data.repository import PriceRepository
from memory_price_monitor.data.database import DatabaseManager
from memory_price_monitor.data.sqlite_database import SQLiteDatabaseManager
from memory_price_monitor.concurrent.thread_safe import ThreadSafeCounter, ThreadSafeSet
from memory_price_monitor.utils.errors import DatabaseError


logger = logging.getLogger(__name__)


class ConnectionPool:
    """Simple connection pool for database connections."""
    
    def __init__(self, db_manager: Union[DatabaseManager, SQLiteDatabaseManager], max_connections: int = 10):
        """
        Initialize connection pool.
        
        Args:
            db_manager: Database manager instance
            max_connections: Maximum number of connections in pool
        """
        self.db_manager = db_manager
        self.max_connections = max_connections
        self._pool = queue.Queue(maxsize=max_connections)
        self._created_connections = ThreadSafeCounter(0)
        self._active_connections = ThreadSafeCounter(0)
        self._lock = threading.Lock()
        self._is_sqlite = isinstance(db_manager, SQLiteDatabaseManager)
        
        # Pre-populate pool for PostgreSQL (SQLite uses per-operation connections)
        if not self._is_sqlite:
            self._initialize_pool()
    
    def _initialize_pool(self) -> None:
        """Initialize the connection pool with initial connections."""
        if self._is_sqlite:
            return  # SQLite doesn't need pre-created connections
        
        try:
            # For PostgreSQL, we can pre-create some connections
            initial_connections = min(2, self.max_connections)
            for _ in range(initial_connections):
                conn = self._create_connection()
                if conn:
                    self._pool.put_nowait(conn)
        except Exception as e:
            logger.warning(f"Failed to initialize connection pool: {e}")
    
    def _create_connection(self):
        """Create a new database connection."""
        if self._is_sqlite:
            # For SQLite, return the manager itself as it handles per-operation connections
            return self.db_manager
        else:
            # For PostgreSQL, we would create actual connections
            # This is simplified - in practice, you'd create actual psycopg2 connections
            return self.db_manager
    
    @contextmanager
    def get_connection(self):
        """
        Get a connection from the pool.
        
        Yields:
            Database connection or manager
        """
        connection = None
        try:
            # Try to get existing connection from pool
            try:
                connection = self._pool.get_nowait()
                self._active_connections.increment()
            except queue.Empty:
                # Create new connection if pool is empty and we haven't reached max
                if self._created_connections.get_value() < self.max_connections:
                    connection = self._create_connection()
                    if connection:
                        self._created_connections.increment()
                        self._active_connections.increment()
                else:
                    # Wait for available connection
                    connection = self._pool.get(timeout=30.0)
                    self._active_connections.increment()
            
            if not connection:
                raise DatabaseError("Failed to obtain database connection")
            
            yield connection
            
        except Exception as e:
            logger.error(f"Connection pool error: {e}")
            raise DatabaseError("Connection pool operation failed", {"error": str(e)})
        finally:
            if connection:
                self._active_connections.decrement()
                try:
                    self._pool.put_nowait(connection)
                except queue.Full:
                    # Pool is full, connection will be discarded
                    self._created_connections.decrement()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get connection pool statistics."""
        return {
            "max_connections": self.max_connections,
            "created_connections": self._created_connections.get_value(),
            "active_connections": self._active_connections.get_value(),
            "available_connections": self._pool.qsize(),
            "is_sqlite": self._is_sqlite
        }
    
    def close_all(self) -> None:
        """Close all connections in the pool."""
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                if hasattr(conn, 'close') and not self._is_sqlite:
                    conn.close()
            except queue.Empty:
                break
        
        self._created_connections.set_value(0)
        self._active_connections.set_value(0)


class ThreadSafePriceRepository(PriceRepository):
    """Thread-safe extension of PriceRepository for concurrent operations."""
    
    def __init__(self, db_manager: Union[DatabaseManager, SQLiteDatabaseManager], max_connections: int = 10):
        """
        Initialize thread-safe repository.
        
        Args:
            db_manager: Database manager instance
            max_connections: Maximum connections in pool
        """
        super().__init__(db_manager)
        
        # Thread safety components
        self._write_lock = threading.RLock()  # Reentrant lock for nested operations
        self._read_lock = threading.RLock()
        self._connection_pool = ConnectionPool(db_manager, max_connections)
        
        # Counters for monitoring
        self._write_operations = ThreadSafeCounter(0)
        self._read_operations = ThreadSafeCounter(0)
        self._failed_operations = ThreadSafeCounter(0)
        
        # Deduplication tracking
        self._processed_products = ThreadSafeSet()
        self._processing_products = ThreadSafeSet()  # Currently being processed
        
        # Batch processing
        self._batch_queue = queue.Queue()
        self._batch_size = 50
        self._batch_timeout = 5.0
        self._batch_thread = None
        self._batch_stop_event = threading.Event()
        
        # Start batch processing thread
        self._start_batch_processor()
    
    def _start_batch_processor(self) -> None:
        """Start the batch processing thread."""
        self._batch_thread = threading.Thread(
            target=self._batch_processor,
            name="BatchProcessor",
            daemon=True
        )
        self._batch_thread.start()
        logger.info("Batch processor thread started")
    
    def _batch_processor(self) -> None:
        """Process batched write operations."""
        batch = []
        last_flush = time.time()
        
        while not self._batch_stop_event.is_set():
            try:
                # Try to get item with timeout
                try:
                    item = self._batch_queue.get(timeout=1.0)
                    batch.append(item)
                except queue.Empty:
                    pass
                
                # Flush batch if it's full or timeout reached
                current_time = time.time()
                should_flush = (
                    len(batch) >= self._batch_size or
                    (batch and current_time - last_flush >= self._batch_timeout)
                )
                
                if should_flush and batch:
                    self._flush_batch(batch)
                    batch.clear()
                    last_flush = current_time
                    
            except Exception as e:
                logger.error(f"Batch processor error: {e}")
                # Clear problematic batch
                batch.clear()
                last_flush = time.time()
        
        # Flush remaining items on shutdown
        if batch:
            self._flush_batch(batch)
    
    def _flush_batch(self, batch: List[Dict[str, Any]]) -> None:
        """Flush a batch of operations to database."""
        if not batch:
            return
        
        try:
            with self._write_lock:
                with self._connection_pool.get_connection() as conn_manager:
                    # Group operations by type
                    products_to_save = []
                    
                    for item in batch:
                        if item['type'] == 'save_product':
                            products_to_save.append(item['product'])
                    
                    # Batch save products
                    if products_to_save:
                        self._batch_save_products(products_to_save, conn_manager)
                        
            logger.debug(f"Flushed batch of {len(batch)} operations")
            
        except Exception as e:
            logger.error(f"Failed to flush batch: {e}")
            self._failed_operations.increment(len(batch))
    
    def _batch_save_products(self, products: List[StandardizedProduct], conn_manager) -> None:
        """Save multiple products in a single transaction."""
        try:
            # Use transaction for batch operations
            if hasattr(conn_manager, 'transaction'):
                with conn_manager.transaction():
                    for product in products:
                        self._save_single_product(product, conn_manager)
            else:
                # Fallback for SQLite
                for product in products:
                    self._save_single_product(product, conn_manager)
                    
        except Exception as e:
            logger.error(f"Batch save failed: {e}")
            raise
    
    def _save_single_product(self, product: StandardizedProduct, conn_manager) -> None:
        """Save a single product using the provided connection manager."""
        # Create a temporary repository instance with the connection manager
        temp_repo = PriceRepository(conn_manager)
        temp_repo.save_price_record(product)
    
    def save_price_record(self, product: StandardizedProduct, use_batch: bool = True) -> bool:
        """
        Thread-safe save of price record.
        
        Args:
            product: Standardized product data
            use_batch: Whether to use batch processing (default: True)
            
        Returns:
            True if saved successfully, False otherwise
        """
        product_key = f"{product.source}:{product.product_id}"
        
        # Check if already processed to avoid duplicates
        if self._processed_products.contains(product_key):
            logger.debug(f"Product {product_key} already processed, skipping")
            return True
        
        # Check if currently being processed
        if not self._processing_products.add(product_key):
            logger.debug(f"Product {product_key} is being processed by another thread")
            return True
        
        try:
            if use_batch:
                # Add to batch queue
                batch_item = {
                    'type': 'save_product',
                    'product': product,
                    'timestamp': datetime.now()
                }
                self._batch_queue.put(batch_item, timeout=5.0)
                self._write_operations.increment()
                
                # Mark as processed
                self._processed_products.add(product_key)
                return True
            else:
                # Direct save with thread safety
                with self._write_lock:
                    with self._connection_pool.get_connection() as conn_manager:
                        temp_repo = PriceRepository(conn_manager)
                        result = temp_repo.save_price_record(product)
                        
                        if result:
                            self._write_operations.increment()
                            self._processed_products.add(product_key)
                        else:
                            self._failed_operations.increment()
                        
                        return result
                        
        except Exception as e:
            logger.error(f"Failed to save price record: {e}")
            self._failed_operations.increment()
            return False
        finally:
            # Remove from processing set
            self._processing_products.remove(product_key)
    
    def get_price_history(
        self, 
        product_id: str, 
        start_date: date, 
        end_date: date,
        source: str = None
    ) -> List[PriceRecord]:
        """
        Thread-safe get price history.
        
        Args:
            product_id: Product identifier
            start_date: Start date for history
            end_date: End date for history
            source: Optional source filter
            
        Returns:
            List of price records
        """
        with self._read_lock:
            try:
                with self._connection_pool.get_connection() as conn_manager:
                    temp_repo = PriceRepository(conn_manager)
                    result = temp_repo.get_price_history(product_id, start_date, end_date, source)
                    self._read_operations.increment()
                    return result
            except Exception as e:
                logger.error(f"Failed to get price history: {e}")
                self._failed_operations.increment()
                raise
    
    def get_weekly_data(self, week_offset: int = 0) -> List[PriceRecord]:
        """
        Thread-safe get weekly data.
        
        Args:
            week_offset: Week offset from current week
            
        Returns:
            List of price records for the week
        """
        with self._read_lock:
            try:
                with self._connection_pool.get_connection() as conn_manager:
                    temp_repo = PriceRepository(conn_manager)
                    result = temp_repo.get_weekly_data(week_offset)
                    self._read_operations.increment()
                    return result
            except Exception as e:
                logger.error(f"Failed to get weekly data: {e}")
                self._failed_operations.increment()
                raise
    
    def get_products_by_specs(
        self, 
        brand: str = None, 
        capacity: str = None,
        type: str = None
    ) -> List[StandardizedProduct]:
        """
        Thread-safe get products by specifications.
        
        Args:
            brand: Brand filter
            capacity: Capacity filter
            type: Memory type filter
            
        Returns:
            List of standardized products
        """
        with self._read_lock:
            try:
                with self._connection_pool.get_connection() as conn_manager:
                    # Build query with proper placeholders
                    placeholder = "?" if self.is_sqlite else "%s"
                    query = """
                    SELECT source, product_id, brand, model, capacity, frequency, type, url, created_at
                    FROM products
                    WHERE 1=1
                    """
                    
                    params = []
                    
                    if brand:
                        query += f" AND brand = {placeholder}"
                        params.append(brand)
                    
                    if capacity:
                        query += f" AND capacity = {placeholder}"
                        params.append(capacity)
                    
                    if type:
                        query += f" AND type = {placeholder}"
                        params.append(type)
                    
                    query += " ORDER BY brand, capacity, type"
                    
                    results = conn_manager.execute_query(query, tuple(params))
                    
                    products = []
                    for row in results or []:
                        # Get latest price for this product
                        latest_price = self._get_latest_price_direct(row[1], row[0], conn_manager)
                        
                        product = StandardizedProduct(
                            source=row[0],
                            product_id=row[1],
                            brand=row[2],
                            model=row[3],
                            capacity=row[4],
                            frequency=row[5] or "",
                            type=row[6],
                            current_price=latest_price.get('current_price', Decimal('0')),
                            original_price=latest_price.get('original_price', Decimal('0')),
                            url=row[7],
                            timestamp=latest_price.get('timestamp', row[8]),
                            metadata=latest_price.get('metadata', {})
                        )
                        products.append(product)
                    
                    self._read_operations.increment()
                    return products
                    
            except Exception as e:
                logger.error(f"Failed to get products by specs: {e}")
                self._failed_operations.increment()
                raise
    
    def export_data(
        self,
        format: str,
        start_date: date = None,
        end_date: date = None,
        brand: str = None,
        capacity: str = None,
        type: str = None
    ) -> str:
        """
        Thread-safe export data.
        
        Args:
            format: Export format ('csv' or 'json')
            start_date: Optional start date filter
            end_date: Optional end date filter
            brand: Optional brand filter
            capacity: Optional capacity filter
            type: Optional memory type filter
            
        Returns:
            Exported data as string
        """
        with self._read_lock:
            try:
                with self._connection_pool.get_connection() as conn_manager:
                    temp_repo = PriceRepository(conn_manager)
                    result = temp_repo.export_data(format, start_date, end_date, brand, capacity, type)
                    self._read_operations.increment()
                    return result
            except Exception as e:
                logger.error(f"Failed to export data: {e}")
                self._failed_operations.increment()
                raise
    
    def cleanup_old_data(self, retention_days: int = 365) -> int:
        """
        Thread-safe cleanup of old data.
        
        Args:
            retention_days: Number of days to retain data
            
        Returns:
            Number of records deleted
        """
        with self._write_lock:
            try:
                with self._connection_pool.get_connection() as conn_manager:
                    temp_repo = PriceRepository(conn_manager)
                    result = temp_repo.cleanup_old_data(retention_days)
                    self._write_operations.increment()
                    return result
            except Exception as e:
                logger.error(f"Failed to cleanup old data: {e}")
                self._failed_operations.increment()
                raise
    
    def get_database_stats(self) -> Dict[str, Any]:
        """
        Thread-safe get database statistics.
        
        Returns:
            Dictionary with database statistics including thread-safety metrics
        """
        with self._read_lock:
            try:
                with self._connection_pool.get_connection() as conn_manager:
                    temp_repo = PriceRepository(conn_manager)
                    base_stats = temp_repo.get_database_stats()
                    
                    # Add thread-safety specific stats
                    thread_stats = {
                        "thread_safety": {
                            "write_operations": self._write_operations.get_value(),
                            "read_operations": self._read_operations.get_value(),
                            "failed_operations": self._failed_operations.get_value(),
                            "processed_products": self._processed_products.size(),
                            "currently_processing": self._processing_products.size(),
                            "batch_queue_size": self._batch_queue.qsize(),
                            "connection_pool": self._connection_pool.get_stats()
                        }
                    }
                    
                    base_stats.update(thread_stats)
                    self._read_operations.increment()
                    return base_stats
                    
            except Exception as e:
                logger.error(f"Failed to get database stats: {e}")
                self._failed_operations.increment()
                raise
    
    def get_product_by_id(self, product_id: str, source: str) -> Optional[StandardizedProduct]:
        """
        Thread-safe get product by ID.
        
        Args:
            product_id: Product identifier
            source: Product source
            
        Returns:
            StandardizedProduct if found, None otherwise
        """
        with self._read_lock:
            try:
                with self._connection_pool.get_connection() as conn_manager:
                    temp_repo = PriceRepository(conn_manager)
                    result = temp_repo.get_product_by_id(product_id, source)
                    self._read_operations.increment()
                    return result
            except Exception as e:
                logger.error(f"Failed to get product by ID: {e}")
                self._failed_operations.increment()
                raise
    
    def wait_for_batch_completion(self, timeout: float = 30.0) -> bool:
        """
        Wait for all batched operations to complete.
        
        Args:
            timeout: Maximum time to wait in seconds
            
        Returns:
            True if all operations completed, False if timeout
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            if self._batch_queue.empty() and self._processing_products.is_empty():
                return True
            time.sleep(0.1)
        
        return False
    
    def force_batch_flush(self) -> None:
        """Force immediate flush of batch queue."""
        batch = []
        
        # Drain the queue
        try:
            while True:
                item = self._batch_queue.get_nowait()
                batch.append(item)
        except queue.Empty:
            pass
        
        # Flush the batch
        if batch:
            self._flush_batch(batch)
    
    def get_thread_safety_stats(self) -> Dict[str, Any]:
        """
        Get detailed thread safety statistics.
        
        Returns:
            Dictionary with thread safety metrics
        """
        return {
            "operations": {
                "write_operations": self._write_operations.get_value(),
                "read_operations": self._read_operations.get_value(),
                "failed_operations": self._failed_operations.get_value(),
                "success_rate": self._calculate_success_rate()
            },
            "deduplication": {
                "processed_products": self._processed_products.size(),
                "currently_processing": self._processing_products.size()
            },
            "batching": {
                "queue_size": self._batch_queue.qsize(),
                "batch_size": self._batch_size,
                "batch_timeout": self._batch_timeout,
                "batch_thread_alive": self._batch_thread.is_alive() if self._batch_thread else False
            },
            "connection_pool": self._connection_pool.get_stats()
        }
    
    def _calculate_success_rate(self) -> float:
        """Calculate operation success rate."""
        total_ops = self._write_operations.get_value() + self._read_operations.get_value()
        if total_ops == 0:
            return 100.0
        
        failed_ops = self._failed_operations.get_value()
        return ((total_ops - failed_ops) / total_ops) * 100.0
    
    def _get_latest_price_direct(self, product_id: str, source: str, conn_manager) -> Dict[str, Any]:
        """
        Get the latest price record for a product using direct connection.
        
        Args:
            product_id: Product identifier
            source: Product source
            conn_manager: Database connection manager
            
        Returns:
            Dictionary with latest price information
        """
        placeholder = "?" if self.is_sqlite else "%s"
        query = f"""
        SELECT pr.current_price, pr.original_price, pr.recorded_at, pr.metadata
        FROM price_records pr
        JOIN products p ON pr.product_id = p.id
        WHERE p.product_id = {placeholder} AND p.source = {placeholder}
        ORDER BY pr.recorded_at DESC
        LIMIT 1
        """
        
        result = conn_manager.execute_query(query, (product_id, source))
        
        if result:
            row = result[0]
            metadata = row[3] or {}
            
            # Handle JSON string metadata (for SQLite compatibility)
            if isinstance(metadata, str):
                import json
                try:
                    metadata = json.loads(metadata)
                except (json.JSONDecodeError, TypeError):
                    metadata = {}
            
            # Handle timestamp conversion from string (SQLite compatibility)
            timestamp = row[2]
            if isinstance(timestamp, str):
                from datetime import datetime
                try:
                    timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                except ValueError:
                    timestamp = datetime.now()
            
            return {
                'current_price': Decimal(str(row[0])) if row[0] else Decimal('0'),
                'original_price': Decimal(str(row[1])) if row[1] else Decimal('0'),
                'timestamp': timestamp,
                'metadata': metadata
            }
        
        return {
            'current_price': Decimal('0'),
            'original_price': Decimal('0'),
            'timestamp': datetime.now(),
            'metadata': {}
        }
    
    def close(self) -> None:
        """Close the thread-safe repository and cleanup resources."""
        logger.info("Shutting down thread-safe repository")
        
        # Stop batch processor
        if self._batch_thread and self._batch_thread.is_alive():
            self._batch_stop_event.set()
            self._batch_thread.join(timeout=10.0)
        
        # Force flush remaining batches
        self.force_batch_flush()
        
        # Close connection pool
        self._connection_pool.close_all()
        
        # Clear tracking sets
        self._processed_products.clear()
        self._processing_products.clear()
        
        logger.info("Thread-safe repository shutdown complete")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
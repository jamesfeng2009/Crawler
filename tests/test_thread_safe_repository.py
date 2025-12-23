"""
Unit tests for ThreadSafePriceRepository.
"""

import pytest
import threading
import time
from datetime import datetime, date
from decimal import Decimal
from unittest.mock import Mock, patch

from memory_price_monitor.data.thread_safe_repository import ThreadSafePriceRepository, ConnectionPool
from memory_price_monitor.data.sqlite_database import SQLiteDatabaseManager
from memory_price_monitor.data.models import StandardizedProduct, PriceRecord
from memory_price_monitor.utils.errors import DatabaseError


class TestConnectionPool:
    """Test connection pool functionality."""
    
    def test_connection_pool_creation(self):
        """Test connection pool creation."""
        db_manager = Mock()
        pool = ConnectionPool(db_manager, max_connections=5)
        
        assert pool.max_connections == 5
        assert pool._created_connections.get_value() == 0
        assert pool._active_connections.get_value() == 0
    
    def test_connection_pool_stats(self):
        """Test connection pool statistics."""
        db_manager = Mock()
        pool = ConnectionPool(db_manager, max_connections=3)
        
        stats = pool.get_stats()
        assert stats['max_connections'] == 3
        assert stats['created_connections'] == 0
        assert stats['active_connections'] == 0
        assert 'is_sqlite' in stats


class TestThreadSafePriceRepository:
    """Test thread-safe repository functionality."""
    
    def setup_method(self):
        """Set up test environment."""
        # Use a temporary file for SQLite to ensure shared state
        import tempfile
        import os
        
        # Create a temporary file for the database
        self.temp_db_file = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db_file.close()
        
        self.db_manager = SQLiteDatabaseManager(self.temp_db_file.name)
        self.db_manager.initialize()
        
        self.repository = ThreadSafePriceRepository(self.db_manager, max_connections=3)
    
    def teardown_method(self):
        """Clean up test environment."""
        if hasattr(self, 'repository'):
            self.repository.close()
        
        # Clean up temporary database file
        if hasattr(self, 'temp_db_file'):
            import os
            try:
                os.unlink(self.temp_db_file.name)
            except FileNotFoundError:
                pass
    
    def test_repository_initialization(self):
        """Test repository initialization."""
        assert self.repository is not None
        assert self.repository._write_operations.get_value() == 0
        assert self.repository._read_operations.get_value() == 0
        assert self.repository._failed_operations.get_value() == 0
    
    def test_single_product_save(self):
        """Test saving a single product."""
        product = StandardizedProduct(
            source="test",
            product_id="test_001",
            brand="TestBrand",
            model="TestModel",
            capacity="16GB",
            frequency="3200MHz",
            type="DDR4",
            current_price=Decimal("100.00"),
            original_price=Decimal("120.00"),
            url="http://test.com/product1",
            timestamp=datetime.now(),
            metadata={"test": True}
        )
        
        result = self.repository.save_price_record(product, use_batch=False)
        assert result is True
        assert self.repository._write_operations.get_value() == 1
    
    def test_batch_product_save(self):
        """Test batch saving of products."""
        products = []
        for i in range(5):
            product = StandardizedProduct(
                source="test",
                product_id=f"test_{i:03d}",
                brand="TestBrand",
                model=f"TestModel{i}",
                capacity="8GB",
                frequency="2400MHz",
                type="DDR4",
                current_price=Decimal(f"{50 + i}.00"),
                original_price=Decimal(f"{60 + i}.00"),
                url=f"http://test.com/product{i}",
                timestamp=datetime.now(),
                metadata={"batch": True, "index": i}
            )
            products.append(product)
        
        # Save products using batch
        for product in products:
            result = self.repository.save_price_record(product, use_batch=True)
            assert result is True
        
        # Wait for batch processing
        batch_completed = self.repository.wait_for_batch_completion(timeout=5.0)
        assert batch_completed is True
        
        # Check that operations were recorded
        assert self.repository._write_operations.get_value() == 5
    
    def test_concurrent_saves(self):
        """Test concurrent saving of products."""
        def worker_save_products(worker_id: int, num_products: int):
            """Worker function to save products concurrently."""
            for i in range(num_products):
                product = StandardizedProduct(
                    source="test",
                    product_id=f"worker_{worker_id}_product_{i}",
                    brand=f"Brand{worker_id}",
                    model=f"Model{i}",
                    capacity="8GB",
                    frequency="2400MHz",
                    type="DDR4",
                    current_price=Decimal(f"{50 + i}.00"),
                    original_price=Decimal(f"{60 + i}.00"),
                    url=f"http://test.com/worker{worker_id}/product{i}",
                    timestamp=datetime.now(),
                    metadata={"worker_id": worker_id, "product_num": i}
                )
                
                self.repository.save_price_record(product, use_batch=True)
        
        # Create multiple threads
        threads = []
        num_workers = 3
        products_per_worker = 3
        
        for worker_id in range(num_workers):
            thread = threading.Thread(
                target=worker_save_products,
                args=(worker_id, products_per_worker)
            )
            threads.append(thread)
        
        # Start all threads
        for thread in threads:
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Wait for batch processing to complete
        batch_completed = self.repository.wait_for_batch_completion(timeout=10.0)
        assert batch_completed is True
        
        # Check that all operations were recorded
        expected_operations = num_workers * products_per_worker
        assert self.repository._write_operations.get_value() == expected_operations
    
    def test_duplicate_product_handling(self):
        """Test handling of duplicate products."""
        product = StandardizedProduct(
            source="test",
            product_id="duplicate_test",
            brand="TestBrand",
            model="TestModel",
            capacity="16GB",
            frequency="3200MHz",
            type="DDR4",
            current_price=Decimal("100.00"),
            original_price=Decimal("120.00"),
            url="http://test.com/product1",
            timestamp=datetime.now(),
            metadata={"test": True}
        )
        
        # Save the same product twice
        result1 = self.repository.save_price_record(product, use_batch=False)
        result2 = self.repository.save_price_record(product, use_batch=False)
        
        assert result1 is True
        assert result2 is True  # Should return True but not actually save duplicate
        
        # Only one write operation should be recorded for the first save
        assert self.repository._write_operations.get_value() == 1
    
    def test_read_operations(self):
        """Test read operations."""
        # First save some test data
        product = StandardizedProduct(
            source="test",
            product_id="read_test",
            brand="ReadBrand",
            model="ReadModel",
            capacity="32GB",
            frequency="3600MHz",
            type="DDR4",
            current_price=Decimal("200.00"),
            original_price=Decimal("250.00"),
            url="http://test.com/read_product",
            timestamp=datetime.now(),
            metadata={"read_test": True}
        )
        
        self.repository.save_price_record(product, use_batch=False)
        
        # Test get_products_by_specs
        products = self.repository.get_products_by_specs(brand="ReadBrand")
        assert len(products) == 1
        assert products[0].brand == "ReadBrand"
        assert self.repository._read_operations.get_value() == 1
    
    def test_thread_safety_stats(self):
        """Test thread safety statistics."""
        # Perform some operations
        product = StandardizedProduct(
            source="test",
            product_id="stats_test",
            brand="StatsBrand",
            model="StatsModel",
            capacity="16GB",
            frequency="3200MHz",
            type="DDR4",
            current_price=Decimal("100.00"),
            original_price=Decimal("120.00"),
            url="http://test.com/stats_product",
            timestamp=datetime.now(),
            metadata={"stats_test": True}
        )
        
        self.repository.save_price_record(product, use_batch=False)
        self.repository.get_products_by_specs(brand="StatsBrand")
        
        stats = self.repository.get_thread_safety_stats()
        
        assert 'operations' in stats
        assert 'deduplication' in stats
        assert 'batching' in stats
        assert 'connection_pool' in stats
        
        assert stats['operations']['write_operations'] == 1
        assert stats['operations']['read_operations'] == 1
        assert stats['operations']['failed_operations'] == 0
        assert stats['operations']['success_rate'] == 100.0
    
    def test_database_stats(self):
        """Test database statistics."""
        # Save a product first
        product = StandardizedProduct(
            source="test",
            product_id="db_stats_test",
            brand="DBStatsBrand",
            model="DBStatsModel",
            capacity="16GB",
            frequency="3200MHz",
            type="DDR4",
            current_price=Decimal("100.00"),
            original_price=Decimal("120.00"),
            url="http://test.com/db_stats_product",
            timestamp=datetime.now(),
            metadata={"db_stats_test": True}
        )
        
        self.repository.save_price_record(product, use_batch=False)
        
        stats = self.repository.get_database_stats()
        
        assert 'total_products' in stats
        assert 'total_price_records' in stats
        assert 'thread_safety' in stats
        
        assert stats['total_products'] >= 1
        assert stats['total_price_records'] >= 1
    
    def test_force_batch_flush(self):
        """Test forcing batch flush."""
        product = StandardizedProduct(
            source="test",
            product_id="flush_test",
            brand="FlushBrand",
            model="FlushModel",
            capacity="16GB",
            frequency="3200MHz",
            type="DDR4",
            current_price=Decimal("100.00"),
            original_price=Decimal("120.00"),
            url="http://test.com/flush_product",
            timestamp=datetime.now(),
            metadata={"flush_test": True}
        )
        
        # Add to batch but don't wait for automatic flush
        self.repository.save_price_record(product, use_batch=True)
        
        # Force flush
        self.repository.force_batch_flush()
        
        # Check that operation was processed
        assert self.repository._write_operations.get_value() == 1
    
    def test_repository_context_manager(self):
        """Test repository as context manager."""
        with ThreadSafePriceRepository(self.db_manager, max_connections=2) as repo:
            product = StandardizedProduct(
                source="test",
                product_id="context_test",
                brand="ContextBrand",
                model="ContextModel",
                capacity="16GB",
                frequency="3200MHz",
                type="DDR4",
                current_price=Decimal("100.00"),
                original_price=Decimal("120.00"),
                url="http://test.com/context_product",
                timestamp=datetime.now(),
                metadata={"context_test": True}
            )
            
            result = repo.save_price_record(product, use_batch=False)
            assert result is True
        
        # Repository should be closed after context exit
        # This is mainly testing that no exceptions are raised
    
    def test_error_handling(self):
        """Test error handling in thread-safe operations."""
        # Create a repository with a mock database manager that will fail
        mock_db_manager = Mock()
        mock_db_manager.execute_query.side_effect = Exception("Database error")
        
        repo = ThreadSafePriceRepository(mock_db_manager, max_connections=1)
        
        try:
            product = StandardizedProduct(
                source="test",
                product_id="error_test",
                brand="ErrorBrand",
                model="ErrorModel",
                capacity="16GB",
                frequency="3200MHz",
                type="DDR4",
                current_price=Decimal("100.00"),
                original_price=Decimal("120.00"),
                url="http://test.com/error_product",
                timestamp=datetime.now(),
                metadata={"error_test": True}
            )
            
            # This should handle the error gracefully
            result = repo.save_price_record(product, use_batch=False)
            assert result is False
            
            # Failed operations should be recorded
            assert repo._failed_operations.get_value() > 0
            
        finally:
            repo.close()


if __name__ == "__main__":
    pytest.main([__file__])
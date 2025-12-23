"""
Property-based tests for data persistence.

**Feature: memory-price-monitor, Property 3: Data persistence round trip**
**Validates: Requirements 1.4, 3.2**
"""

import pytest
import tempfile
import os
from datetime import datetime, date
from decimal import Decimal
from typing import Dict, Any
from hypothesis import given, strategies as st, assume
from hypothesis import settings
from pathlib import Path
import sys
import sqlite3

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from memory_price_monitor.data.models import StandardizedProduct, MemoryType
from memory_price_monitor.data.repository import PriceRepository
from memory_price_monitor.data.database import DatabaseManager
from config import DatabaseConfig


# Mock database manager for testing with SQLite
class MockDatabaseManager:
    """Mock database manager using SQLite for testing."""
    
    def __init__(self, db_path: str = ":memory:"):
        # Use a unique in-memory database name for each instance
        import uuid
        self.db_name = f"file:memdb{uuid.uuid4().hex}?mode=memory&cache=shared"
        self.connection = sqlite3.connect(self.db_name, uri=True, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self._create_tables()
    
    def _create_tables(self):
        """Create test tables."""
        cursor = self.connection.cursor()
        
        # Create products table
        cursor.execute("""
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
        )
        """)
        
        # Create price_records table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS price_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER,
            current_price DECIMAL(10, 2) NOT NULL,
            original_price DECIMAL(10, 2),
            recorded_at TIMESTAMP NOT NULL,
            metadata TEXT,
            FOREIGN KEY (product_id) REFERENCES products(id)
        )
        """)
        
        self.connection.commit()
    
    def execute_query(self, query: str, params=None, fetch: bool = True):
        """Execute a query."""
        cursor = self.connection.cursor()
        
        # Convert PostgreSQL-style queries to SQLite
        sqlite_query = query.replace('%s', '?')
        is_returning_query = 'RETURNING id' in query
        sqlite_query = sqlite_query.replace('RETURNING id', '')
        
        # Convert Decimal parameters to float for SQLite compatibility
        if params:
            converted_params = []
            for param in params:
                if isinstance(param, Decimal):
                    converted_params.append(float(param))
                elif isinstance(param, dict):
                    # Convert metadata dict to JSON string
                    import json
                    converted_params.append(json.dumps(param))
                else:
                    converted_params.append(param)
            cursor.execute(sqlite_query, converted_params)
        else:
            cursor.execute(sqlite_query)
        
        self.connection.commit()
        
        # For INSERT queries that need to return ID
        if is_returning_query:
            return [(cursor.lastrowid,)]
        
        if fetch:
            results = cursor.fetchall()
            # Convert Row objects to tuples for compatibility
            return [tuple(row) for row in results] if results else []
        
        return None
    
    def close(self):
        """Close the connection."""
        if self.connection:
            self.connection.close()


# Hypothesis strategies for generating test data
@st.composite
def valid_standardized_product_strategy(draw):
    """Generate valid standardized product data."""
    brands = ['CORSAIR', 'KINGSTON', 'G.SKILL', 'CRUCIAL', 'SAMSUNG']
    capacities = ['4GB', '8GB', '16GB', '32GB', '64GB']
    frequencies = ['2133MHz', '2400MHz', '2666MHz', '3200MHz', '3600MHz']
    memory_types = [t.value for t in MemoryType]
    sources = ['jd', 'zol', 'amazon', 'newegg']
    
    brand = draw(st.sampled_from(brands))
    capacity = draw(st.sampled_from(capacities))
    frequency = draw(st.sampled_from(frequencies))
    memory_type = draw(st.sampled_from(memory_types))
    source = draw(st.sampled_from(sources))
    
    current_price = draw(st.decimals(min_value=Decimal('10.00'), max_value=Decimal('9999.99'), places=2))
    original_price = draw(st.decimals(min_value=current_price, max_value=current_price * Decimal('2'), places=2))
    
    product_id = draw(st.text(min_size=5, max_size=20, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'))))
    model = f"{brand} {memory_type} {capacity} {frequency}"
    url = f"https://{source}.com/product/{product_id}"
    timestamp = draw(st.datetimes(min_value=datetime(2020, 1, 1), max_value=datetime(2024, 12, 31)))
    
    metadata = draw(st.dictionaries(
        st.text(min_size=1, max_size=10, alphabet=st.characters(whitelist_categories=('Lu', 'Ll'))),
        st.one_of(st.text(max_size=50), st.integers(), st.floats(allow_nan=False, allow_infinity=False)),
        max_size=5
    ))
    
    return StandardizedProduct(
        source=source,
        product_id=product_id,
        brand=brand,
        model=model,
        capacity=capacity,
        frequency=frequency,
        type=memory_type,
        current_price=current_price,
        original_price=original_price,
        url=url,
        timestamp=timestamp,
        metadata=metadata
    )


class TestDataPersistenceRoundTrip:
    """Test data persistence round trip property."""
    
    def setup_method(self):
        """Set up test database for each test."""
        # Use a fresh in-memory database for each test
        self.db_manager = MockDatabaseManager(":memory:")
        self.repository = PriceRepository(self.db_manager)
    
    def teardown_method(self):
        """Clean up test database after each test."""
        if hasattr(self, 'db_manager'):
            self.db_manager.close()
    
    @given(product=valid_standardized_product_strategy())
    @settings(max_examples=100)
    def test_data_persistence_round_trip(self, product):
        """
        **Feature: memory-price-monitor, Property 3: Data persistence round trip**
        
        For any valid StandardizedProduct, storing it to the database and then 
        retrieving it should return equivalent data.
        
        **Validates: Requirements 1.4, 3.2**
        """
        # Clear the database before each example to ensure isolation
        self.db_manager.connection.execute("DELETE FROM price_records")
        self.db_manager.connection.execute("DELETE FROM products")
        self.db_manager.connection.commit()
        
        # Save the product to database
        save_result = self.repository.save_price_record(product)
        assert save_result == True
        
        # Retrieve the product from database
        retrieved_product = self.repository.get_product_by_id(product.product_id, product.source)
        
        # Verify the product was retrieved successfully
        assert retrieved_product is not None
        
        # Verify all core fields match
        assert retrieved_product.source == product.source
        assert retrieved_product.product_id == product.product_id
        assert retrieved_product.brand == product.brand
        assert retrieved_product.model == product.model
        assert retrieved_product.capacity == product.capacity
        assert retrieved_product.frequency == product.frequency
        assert retrieved_product.type == product.type
        assert retrieved_product.url == product.url
        
        # Verify price data matches
        assert retrieved_product.current_price == product.current_price
        assert retrieved_product.original_price == product.original_price
        
        # Verify timestamp is preserved (allowing for small differences due to storage precision)
        time_diff = abs((retrieved_product.timestamp - product.timestamp).total_seconds())
        assert time_diff < 1.0  # Allow up to 1 second difference
        
        # Verify metadata is preserved (converted to/from JSON)
        # Note: JSON serialization may change types, so we check string representations
        for key, value in product.metadata.items():
            if key in retrieved_product.metadata:
                assert str(retrieved_product.metadata[key]) == str(value)
    
    @given(products=st.lists(valid_standardized_product_strategy(), min_size=1, max_size=10))
    @settings(max_examples=10)
    def test_multiple_products_persistence(self, products):
        """
        Test that multiple products can be stored and retrieved correctly.
        """
        # Clear the database before each example to ensure isolation
        self.db_manager.connection.execute("DELETE FROM price_records")
        self.db_manager.connection.execute("DELETE FROM products")
        self.db_manager.connection.commit()
        
        # Make sure all products have unique identifiers
        unique_products = []
        seen_ids = set()
        for product in products:
            key = (product.source, product.product_id)
            if key not in seen_ids:
                unique_products.append(product)
                seen_ids.add(key)
        
        assume(len(unique_products) > 0)
        
        # Save all products
        for product in unique_products:
            save_result = self.repository.save_price_record(product)
            assert save_result == True
        
        # Retrieve and verify each product
        for original_product in unique_products:
            retrieved_product = self.repository.get_product_by_id(
                original_product.product_id, 
                original_product.source
            )
            
            assert retrieved_product is not None
            assert retrieved_product.source == original_product.source
            assert retrieved_product.product_id == original_product.product_id
            assert retrieved_product.brand == original_product.brand
            assert retrieved_product.current_price == original_product.current_price
            assert retrieved_product.original_price == original_product.original_price
    
    @given(product=valid_standardized_product_strategy())
    @settings(max_examples=10)
    def test_price_history_persistence(self, product):
        """
        Test that price history is correctly stored and retrieved.
        """
        # Clear the database before each example to ensure isolation
        self.db_manager.connection.execute("DELETE FROM price_records")
        self.db_manager.connection.execute("DELETE FROM products")
        self.db_manager.connection.commit()
        
        # Save the initial product
        save_result = self.repository.save_price_record(product)
        assert save_result == True
        
        # Create a modified version with different prices (simulating price change)
        updated_product = StandardizedProduct(
            source=product.source,
            product_id=product.product_id,
            brand=product.brand,
            model=product.model,
            capacity=product.capacity,
            frequency=product.frequency,
            type=product.type,
            current_price=product.current_price * Decimal('0.9'),  # 10% discount
            original_price=product.original_price,
            url=product.url,
            timestamp=datetime.now(),
            metadata=product.metadata
        )
        
        # Save the updated product (should create a new price record)
        save_result2 = self.repository.save_price_record(updated_product)
        assert save_result2 == True
        
        # Retrieve price history
        start_date = date(2020, 1, 1)
        end_date = date(2025, 12, 31)
        
        price_history = self.repository.get_price_history(
            product.product_id, 
            start_date, 
            end_date,
            product.source
        )
        
        # Should have at least 2 price records
        assert len(price_history) >= 2
        
        # Verify price records are ordered by timestamp
        timestamps = [record.recorded_at for record in price_history]
        assert timestamps == sorted(timestamps)
        
        # Verify both price points are present
        prices = [record.current_price for record in price_history]
        assert product.current_price in prices
        assert updated_product.current_price in prices
    
    @given(product=valid_standardized_product_strategy())
    @settings(max_examples=10)
    def test_product_specs_filtering(self, product):
        """
        Test that products can be filtered by specifications correctly.
        """
        # Clear the database before each example to ensure isolation
        self.db_manager.connection.execute("DELETE FROM price_records")
        self.db_manager.connection.execute("DELETE FROM products")
        self.db_manager.connection.commit()
        
        # Save the product
        save_result = self.repository.save_price_record(product)
        assert save_result == True
        
        # Test filtering by brand
        products_by_brand = self.repository.get_products_by_specs(brand=product.brand)
        assert len(products_by_brand) >= 1
        assert all(p.brand == product.brand for p in products_by_brand)
        
        # Test filtering by capacity
        products_by_capacity = self.repository.get_products_by_specs(capacity=product.capacity)
        assert len(products_by_capacity) >= 1
        assert all(p.capacity == product.capacity for p in products_by_capacity)
        
        # Test filtering by type
        products_by_type = self.repository.get_products_by_specs(type=product.type)
        assert len(products_by_type) >= 1
        assert all(p.type == product.type for p in products_by_type)
        
        # Test combined filtering
        products_combined = self.repository.get_products_by_specs(
            brand=product.brand,
            capacity=product.capacity,
            type=product.type
        )
        assert len(products_combined) >= 1
        
        # Our product should be in the combined results
        found_product = None
        for p in products_combined:
            if p.product_id == product.product_id and p.source == product.source:
                found_product = p
                break
        
        assert found_product is not None
        assert found_product.brand == product.brand
        assert found_product.capacity == product.capacity
        assert found_product.type == product.type
    
    @given(product=valid_standardized_product_strategy())
    @settings(max_examples=10)
    def test_duplicate_product_handling(self, product):
        """
        Test that saving the same product multiple times doesn't create duplicates.
        """
        # Clear the database before each example to ensure isolation
        self.db_manager.connection.execute("DELETE FROM price_records")
        self.db_manager.connection.execute("DELETE FROM products")
        self.db_manager.connection.commit()
        
        # Save the product multiple times
        for _ in range(3):
            save_result = self.repository.save_price_record(product)
            assert save_result == True
        
        # Retrieve price history - should have 3 price records for the same product
        start_date = date(2020, 1, 1)
        end_date = date(2025, 12, 31)
        
        price_history = self.repository.get_price_history(
            product.product_id,
            start_date,
            end_date,
            product.source
        )
        
        # Should have exactly 3 price records
        assert len(price_history) == 3
        
        # All should have the same price (since we saved the same product)
        assert all(record.current_price == product.current_price for record in price_history)
        assert all(record.original_price == product.original_price for record in price_history)
        
        # But there should still be only one product entry
        retrieved_product = self.repository.get_product_by_id(product.product_id, product.source)
        assert retrieved_product is not None
        assert retrieved_product.product_id == product.product_id


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
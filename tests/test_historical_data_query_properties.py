"""
Property-based tests for historical data query accuracy.

**Feature: memory-price-monitor, Property 23: Historical data query accuracy**
**Validates: Requirements 7.1**
"""

import pytest
import tempfile
import os
from datetime import datetime, date, timedelta
from decimal import Decimal
from typing import Dict, Any, List
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


@st.composite
def date_range_strategy(draw):
    """Generate valid date ranges."""
    start_date = draw(st.dates(min_value=date(2020, 1, 1), max_value=date(2024, 6, 1)))
    end_date = draw(st.dates(min_value=start_date, max_value=date(2024, 12, 31)))
    return start_date, end_date


class TestHistoricalDataQueryAccuracy:
    """Test historical data query accuracy property."""
    
    def setup_method(self):
        """Set up test database for each test."""
        # Use a fresh in-memory database for each test
        self.db_manager = MockDatabaseManager(":memory:")
        self.repository = PriceRepository(self.db_manager)
    
    def teardown_method(self):
        """Clean up test database after each test."""
        if hasattr(self, 'db_manager'):
            self.db_manager.close()
    
    @given(
        products=st.lists(valid_standardized_product_strategy(), min_size=1, max_size=10),
        date_range=date_range_strategy()
    )
    @settings(max_examples=10)
    def test_historical_data_query_accuracy(self, products, date_range):
        """
        **Feature: memory-price-monitor, Property 23: Historical data query accuracy**
        
        For any query with date range, brand, and specification filters, the system 
        should return only data matching all specified criteria.
        
        **Validates: Requirements 7.1**
        """
        start_date, end_date = date_range
        
        # Clear the database before each example to ensure isolation
        self.db_manager.connection.execute("DELETE FROM price_records")
        self.db_manager.connection.execute("DELETE FROM products")
        self.db_manager.connection.commit()
        
        # Make products unique by (source, product_id)
        unique_products = []
        seen_ids = set()
        for product in products:
            key = (product.source, product.product_id)
            if key not in seen_ids:
                unique_products.append(product)
                seen_ids.add(key)
        
        assume(len(unique_products) > 0)
        
        # Create products with timestamps both inside and outside the date range
        products_in_range = []
        products_out_of_range = []
        
        for i, product in enumerate(unique_products):
            # Alternate between in-range and out-of-range timestamps
            if i % 2 == 0:
                # In range - set timestamp within the date range
                # Handle case where start_date == end_date
                if start_date == end_date:
                    in_range_date = start_date
                else:
                    days_diff = (end_date - start_date).days
                    offset_days = max(0, days_diff // 2)
                    in_range_date = start_date + timedelta(days=offset_days)
                
                product_in_range = StandardizedProduct(
                    source=product.source,
                    product_id=product.product_id,
                    brand=product.brand,
                    model=product.model,
                    capacity=product.capacity,
                    frequency=product.frequency,
                    type=product.type,
                    current_price=product.current_price,
                    original_price=product.original_price,
                    url=product.url,
                    timestamp=datetime.combine(in_range_date, datetime.min.time()),
                    metadata=product.metadata
                )
                products_in_range.append(product_in_range)
                self.repository.save_price_record(product_in_range)
            else:
                # Out of range - set timestamp before the start date
                out_of_range_date = start_date - timedelta(days=30)
                product_out_of_range = StandardizedProduct(
                    source=product.source,
                    product_id=product.product_id,
                    brand=product.brand,
                    model=product.model,
                    capacity=product.capacity,
                    frequency=product.frequency,
                    type=product.type,
                    current_price=product.current_price,
                    original_price=product.original_price,
                    url=product.url,
                    timestamp=datetime.combine(out_of_range_date, datetime.min.time()),
                    metadata=product.metadata
                )
                products_out_of_range.append(product_out_of_range)
                self.repository.save_price_record(product_out_of_range)
        
        assume(len(products_in_range) > 0)
        
        # Test date range filtering
        for product_in_range in products_in_range:
            # Convert dates to datetime for the query
            start_datetime = datetime.combine(start_date, datetime.min.time())
            end_datetime = datetime.combine(end_date, datetime.max.time())
            
            price_history = self.repository.get_price_history(
                product_in_range.product_id,
                start_datetime,
                end_datetime,
                product_in_range.source
            )
            
            # Should return records only within the date range
            assert len(price_history) > 0
            
            for record in price_history:
                record_date = record.recorded_at.date()
                assert start_date <= record_date <= end_date, \
                    f"Record date {record_date} not in range [{start_date}, {end_date}]"
        
        # Test that out-of-range products are not returned
        for product_out_of_range in products_out_of_range:
            # Convert dates to datetime for the query
            start_datetime = datetime.combine(start_date, datetime.min.time())
            end_datetime = datetime.combine(end_date, datetime.max.time())
            
            price_history = self.repository.get_price_history(
                product_out_of_range.product_id,
                start_datetime,
                end_datetime,
                product_out_of_range.source
            )
            
            # Should return no records for out-of-range dates
            assert len(price_history) == 0
    
    @given(
        products=st.lists(valid_standardized_product_strategy(), min_size=2, max_size=5),
        brand_filter=st.sampled_from(['CORSAIR', 'KINGSTON', 'G.SKILL'])
    )
    @settings(max_examples=10)
    def test_brand_filtering_accuracy(self, products, brand_filter):
        """
        Test that brand filtering returns only products matching the specified brand.
        """
        # Clear the database before each example to ensure isolation
        self.db_manager.connection.execute("DELETE FROM price_records")
        self.db_manager.connection.execute("DELETE FROM products")
        self.db_manager.connection.commit()
        
        # Make products unique and set different brands
        unique_products = []
        seen_ids = set()
        brands = ['CORSAIR', 'KINGSTON', 'G.SKILL', 'CRUCIAL', 'SAMSUNG']
        
        for i, product in enumerate(products):
            key = (product.source, product.product_id)
            if key not in seen_ids:
                # Assign different brands to products
                modified_product = StandardizedProduct(
                    source=product.source,
                    product_id=product.product_id,
                    brand=brands[i % len(brands)],  # Cycle through brands
                    model=product.model,
                    capacity=product.capacity,
                    frequency=product.frequency,
                    type=product.type,
                    current_price=product.current_price,
                    original_price=product.original_price,
                    url=product.url,
                    timestamp=product.timestamp,
                    metadata=product.metadata
                )
                unique_products.append(modified_product)
                seen_ids.add(key)
        
        assume(len(unique_products) >= 2)
        
        # Save all products
        for product in unique_products:
            self.repository.save_price_record(product)
        
        # Query products by brand
        filtered_products = self.repository.get_products_by_specs(brand=brand_filter)
        
        # All returned products should have the specified brand
        for product in filtered_products:
            assert product.brand == brand_filter, \
                f"Product brand {product.brand} does not match filter {brand_filter}"
        
        # Count expected products with the specified brand
        expected_count = sum(1 for p in unique_products if p.brand == brand_filter)
        assert len(filtered_products) == expected_count, \
            f"Expected {expected_count} products with brand {brand_filter}, got {len(filtered_products)}"
    
    @given(
        products=st.lists(valid_standardized_product_strategy(), min_size=2, max_size=5),
        capacity_filter=st.sampled_from(['8GB', '16GB', '32GB'])
    )
    @settings(max_examples=10)
    def test_capacity_filtering_accuracy(self, products, capacity_filter):
        """
        Test that capacity filtering returns only products matching the specified capacity.
        """
        # Clear the database before each example to ensure isolation
        self.db_manager.connection.execute("DELETE FROM price_records")
        self.db_manager.connection.execute("DELETE FROM products")
        self.db_manager.connection.commit()
        
        # Make products unique and set different capacities
        unique_products = []
        seen_ids = set()
        capacities = ['4GB', '8GB', '16GB', '32GB', '64GB']
        
        for i, product in enumerate(products):
            key = (product.source, product.product_id)
            if key not in seen_ids:
                # Assign different capacities to products
                modified_product = StandardizedProduct(
                    source=product.source,
                    product_id=product.product_id,
                    brand=product.brand,
                    model=product.model,
                    capacity=capacities[i % len(capacities)],  # Cycle through capacities
                    frequency=product.frequency,
                    type=product.type,
                    current_price=product.current_price,
                    original_price=product.original_price,
                    url=product.url,
                    timestamp=product.timestamp,
                    metadata=product.metadata
                )
                unique_products.append(modified_product)
                seen_ids.add(key)
        
        assume(len(unique_products) >= 2)
        
        # Save all products
        for product in unique_products:
            self.repository.save_price_record(product)
        
        # Query products by capacity
        filtered_products = self.repository.get_products_by_specs(capacity=capacity_filter)
        
        # All returned products should have the specified capacity
        for product in filtered_products:
            assert product.capacity == capacity_filter, \
                f"Product capacity {product.capacity} does not match filter {capacity_filter}"
        
        # Count expected products with the specified capacity
        expected_count = sum(1 for p in unique_products if p.capacity == capacity_filter)
        assert len(filtered_products) == expected_count, \
            f"Expected {expected_count} products with capacity {capacity_filter}, got {len(filtered_products)}"
    
    @given(
        products=st.lists(valid_standardized_product_strategy(), min_size=2, max_size=5),
        type_filter=st.sampled_from(['DDR4', 'DDR5'])
    )
    @settings(max_examples=10)
    def test_type_filtering_accuracy(self, products, type_filter):
        """
        Test that memory type filtering returns only products matching the specified type.
        """
        # Clear the database before each example to ensure isolation
        self.db_manager.connection.execute("DELETE FROM price_records")
        self.db_manager.connection.execute("DELETE FROM products")
        self.db_manager.connection.commit()
        
        # Make products unique and set different memory types
        unique_products = []
        seen_ids = set()
        memory_types = ['DDR3', 'DDR4', 'DDR5', 'LPDDR4', 'LPDDR5']
        
        for i, product in enumerate(products):
            key = (product.source, product.product_id)
            if key not in seen_ids:
                # Assign different memory types to products
                modified_product = StandardizedProduct(
                    source=product.source,
                    product_id=product.product_id,
                    brand=product.brand,
                    model=product.model,
                    capacity=product.capacity,
                    frequency=product.frequency,
                    type=memory_types[i % len(memory_types)],  # Cycle through types
                    current_price=product.current_price,
                    original_price=product.original_price,
                    url=product.url,
                    timestamp=product.timestamp,
                    metadata=product.metadata
                )
                unique_products.append(modified_product)
                seen_ids.add(key)
        
        assume(len(unique_products) >= 2)
        
        # Save all products
        for product in unique_products:
            self.repository.save_price_record(product)
        
        # Query products by memory type
        filtered_products = self.repository.get_products_by_specs(type=type_filter)
        
        # All returned products should have the specified memory type
        for product in filtered_products:
            assert product.type == type_filter, \
                f"Product type {product.type} does not match filter {type_filter}"
        
        # Count expected products with the specified type
        expected_count = sum(1 for p in unique_products if p.type == type_filter)
        assert len(filtered_products) == expected_count, \
            f"Expected {expected_count} products with type {type_filter}, got {len(filtered_products)}"
    
    @given(
        products=st.lists(valid_standardized_product_strategy(), min_size=3, max_size=8),
        brand_filter=st.sampled_from(['CORSAIR', 'KINGSTON']),
        capacity_filter=st.sampled_from(['16GB', '32GB']),
        type_filter=st.sampled_from(['DDR4', 'DDR5'])
    )
    @settings(max_examples=10)
    def test_combined_filtering_accuracy(self, products, brand_filter, capacity_filter, type_filter):
        """
        Test that combined filtering (brand + capacity + type) returns only products matching all criteria.
        """
        # Clear the database before each example to ensure isolation
        self.db_manager.connection.execute("DELETE FROM price_records")
        self.db_manager.connection.execute("DELETE FROM products")
        self.db_manager.connection.commit()
        
        # Make products unique and create a mix of products
        unique_products = []
        seen_ids = set()
        brands = ['CORSAIR', 'KINGSTON', 'G.SKILL']
        capacities = ['8GB', '16GB', '32GB', '64GB']
        memory_types = ['DDR4', 'DDR5', 'LPDDR4']
        
        for i, product in enumerate(products):
            key = (product.source, product.product_id)
            if key not in seen_ids:
                # Create a mix of products, some matching all criteria, some not
                modified_product = StandardizedProduct(
                    source=product.source,
                    product_id=product.product_id,
                    brand=brands[i % len(brands)],
                    model=product.model,
                    capacity=capacities[i % len(capacities)],
                    frequency=product.frequency,
                    type=memory_types[i % len(memory_types)],
                    current_price=product.current_price,
                    original_price=product.original_price,
                    url=product.url,
                    timestamp=product.timestamp,
                    metadata=product.metadata
                )
                unique_products.append(modified_product)
                seen_ids.add(key)
        
        assume(len(unique_products) >= 3)
        
        # Save all products
        for product in unique_products:
            self.repository.save_price_record(product)
        
        # Query products with combined filters
        filtered_products = self.repository.get_products_by_specs(
            brand=brand_filter,
            capacity=capacity_filter,
            type=type_filter
        )
        
        # All returned products should match ALL specified criteria
        for product in filtered_products:
            assert product.brand == brand_filter, \
                f"Product brand {product.brand} does not match filter {brand_filter}"
            assert product.capacity == capacity_filter, \
                f"Product capacity {product.capacity} does not match filter {capacity_filter}"
            assert product.type == type_filter, \
                f"Product type {product.type} does not match filter {type_filter}"
        
        # Count expected products matching all criteria
        expected_count = sum(
            1 for p in unique_products 
            if p.brand == brand_filter and p.capacity == capacity_filter and p.type == type_filter
        )
        assert len(filtered_products) == expected_count, \
            f"Expected {expected_count} products matching all criteria, got {len(filtered_products)}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
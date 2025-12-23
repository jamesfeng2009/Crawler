"""
Property-based tests for multi-format export validity.

**Feature: memory-price-monitor, Property 25: Multi-format export validity**
**Validates: Requirements 7.3**
"""

import pytest
import tempfile
import os
import json
import csv
import io
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


class TestMultiFormatExportValidity:
    """Test multi-format export validity property."""
    
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
        export_format=st.sampled_from(['csv', 'json', 'CSV', 'JSON'])
    )
    @settings(max_examples=100)
    def test_multi_format_export_validity(self, products, export_format):
        """
        **Feature: memory-price-monitor, Property 25: Multi-format export validity**
        
        For any data export request, the system should generate valid files in the 
        requested format (CSV, JSON) with complete data.
        
        **Validates: Requirements 7.3**
        """
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
        
        # Save all products to database
        for product in unique_products:
            self.repository.save_price_record(product)
        
        # Export data in the specified format
        exported_data = self.repository.export_data(export_format)
        
        # Verify export is not empty
        assert exported_data != "", "Exported data should not be empty"
        
        # Verify format-specific validity
        if export_format.lower() == 'csv':
            self._validate_csv_export(exported_data, unique_products)
        elif export_format.lower() == 'json':
            self._validate_json_export(exported_data, unique_products)
    
    def _validate_csv_export(self, csv_data: str, expected_products: List[StandardizedProduct]):
        """Validate CSV export format and content."""
        # Parse CSV data
        csv_reader = csv.DictReader(io.StringIO(csv_data))
        rows = list(csv_reader)
        
        # Verify CSV has data
        assert len(rows) > 0, "CSV should contain data rows"
        
        # Verify CSV has expected headers
        expected_headers = [
            'source', 'product_id', 'brand', 'model', 'capacity',
            'frequency', 'type', 'url', 'current_price', 'original_price',
            'recorded_at', 'metadata'
        ]
        
        actual_headers = csv_reader.fieldnames
        assert actual_headers == expected_headers, \
            f"CSV headers {actual_headers} do not match expected {expected_headers}"
        
        # Verify each row has all required fields
        for row in rows:
            assert row['source'] != "", "Source should not be empty"
            assert row['product_id'] != "", "Product ID should not be empty"
            assert row['brand'] != "", "Brand should not be empty"
            assert row['model'] != "", "Model should not be empty"
            assert row['capacity'] != "", "Capacity should not be empty"
            assert row['type'] != "", "Type should not be empty"
            assert row['url'] != "", "URL should not be empty"
            assert row['current_price'] != "", "Current price should not be empty"
            assert row['original_price'] != "", "Original price should not be empty"
            assert row['recorded_at'] != "", "Recorded at should not be empty"
            
            # Verify price values are numeric
            try:
                float(row['current_price'])
                float(row['original_price'])
            except ValueError:
                pytest.fail(f"Price values should be numeric: {row['current_price']}, {row['original_price']}")
            
            # Verify metadata is valid JSON string
            try:
                json.loads(row['metadata'])
            except json.JSONDecodeError:
                pytest.fail(f"Metadata should be valid JSON: {row['metadata']}")
        
        # Verify we have the expected number of records
        assert len(rows) == len(expected_products), \
            f"Expected {len(expected_products)} CSV rows, got {len(rows)}"
    
    def _validate_json_export(self, json_data: str, expected_products: List[StandardizedProduct]):
        """Validate JSON export format and content."""
        # Parse JSON data
        try:
            data = json.loads(json_data)
        except json.JSONDecodeError as e:
            pytest.fail(f"Invalid JSON format: {e}")
        
        # Verify JSON is a list
        assert isinstance(data, list), "JSON export should be a list of records"
        
        # Verify JSON has data
        assert len(data) > 0, "JSON should contain data records"
        
        # Verify each record has all required fields
        for record in data:
            assert isinstance(record, dict), "Each JSON record should be a dictionary"
            
            required_fields = [
                'source', 'product_id', 'brand', 'model', 'capacity',
                'frequency', 'type', 'url', 'current_price', 'original_price',
                'recorded_at', 'metadata'
            ]
            
            for field in required_fields:
                assert field in record, f"JSON record missing required field: {field}"
            
            # Verify field types
            assert isinstance(record['source'], str), "Source should be string"
            assert isinstance(record['product_id'], str), "Product ID should be string"
            assert isinstance(record['brand'], str), "Brand should be string"
            assert isinstance(record['model'], str), "Model should be string"
            assert isinstance(record['capacity'], str), "Capacity should be string"
            assert isinstance(record['type'], str), "Type should be string"
            assert isinstance(record['url'], str), "URL should be string"
            assert isinstance(record['current_price'], (int, float)), "Current price should be numeric"
            assert isinstance(record['original_price'], (int, float)), "Original price should be numeric"
            assert isinstance(record['recorded_at'], str), "Recorded at should be string"
            assert isinstance(record['metadata'], dict), "Metadata should be dictionary"
            
            # Verify price values are positive
            assert record['current_price'] > 0, "Current price should be positive"
            assert record['original_price'] > 0, "Original price should be positive"
            
            # Verify timestamp format
            try:
                datetime.fromisoformat(record['recorded_at'].replace('Z', '+00:00'))
            except ValueError:
                pytest.fail(f"Invalid timestamp format: {record['recorded_at']}")
        
        # Verify we have the expected number of records
        assert len(data) == len(expected_products), \
            f"Expected {len(expected_products)} JSON records, got {len(data)}"
    
    @given(products=st.lists(valid_standardized_product_strategy(), min_size=1, max_size=5))
    @settings(max_examples=10)
    def test_export_with_filters(self, products):
        """
        Test that export with filters returns only matching data.
        """
        # Clear the database before each example to ensure isolation
        self.db_manager.connection.execute("DELETE FROM price_records")
        self.db_manager.connection.execute("DELETE FROM products")
        self.db_manager.connection.commit()
        
        # Make products unique and assign specific brands
        unique_products = []
        seen_ids = set()
        brands = ['CORSAIR', 'KINGSTON', 'G.SKILL']
        
        for i, product in enumerate(products):
            key = (product.source, product.product_id)
            if key not in seen_ids:
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
        
        assume(len(unique_products) > 0)
        
        # Save all products
        for product in unique_products:
            self.repository.save_price_record(product)
        
        # Test export with brand filter
        brand_filter = 'CORSAIR'
        csv_export = self.repository.export_data('csv', brand=brand_filter)
        json_export = self.repository.export_data('json', brand=brand_filter)
        
        # Parse and verify CSV export
        if csv_export:
            csv_reader = csv.DictReader(io.StringIO(csv_export))
            csv_rows = list(csv_reader)
            
            for row in csv_rows:
                assert row['brand'] == brand_filter, \
                    f"CSV export should only contain {brand_filter} products, found {row['brand']}"
        
        # Parse and verify JSON export
        if json_export and json_export != "[]":
            json_data = json.loads(json_export)
            
            for record in json_data:
                assert record['brand'] == brand_filter, \
                    f"JSON export should only contain {brand_filter} products, found {record['brand']}"
        
        # Count expected products with the brand filter
        expected_count = sum(1 for p in unique_products if p.brand == brand_filter)
        
        if expected_count > 0:
            csv_reader = csv.DictReader(io.StringIO(csv_export))
            csv_rows = list(csv_reader)
            assert len(csv_rows) == expected_count, \
                f"CSV export should contain {expected_count} records, got {len(csv_rows)}"
            
            json_data = json.loads(json_export)
            assert len(json_data) == expected_count, \
                f"JSON export should contain {expected_count} records, got {len(json_data)}"
    
    @given(products=st.lists(valid_standardized_product_strategy(), min_size=1, max_size=5))
    @settings(max_examples=10)
    def test_export_with_date_range(self, products):
        """
        Test that export with date range filters returns only data within the specified range.
        """
        # Clear the database before each example to ensure isolation
        self.db_manager.connection.execute("DELETE FROM price_records")
        self.db_manager.connection.execute("DELETE FROM products")
        self.db_manager.connection.commit()
        
        # Make products unique
        unique_products = []
        seen_ids = set()
        for product in products:
            key = (product.source, product.product_id)
            if key not in seen_ids:
                unique_products.append(product)
                seen_ids.add(key)
        
        assume(len(unique_products) > 0)
        
        # Set specific timestamps for products
        base_date = date(2023, 6, 1)
        for i, product in enumerate(unique_products):
            # Set timestamps with different dates
            product_date = base_date + timedelta(days=i * 10)
            product.timestamp = datetime.combine(product_date, datetime.min.time())
            self.repository.save_price_record(product)
        
        # Define date range filter
        start_date = base_date + timedelta(days=5)
        end_date = base_date + timedelta(days=25)
        
        # Export with date range filter
        json_export = self.repository.export_data(
            'json',
            start_date=start_date,
            end_date=end_date
        )
        
        # Parse and verify JSON export
        if json_export and json_export != "[]":
            json_data = json.loads(json_export)
            
            for record in json_data:
                record_date = datetime.fromisoformat(record['recorded_at'].replace('Z', '+00:00')).date()
                assert start_date <= record_date <= end_date, \
                    f"Record date {record_date} not in range [{start_date}, {end_date}]"
    
    @given(invalid_format=st.text(min_size=1, max_size=10).filter(lambda x: x.lower() not in ['csv', 'json']))
    @settings(max_examples=10)
    def test_invalid_format_handling(self, invalid_format):
        """
        Test that invalid export formats raise appropriate errors.
        """
        # Clear the database before each example to ensure isolation
        self.db_manager.connection.execute("DELETE FROM price_records")
        self.db_manager.connection.execute("DELETE FROM products")
        self.db_manager.connection.commit()
        
        # Test that invalid format raises ValueError
        with pytest.raises(ValueError) as exc_info:
            self.repository.export_data(invalid_format)
        
        assert "Unsupported export format" in str(exc_info.value)
        assert invalid_format in str(exc_info.value)
    
    def test_empty_data_export(self):
        """
        Test that exporting empty data returns appropriate empty results.
        """
        # Clear the database to ensure no data
        self.db_manager.connection.execute("DELETE FROM price_records")
        self.db_manager.connection.execute("DELETE FROM products")
        self.db_manager.connection.commit()
        
        # Export CSV from empty database
        csv_export = self.repository.export_data('csv')
        assert csv_export == "", "Empty CSV export should return empty string"
        
        # Export JSON from empty database
        json_export = self.repository.export_data('json')
        assert json_export == "[]", "Empty JSON export should return empty array"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
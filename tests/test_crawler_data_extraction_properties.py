"""
Property-based tests for crawler data extraction.

**Feature: memory-price-monitor, Property 1: Crawler data extraction completeness**
**Validates: Requirements 1.1, 1.2**
"""

import pytest
from datetime import datetime
from decimal import Decimal
from typing import Dict, Any, List
from unittest.mock import Mock, patch, MagicMock
from hypothesis import given, strategies as st, assume
from hypothesis import settings
from pathlib import Path
import sys
import requests

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from memory_price_monitor.crawlers.base import BaseCrawler, RawProduct, CrawlResult
from memory_price_monitor.utils.errors import CrawlerError


# Mock crawler implementation for testing
class MockCrawler(BaseCrawler):
    """Mock crawler for testing data extraction completeness."""
    
    def __init__(self, source_name: str, config: Dict[str, Any] = None, mock_data: List[Dict[str, Any]] = None):
        super().__init__(source_name, config)
        self.mock_data = mock_data or []
        self.should_fail = config.get('should_fail', False) if config else False
        self.parsing_errors = config.get('parsing_errors', []) if config else []
    
    def fetch_products(self) -> List[RawProduct]:
        """Mock implementation that returns test data."""
        if self.should_fail:
            raise CrawlerError("Mock fetch failure")
        
        products = []
        for i, data in enumerate(self.mock_data):
            product = RawProduct(
                source=self.source_name,
                product_id=data.get('product_id', f'mock_{i}'),
                raw_data=data,
                url=data.get('url', f'https://example.com/product/{i}'),
                timestamp=datetime.now()
            )
            products.append(product)
        
        return products
    
    def parse_product(self, raw_data: Any) -> RawProduct:
        """Mock implementation that parses raw data."""
        if isinstance(raw_data, RawProduct):
            # Check if this product should cause a parsing error
            product_id = raw_data.product_id
            if product_id in self.parsing_errors:
                raise CrawlerError(f"Mock parsing error for {product_id}")
            
            # Validate required fields are present
            data = raw_data.raw_data
            required_fields = ['brand', 'price', 'title']
            missing_fields = [field for field in required_fields if not data.get(field)]
            
            if missing_fields:
                raise CrawlerError(f"Missing required fields: {missing_fields}")
            
            return raw_data
        else:
            raise CrawlerError("Invalid raw data format")
    
    def get_price_history(self, product_id: str) -> List:
        """Mock implementation - not used in this test."""
        return []


# Hypothesis strategies for generating test data
@st.composite
def valid_product_data_strategy(draw):
    """Generate valid product data."""
    brands = ['CORSAIR', 'KINGSTON', 'G.SKILL', 'CRUCIAL', 'SAMSUNG']
    capacities = ['4GB', '8GB', '16GB', '32GB', '64GB']
    frequencies = ['2133MHz', '2400MHz', '2666MHz', '3200MHz', '3600MHz']
    memory_types = ['DDR4', 'DDR5', 'LPDDR4', 'LPDDR5']
    
    brand = draw(st.sampled_from(brands))
    capacity = draw(st.sampled_from(capacities))
    frequency = draw(st.sampled_from(frequencies))
    memory_type = draw(st.sampled_from(memory_types))
    
    price = draw(st.decimals(min_value=Decimal('10.00'), max_value=Decimal('9999.99'), places=2))
    original_price = draw(st.decimals(min_value=price, max_value=price * Decimal('2'), places=2))
    
    # Generate unique product IDs using integers to avoid collisions
    product_id = f"PROD_{draw(st.integers(min_value=1000, max_value=999999))}"
    title = f"{brand} {memory_type} {capacity} {frequency} Memory Module"
    url = f"https://example.com/product/{product_id}"
    
    return {
        'product_id': product_id,
        'brand': brand,
        'title': title,
        'capacity': capacity,
        'frequency': frequency,
        'type': memory_type,
        'price': float(price),
        'original_price': float(original_price),
        'url': url,
        'specifications': {
            'capacity': capacity,
            'frequency': frequency,
            'type': memory_type
        }
    }


@st.composite
def crawler_config_strategy(draw):
    """Generate crawler configuration."""
    return {
        'rate_limit': {
            'requests_per_second': draw(st.floats(min_value=0.1, max_value=10.0)),
            'requests_per_minute': draw(st.integers(min_value=1, max_value=600)),
            'requests_per_hour': draw(st.integers(min_value=60, max_value=36000))
        },
        'retry': {
            'max_attempts': draw(st.integers(min_value=1, max_value=5)),
            'backoff_factor': draw(st.floats(min_value=1.0, max_value=5.0)),
            'initial_delay': draw(st.floats(min_value=0.1, max_value=5.0))
        },
        'timeout': draw(st.floats(min_value=5.0, max_value=120.0))
    }


class TestCrawlerDataExtractionCompleteness:
    """Test crawler data extraction completeness property."""
    
    @given(
        products_data=st.lists(valid_product_data_strategy(), min_size=1, max_size=20),
        config=crawler_config_strategy(),
        source_name=st.text(min_size=2, max_size=10, alphabet=st.characters(whitelist_categories=('Lu', 'Ll')))
    )
    @settings(max_examples=10)
    def test_crawler_data_extraction_completeness(self, products_data, config, source_name):
        """
        **Feature: memory-price-monitor, Property 1: Crawler data extraction completeness**
        
        For any crawler module and any valid product page, executing the crawler should 
        extract all required fields (brand, specifications, current price, timestamp) 
        with valid values.
        
        **Validates: Requirements 1.1, 1.2**
        """
        # Create mock crawler with test data
        crawler = MockCrawler(source_name, config, products_data)
        
        # Execute the crawler
        result = crawler.execute()
        
        # Verify crawl was successful
        assert result.success == True
        assert result.source == source_name
        assert result.products_found == len(products_data)
        assert result.products_extracted == len(products_data)
        assert len(result.errors) == 0
        
        # Verify timing information
        assert result.started_at is not None
        assert result.completed_at is not None
        assert result.duration_seconds is not None
        assert result.duration_seconds >= 0
        assert result.completed_at >= result.started_at
        
        # Fetch and verify individual products
        raw_products = crawler.fetch_products()
        assert len(raw_products) == len(products_data)
        
        for i, raw_product in enumerate(raw_products):
            original_data = products_data[i]
            
            # Verify all required fields are present and valid
            assert raw_product.source == source_name
            assert raw_product.product_id != ""
            assert raw_product.url != ""
            assert raw_product.timestamp is not None
            assert isinstance(raw_product.timestamp, datetime)
            assert isinstance(raw_product.raw_data, dict)
            
            # Verify raw data contains required fields
            raw_data = raw_product.raw_data
            assert 'brand' in raw_data
            assert 'price' in raw_data
            assert 'title' in raw_data
            
            # Verify field values are valid
            assert raw_data['brand'] != ""
            assert isinstance(raw_data['price'], (int, float))
            assert raw_data['price'] > 0
            assert raw_data['title'] != ""
            
            # Verify specifications are present (for memory products)
            if 'specifications' in raw_data:
                specs = raw_data['specifications']
                assert isinstance(specs, dict)
                # Memory-specific fields should be present
                if 'capacity' in specs:
                    assert specs['capacity'] != ""
                if 'frequency' in specs:
                    assert specs['frequency'] != ""
                if 'type' in specs:
                    assert specs['type'] != ""
            
            # Verify parsing works correctly
            parsed_product = crawler.parse_product(raw_product)
            assert parsed_product is not None
            assert parsed_product.source == source_name
            assert parsed_product.product_id == raw_product.product_id
            assert parsed_product.raw_data == raw_product.raw_data
    
    @given(
        num_products=st.integers(min_value=5, max_value=15),
        num_errors=st.integers(min_value=1, max_value=3)
    )
    @settings(max_examples=10)
    def test_crawler_handles_parsing_errors_gracefully(self, num_products, num_errors):
        """
        Test that crawler continues processing when some products fail to parse.
        This validates the error handling continuity aspect of data extraction.
        """
        # Create products with unique IDs
        products_data = []
        for i in range(num_products):
            products_data.append({
                'product_id': f'PROD_{i:04d}',
                'brand': 'CORSAIR',
                'title': f'CORSAIR DDR4 Memory {i}',
                'price': 99.99,
                'original_price': 129.99,
                'url': f'https://example.com/product/PROD_{i:04d}',
                'specifications': {
                    'capacity': '16GB',
                    'frequency': '3200MHz',
                    'type': 'DDR4'
                }
            })
        
        # Select products that should cause parsing errors
        error_indices = list(range(min(num_errors, num_products)))
        error_product_ids = [products_data[i]['product_id'] for i in error_indices]
        
        config = {'parsing_errors': error_product_ids}
        crawler = MockCrawler('test_source', config, products_data)
        
        # Execute the crawler
        result = crawler.execute()
        
        # Verify crawl completed (even with errors)
        assert result.success == True
        assert result.source == 'test_source'
        assert result.products_found == num_products
        
        # Verify that successful products were extracted
        expected_successful = num_products - len(error_indices)
        assert result.products_extracted == expected_successful
        
        # Verify errors were recorded
        assert len(result.errors) == len(error_indices)
        
        # Verify error messages contain product IDs
        for error_msg in result.errors:
            assert any(product_id in error_msg for product_id in error_product_ids)
    
    @given(
        products_data=st.lists(valid_product_data_strategy(), min_size=1, max_size=10)
    )
    @settings(max_examples=10)
    def test_crawler_handles_fetch_failures(self, products_data):
        """
        Test that crawler handles fetch failures appropriately.
        """
        config = {'should_fail': True}
        crawler = MockCrawler('test_source', config, products_data)
        
        # Execute the crawler
        result = crawler.execute()
        
        # Verify crawl failed appropriately
        assert result.success == False
        assert result.source == 'test_source'
        assert result.products_found == 0
        assert result.products_extracted == 0
        assert len(result.errors) > 0
        
        # Verify error was recorded
        assert any('Mock fetch failure' in error for error in result.errors)
        
        # Verify timing information is still present
        assert result.started_at is not None
        assert result.completed_at is not None
        assert result.duration_seconds is not None
    
    @given(
        products_data=st.lists(valid_product_data_strategy(), min_size=1, max_size=5)
    )
    @settings(max_examples=10)
    def test_crawler_validates_required_fields(self, products_data):
        """
        Test that crawler validates required fields are present in product data.
        """
        # Create products with missing required fields
        incomplete_products = []
        for product in products_data:
            # Randomly remove required fields
            incomplete_product = product.copy()
            if len(incomplete_product) > 1:  # Ensure we don't remove all fields
                # Remove 'brand' field to make it incomplete
                if 'brand' in incomplete_product:
                    del incomplete_product['brand']
            incomplete_products.append(incomplete_product)
        
        crawler = MockCrawler('test_source', {}, incomplete_products)
        
        # Execute the crawler
        result = crawler.execute()
        
        # Should complete but with parsing errors for incomplete products
        assert result.success == True
        assert result.products_found == len(incomplete_products)
        
        # All products should fail parsing due to missing required fields
        assert result.products_extracted == 0
        assert len(result.errors) == len(incomplete_products)
        
        # Verify error messages mention missing fields
        for error_msg in result.errors:
            assert 'Missing required fields' in error_msg
    
    def test_crawler_timestamp_consistency(self):
        """
        Test that crawler timestamps are consistent and reasonable.
        """
        products_data = [
            {
                'product_id': 'test_001',
                'brand': 'CORSAIR',
                'title': 'CORSAIR DDR4 16GB Memory',
                'price': 99.99,
                'url': 'https://example.com/test_001'
            }
        ]
        
        crawler = MockCrawler('test_source', {}, products_data)
        
        # Use a wider time window to account for execution time
        from datetime import timedelta
        start_time = datetime.now() - timedelta(seconds=1)
        result = crawler.execute()
        end_time = datetime.now() + timedelta(seconds=1)
        
        # Verify timing is reasonable
        assert start_time <= result.started_at <= end_time
        assert start_time <= result.completed_at <= end_time
        assert result.started_at <= result.completed_at
        
        # Verify products have reasonable timestamps
        raw_products = crawler.fetch_products()
        for product in raw_products:
            # Timestamps should be within a reasonable window
            assert start_time <= product.timestamp <= end_time


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
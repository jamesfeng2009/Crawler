"""
Property-based tests for error handling continuity.

**Feature: memory-price-monitor, Property 4: Error handling continuity**
**Validates: Requirements 1.5**
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


# Mock crawler implementation for testing error handling
class ErrorHandlingTestCrawler(BaseCrawler):
    """Mock crawler for testing error handling continuity."""
    
    def __init__(self, source_name: str, config: Dict[str, Any] = None, test_data: List[Dict[str, Any]] = None):
        super().__init__(source_name, config)
        self.test_data = test_data or []
        self.fetch_should_fail = config.get('fetch_should_fail', False) if config else False
        self.parsing_failures = config.get('parsing_failures', []) if config else []
        self.network_errors = config.get('network_errors', []) if config else []
        self.validation_errors = config.get('validation_errors', []) if config else []
    
    def fetch_products(self) -> List[RawProduct]:
        """Mock implementation that can simulate various failures."""
        if self.fetch_should_fail:
            raise CrawlerError("Simulated fetch failure")
        
        products = []
        for i, data in enumerate(self.test_data):
            # Skip products that should have network errors (simulate them not being fetched)
            if i in self.network_errors:
                continue
            
            product = RawProduct(
                source=self.source_name,
                product_id=data.get('product_id', f'test_{i}'),
                raw_data=data,
                url=data.get('url', f'https://example.com/product/{i}'),
                timestamp=datetime.now()
            )
            products.append(product)
        
        return products
    
    def parse_product(self, raw_data: Any) -> RawProduct:
        """Mock implementation that can simulate parsing failures."""
        if isinstance(raw_data, RawProduct):
            product_id = raw_data.product_id
            
            # Simulate parsing failures for specific products
            if product_id in self.parsing_failures:
                raise CrawlerError(f"Parsing failed for {product_id}")
            
            # Simulate validation errors
            if product_id in self.validation_errors:
                raise CrawlerError(f"Validation failed for {product_id}")
            
            # Check for required fields
            data = raw_data.raw_data
            if not data.get('brand') or not data.get('price'):
                raise CrawlerError(f"Missing required fields for {product_id}")
            
            return raw_data
        else:
            raise CrawlerError("Invalid raw data format")
    
    def get_price_history(self, product_id: str) -> List:
        """Mock implementation - not used in this test."""
        return []


# Hypothesis strategies for generating test data
@st.composite
def error_test_data_strategy(draw):
    """Generate test data for error handling tests."""
    num_products = draw(st.integers(min_value=3, max_value=20))
    
    products = []
    for i in range(num_products):
        # Some products may have missing fields to test validation
        has_brand = draw(st.booleans())
        has_price = draw(st.booleans())
        
        product = {
            'product_id': f'TEST_{i:03d}',
            'url': f'https://example.com/product/TEST_{i:03d}'
        }
        
        if has_brand:
            product['brand'] = draw(st.sampled_from(['CORSAIR', 'KINGSTON', 'G.SKILL']))
        
        if has_price:
            product['price'] = draw(st.floats(min_value=10.0, max_value=1000.0))
        
        # Add some additional fields
        product['title'] = f'Memory Product {i}'
        product['capacity'] = draw(st.sampled_from(['8GB', '16GB', '32GB']))
        
        products.append(product)
    
    return products


@st.composite
def error_configuration_strategy(draw):
    """Generate error configuration for testing."""
    # Generate indices for different types of errors
    parsing_failures = draw(st.lists(
        st.text(min_size=7, max_size=7, alphabet='TEST_0123456789'), 
        max_size=5
    ))
    
    validation_errors = draw(st.lists(
        st.text(min_size=7, max_size=7, alphabet='TEST_0123456789'), 
        max_size=3
    ))
    
    # Network errors will be filtered based on actual test data size
    network_errors = draw(st.lists(
        st.integers(min_value=0, max_value=19), 
        max_size=3
    ))
    
    return {
        'parsing_failures': parsing_failures,
        'validation_errors': validation_errors,
        'network_errors': network_errors,
        'fetch_should_fail': draw(st.booleans())
    }


class TestErrorHandlingContinuity:
    """Test error handling continuity property."""
    
    @given(
        num_products=st.integers(min_value=5, max_value=15),
        num_errors=st.integers(min_value=1, max_value=5)
    )
    @settings(max_examples=10)
    def test_error_handling_continuity(self, num_products, num_errors):
        """
        **Feature: memory-price-monitor, Property 4: Error handling continuity**
        
        For any parsing error encountered during crawling, the system should log the error 
        and continue processing remaining products without termination.
        
        **Validates: Requirements 1.5**
        """
        # Create test data with some valid and some invalid products
        test_data = []
        parsing_failures = []
        
        for i in range(num_products):
            product_id = f'TEST_{i:03d}'
            
            # Create products with missing fields to cause parsing errors
            if i < min(num_errors, num_products):
                # Missing required fields - will cause parsing error
                product = {
                    'product_id': product_id,
                    'title': f'Memory Product {i}',
                    'url': f'https://example.com/product/{product_id}'
                    # Missing 'brand' and 'price' - will cause parsing error
                }
            else:
                # Valid product
                product = {
                    'product_id': product_id,
                    'brand': 'CORSAIR',
                    'title': f'Memory Product {i}',
                    'price': 99.99,
                    'url': f'https://example.com/product/{product_id}'
                }
            
            test_data.append(product)
        
        # Create crawler with no special error configuration
        crawler = ErrorHandlingTestCrawler('test_source', {}, test_data)
        
        # Execute the crawler
        result = crawler.execute()
        
        # The crawler should complete successfully even with errors
        assert result.success == True
        assert result.source == 'test_source'
        assert result.products_found == num_products
        
        # Verify timing information is present
        assert result.started_at is not None
        assert result.completed_at is not None
        assert result.duration_seconds is not None
        assert result.duration_seconds >= 0
        assert result.completed_at >= result.started_at
        
        # Calculate expected results
        expected_errors = min(num_errors, num_products)
        expected_successful = num_products - expected_errors
        
        # Verify that successful products were extracted
        assert result.products_extracted == expected_successful
        
        # Verify that errors were recorded for failed products
        assert len(result.errors) == expected_errors
        
        # Verify that each error message contains relevant information
        for error_msg in result.errors:
            assert "Failed to parse product" in error_msg
            assert "Missing required fields" in error_msg
    
    @given(
        num_products=st.integers(min_value=10, max_value=50),
        error_rate=st.floats(min_value=0.1, max_value=0.8)
    )
    @settings(max_examples=10)
    def test_high_error_rate_continuity(self, num_products, error_rate):
        """
        Test that crawler continues processing even with high error rates.
        """
        # Create test data
        test_data = []
        parsing_failures = []
        
        for i in range(num_products):
            product_id = f'TEST_{i:03d}'
            product = {
                'product_id': product_id,
                'brand': 'CORSAIR',
                'title': f'Memory Product {i}',
                'price': 99.99,
                'url': f'https://example.com/product/{product_id}'
            }
            test_data.append(product)
            
            # Add to parsing failures based on error rate
            if i < int(num_products * error_rate):
                parsing_failures.append(product_id)
        
        config = {'parsing_failures': parsing_failures}
        crawler = ErrorHandlingTestCrawler('test_source', config, test_data)
        
        # Execute the crawler
        result = crawler.execute()
        
        # Should complete successfully despite high error rate
        assert result.success == True
        assert result.products_found == num_products
        
        # Verify error handling
        expected_errors = len(parsing_failures)
        expected_successful = num_products - expected_errors
        
        assert result.products_extracted == expected_successful
        assert len(result.errors) == expected_errors
        
        # Verify all errors are properly logged
        for error_msg in result.errors:
            assert "Failed to parse product" in error_msg
            assert "Parsing failed" in error_msg
    
    def test_complete_fetch_failure_handling(self):
        """
        Test that complete fetch failure is handled gracefully.
        """
        config = {'fetch_should_fail': True}
        crawler = ErrorHandlingTestCrawler('test_source', config, [])
        
        # Execute the crawler
        result = crawler.execute()
        
        # Should fail gracefully
        assert result.success == False
        assert result.products_found == 0
        assert result.products_extracted == 0
        assert len(result.errors) > 0
        
        # Verify error message
        assert any("Simulated fetch failure" in error for error in result.errors)
        
        # Verify timing information is still present
        assert result.started_at is not None
        assert result.completed_at is not None
        assert result.duration_seconds is not None
    
    def test_mixed_error_types_continuity(self):
        """
        Test continuity with mixed error types (parsing, validation, missing fields).
        """
        test_data = [
            # Valid product
            {
                'product_id': 'VALID_001',
                'brand': 'CORSAIR',
                'price': 99.99,
                'title': 'Valid Memory'
            },
            # Product that will cause parsing error
            {
                'product_id': 'PARSE_ERR',
                'brand': 'KINGSTON',
                'price': 79.99,
                'title': 'Parse Error Memory'
            },
            # Product that will cause validation error
            {
                'product_id': 'VALID_ERR',
                'brand': 'G.SKILL',
                'price': 129.99,
                'title': 'Validation Error Memory'
            },
            # Product with missing brand (missing field error)
            {
                'product_id': 'MISSING_001',
                'price': 59.99,
                'title': 'Missing Brand Memory'
            },
            # Product with missing price (missing field error)
            {
                'product_id': 'MISSING_002',
                'brand': 'CRUCIAL',
                'title': 'Missing Price Memory'
            },
            # Another valid product
            {
                'product_id': 'VALID_002',
                'brand': 'SAMSUNG',
                'price': 149.99,
                'title': 'Another Valid Memory'
            }
        ]
        
        config = {
            'parsing_failures': ['PARSE_ERR'],
            'validation_errors': ['VALID_ERR']
        }
        
        crawler = ErrorHandlingTestCrawler('test_source', config, test_data)
        
        # Execute the crawler
        result = crawler.execute()
        
        # Should complete successfully
        assert result.success == True
        assert result.products_found == 6
        
        # Should extract only the valid products (2 valid products)
        assert result.products_extracted == 2
        
        # Should have 4 errors (1 parsing + 1 validation + 2 missing fields)
        assert len(result.errors) == 4
        
        # Verify error types are present
        error_messages = ' '.join(result.errors)
        assert 'Parsing failed' in error_messages
        assert 'Validation failed' in error_messages
        assert 'Missing required fields' in error_messages
    
    @given(
        num_products=st.integers(min_value=5, max_value=20)
    )
    @settings(max_examples=10)
    def test_no_errors_baseline(self, num_products):
        """
        Test baseline case with no errors to ensure normal operation.
        """
        # Create valid test data
        test_data = []
        for i in range(num_products):
            product = {
                'product_id': f'VALID_{i:03d}',
                'brand': 'CORSAIR',
                'price': 99.99 + i,
                'title': f'Valid Memory Product {i}',
                'url': f'https://example.com/product/VALID_{i:03d}'
            }
            test_data.append(product)
        
        # No error configuration
        config = {}
        crawler = ErrorHandlingTestCrawler('test_source', config, test_data)
        
        # Execute the crawler
        result = crawler.execute()
        
        # Should complete successfully with no errors
        assert result.success == True
        assert result.products_found == num_products
        assert result.products_extracted == num_products
        assert len(result.errors) == 0
        
        # Verify timing information
        assert result.started_at is not None
        assert result.completed_at is not None
        assert result.duration_seconds is not None
        assert result.duration_seconds >= 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
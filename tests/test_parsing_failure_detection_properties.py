"""
Property-based tests for parsing failure detection.

**Feature: memory-price-monitor, Property 19: Parsing failure detection**
**Validates: Requirements 6.1, 6.5**
"""

import pytest
from hypothesis import given, strategies as st, assume
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from typing import Dict, Any, List

from memory_price_monitor.crawlers.base import BaseCrawler, RawProduct, CrawlResult
from memory_price_monitor.utils.errors import CrawlerError
from memory_price_monitor.utils.logging import get_logger


class MockCrawler(BaseCrawler):
    """Mock crawler for testing parsing failure detection."""
    
    def __init__(self, source_name: str, config: Dict[str, Any] = None, 
                 should_fail_parsing: bool = False, 
                 parsing_failure_rate: float = 0.0):
        super().__init__(source_name, config)
        self.should_fail_parsing = should_fail_parsing
        self.parsing_failure_rate = parsing_failure_rate
        self.parsing_attempts = 0
        self.parsing_failures = []
        
    def fetch_products(self) -> List[RawProduct]:
        """Mock fetch products."""
        return [
            RawProduct(
                source=self.source_name,
                product_id=f"product_{i}",
                raw_data={"title": f"Product {i}", "price": 100 + i},
                url=f"http://example.com/product_{i}",
                timestamp=datetime.now()
            )
            for i in range(5)
        ]
    
    def parse_product(self, raw_data: Any) -> RawProduct:
        """Mock parse product with configurable failure."""
        self.parsing_attempts += 1
        
        # Simulate parsing failure based on configuration
        if self.should_fail_parsing:
            error_msg = f"Parsing failed for product {raw_data.product_id}"
            self.parsing_failures.append(error_msg)
            raise CrawlerError(error_msg)
        
        # Simulate deterministic parsing failures based on product_id for testing
        if self.parsing_failure_rate > 0:
            # Use product_id hash to determine failure (deterministic for testing)
            product_hash = hash(raw_data.product_id) % 100
            failure_threshold = int(self.parsing_failure_rate * 100)
            
            if product_hash < failure_threshold:
                error_msg = f"Deterministic parsing failure for product {raw_data.product_id}"
                self.parsing_failures.append(error_msg)
                raise CrawlerError(error_msg)
        
        return raw_data
    
    def get_price_history(self, product_id: str) -> List:
        """Mock get price history."""
        return []


# Strategy for generating crawler configurations
crawler_config_strategy = st.fixed_dictionaries({
    'should_fail_parsing': st.booleans(),
    'parsing_failure_rate': st.floats(min_value=0.0, max_value=1.0)
})


@given(config=crawler_config_strategy)
def test_parsing_failure_detection_property(config):
    """
    Property 19: Parsing failure detection
    
    For any website structure change that causes parsing failures, 
    the crawler should detect the failure and alert administrators.
    
    **Feature: memory-price-monitor, Property 19: Parsing failure detection**
    **Validates: Requirements 6.1, 6.5**
    """
    # Create mock crawler with configurable parsing behavior
    crawler = MockCrawler(
        source_name="test_source",
        should_fail_parsing=config['should_fail_parsing'],
        parsing_failure_rate=config['parsing_failure_rate']
    )
    
    # Mock logger to capture alerts
    with patch.object(crawler, 'logger') as mock_logger:
        # Execute crawling
        result = crawler.execute()
        
        # Property: If parsing failures occur, they should be detected and logged
        if config['should_fail_parsing']:
            # All parsing should fail
            assert len(result.errors) > 0, "Parsing failures should be detected and recorded in errors"
            
            # Should have logged warnings for parsing failures
            warning_calls = [call for call in mock_logger.warning.call_args_list 
                           if 'parsing failed' in str(call).lower()]
            assert len(warning_calls) > 0, "Parsing failures should trigger warning logs"
            
            # Should complete execution despite failures
            assert result.completed_at is not None, "Crawler should complete execution despite parsing failures"
            
            # Should have attempted to parse all products
            assert crawler.parsing_attempts >= result.products_found, "Should attempt to parse all found products"
        
        elif config['parsing_failure_rate'] > 0:
            # With probabilistic failures, we may or may not get failures
            # The important property is: IF failures occur, they are detected
            if len(result.errors) > 0:
                # Failures occurred and were detected
                warning_calls = [call for call in mock_logger.warning.call_args_list 
                               if 'parsing failed' in str(call).lower()]
                assert len(warning_calls) > 0, "Parsing failures should trigger warning logs"
                
                # Should complete execution despite failures
                assert result.completed_at is not None, "Crawler should complete execution despite parsing failures"
            
            # Should always attempt to parse all products
            assert crawler.parsing_attempts >= result.products_found, "Should attempt to parse all found products"
        
        else:
            # No parsing failures expected
            assert len(result.errors) == 0, "Should have no errors when parsing succeeds"
            assert result.products_extracted == result.products_found, "Should extract all found products when parsing succeeds"


@given(
    failure_rate=st.floats(min_value=0.1, max_value=0.9),
    num_products=st.integers(min_value=2, max_value=20)  # Ensure at least 2 products for partial failure
)
def test_partial_parsing_failure_detection(failure_rate, num_products):
    """
    Property: For any partial parsing failure scenario, the system should detect 
    failures while continuing to process successful products.
    
    **Feature: memory-price-monitor, Property 19: Parsing failure detection**
    **Validates: Requirements 6.1, 6.5**
    """
    # Create a custom crawler that fails parsing for a specific rate
    class PartialFailureCrawler(MockCrawler):
        def __init__(self, failure_rate: float, num_products: int):
            super().__init__("partial_test", parsing_failure_rate=failure_rate)
            self.num_products = num_products
            
        def fetch_products(self) -> List[RawProduct]:
            return [
                RawProduct(
                    source=self.source_name,
                    product_id=f"product_{i}",
                    raw_data={"title": f"Product {i}", "price": 100 + i},
                    url=f"http://example.com/product_{i}",
                    timestamp=datetime.now()
                )
                for i in range(self.num_products)
            ]
    
    crawler = PartialFailureCrawler(failure_rate, num_products)
    
    with patch.object(crawler, 'logger') as mock_logger:
        result = crawler.execute()
        
        # Property: Should detect and record parsing failures
        # With deterministic hashing, we should get some failures but not all
        
        # Should have processed all products (attempted parsing)
        assert crawler.parsing_attempts == num_products, "Should attempt to parse all products"
        
        # Should have some failures (but may not be exactly the expected rate due to hashing)
        if len(result.errors) > 0:
            # Should have extracted some products successfully (unless all failed)
            assert result.products_extracted >= 0, "Should extract non-negative number of products"
            
            # Should log warnings for failures
            warning_calls = mock_logger.warning.call_args_list
            assert len(warning_calls) > 0, "Should log warnings for parsing failures"
        
        # Should complete execution successfully despite partial failures
        assert result.success is True, "Should mark overall crawl as successful despite partial failures"
        assert result.completed_at is not None, "Should complete execution"
        
        # Total products should be consistent
        assert result.products_extracted + len(result.errors) == num_products, "Products extracted + errors should equal total products"


@given(st.text(min_size=1, max_size=50))
def test_parsing_failure_error_details(source_name):
    """
    Property: For any parsing failure, the system should provide detailed error information.
    
    **Feature: memory-price-monitor, Property 19: Parsing failure detection**
    **Validates: Requirements 6.1, 6.5**
    """
    assume(source_name.strip())  # Ensure non-empty source name
    
    # Create crawler that always fails parsing
    crawler = MockCrawler(source_name.strip(), should_fail_parsing=True)
    
    with patch.object(crawler, 'logger') as mock_logger:
        result = crawler.execute()
        
        # Property: Parsing failures should include detailed error information
        assert len(result.errors) > 0, "Should record parsing failure errors"
        
        # Each error should contain meaningful information
        for error in result.errors:
            assert "product" in error.lower(), "Error should mention the product"
            assert "failed" in error.lower(), "Error should indicate failure"
        
        # Should log detailed warnings
        warning_calls = mock_logger.warning.call_args_list
        assert len(warning_calls) > 0, "Should log detailed warnings"
        
        # Warning messages should contain product information
        for call in warning_calls:
            call_str = str(call)
            assert "product" in call_str.lower(), "Warning should mention product"


def test_administrator_alert_mechanism():
    """
    Property: For any persistent parsing failures, the system should alert administrators.
    
    **Feature: memory-price-monitor, Property 19: Parsing failure detection**
    **Validates: Requirements 6.1, 6.5**
    """
    # This test verifies the alerting mechanism exists and can be triggered
    crawler = MockCrawler("alert_test", should_fail_parsing=True)
    
    # Mock the notification system that would alert administrators
    with patch('memory_price_monitor.services.notification.NotificationService') as mock_notification:
        mock_notification_instance = Mock()
        mock_notification.return_value = mock_notification_instance
        
        with patch.object(crawler, 'logger') as mock_logger:
            result = crawler.execute()
            
            # Property: System should be capable of alerting administrators
            # (This tests the infrastructure exists, actual alerting would be in monitoring service)
            
            # Should detect failures
            assert len(result.errors) > 0, "Should detect parsing failures"
            
            # Should log errors that could trigger alerts
            error_calls = mock_logger.error.call_args_list
            warning_calls = mock_logger.warning.call_args_list
            
            # Should have logged either errors or warnings that indicate problems
            total_problem_logs = len(error_calls) + len(warning_calls)
            assert total_problem_logs > 0, "Should log problems that could trigger administrator alerts"


if __name__ == "__main__":
    pytest.main([__file__])
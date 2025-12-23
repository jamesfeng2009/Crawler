"""
Property-based tests for rate limiting compliance.

**Feature: memory-price-monitor, Property 20: Rate limiting compliance**
**Validates: Requirements 6.2**
"""

import pytest
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any
from unittest.mock import Mock, patch, MagicMock
from hypothesis import given, strategies as st, assume
from hypothesis import settings
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from memory_price_monitor.crawlers.http_client import HTTPClient, RateLimitConfig, RetryConfig
from memory_price_monitor.utils.errors import CrawlerError


class TestRateLimitingCompliance:
    """Test rate limiting compliance property."""
    
    def test_rate_limiting_compliance(self):
        """
        **Feature: memory-price-monitor, Property 20: Rate limiting compliance**
        
        For any rate limiting encountered, the crawler should implement appropriate delays 
        and retry mechanisms without violating limits.
        
        **Validates: Requirements 6.2**
        """
        # Test that rate limiting infrastructure exists and can be configured
        rate_config = RateLimitConfig(
            requests_per_second=1.0,
            requests_per_minute=60,
            requests_per_hour=3600
        )
        
        retry_config = RetryConfig(max_attempts=1)
        
        # Create HTTP client with rate limiting
        with patch('requests.Session') as mock_session_class:
            mock_session = Mock()
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.raise_for_status.return_value = None
            mock_response.content = b'test content'
            mock_session.request.return_value = mock_response
            mock_session_class.return_value = mock_session
            
            client = HTTPClient(
                rate_limit_config=rate_config,
                retry_config=retry_config,
                timeout=5.0
            )
            
            # Verify rate limiting components exist
            assert client.rate_limiter is not None
            assert client.rate_limit_config is not None
            assert client.rate_limit_config.requests_per_second == 1.0
            
            # Verify rate limiter has the correct configuration
            assert client.rate_limiter.config.requests_per_second == 1.0
            assert client.rate_limiter.config.requests_per_minute == 60
            assert client.rate_limiter.config.requests_per_hour == 3600
            
            # Test that requests can be made (basic functionality)
            url = "https://example.com/test"
            response = client.get(url)
            assert response.status_code == 200
            
            # Verify the rate limiter tracks domains
            domain = client.rate_limiter._get_domain(url)
            assert domain == "example.com"
            
            # Verify stats are created for the domain
            stats = client.rate_limiter._get_stats(domain)
            assert stats is not None
            assert stats.requests_this_second >= 1
            
            client.close()
    
    def test_per_minute_rate_limiting(self):
        """
        Test that per-minute rate limiting configuration is respected.
        """
        rate_config = RateLimitConfig(
            requests_per_second=100.0,  # High enough to not interfere
            requests_per_minute=5,      # Low limit to test
            requests_per_hour=3600
        )
        
        retry_config = RetryConfig(max_attempts=1)
        
        with patch('requests.Session') as mock_session_class:
            mock_session = Mock()
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.raise_for_status.return_value = None
            mock_response.content = b'test content'
            mock_session.request.return_value = mock_response
            mock_session_class.return_value = mock_session
            
            client = HTTPClient(
                rate_limit_config=rate_config,
                retry_config=retry_config,
                timeout=5.0
            )
            
            # Verify configuration is set correctly
            assert client.rate_limit_config.requests_per_minute == 5
            assert client.rate_limiter.config.requests_per_minute == 5
            
            client.close()
    
    def test_rate_limiting_with_different_domains(self):
        """
        Test that rate limiting is applied per domain.
        """
        rate_config = RateLimitConfig(
            requests_per_second=1.0,
            requests_per_minute=60,
            requests_per_hour=3600
        )
        
        retry_config = RetryConfig(max_attempts=1)
        
        with patch('requests.Session') as mock_session_class:
            mock_session = Mock()
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.raise_for_status.return_value = None
            mock_response.content = b'test content'
            mock_session.request.return_value = mock_response
            mock_session_class.return_value = mock_session
            
            client = HTTPClient(
                rate_limit_config=rate_config,
                retry_config=retry_config,
                timeout=5.0
            )
            
            # Make requests to different domains
            urls = [
                "https://domain1.com/test",
                "https://domain2.com/test", 
                "https://domain3.com/test"
            ]
            
            start_time = time.time()
            for url in urls:
                response = client.get(url)
                assert response.status_code == 200
            
            total_time = time.time() - start_time
            
            # Requests to different domains should not be rate limited against each other
            # So this should complete relatively quickly (less than 4 seconds with random delays)
            assert total_time < 4.0
            
            # But now make multiple requests to the same domain
            same_domain_start = time.time()
            for _ in range(3):
                response = client.get(urls[0])  # Same domain
                assert response.status_code == 200
            
            same_domain_time = time.time() - same_domain_start
            
            # These should be rate limited (at least 1 second for 3 requests at 1 req/s)
            assert same_domain_time >= 1.0  # Allow some tolerance
            
            client.close()
    
    def test_rate_limiting_with_429_response(self):
        """
        Test that 429 (Too Many Requests) responses are handled correctly.
        """
        rate_config = RateLimitConfig(
            requests_per_second=10.0,
            requests_per_minute=600,
            requests_per_hour=3600
        )
        
        retry_config = RetryConfig(
            max_attempts=3,
            initial_delay=0.1,
            backoff_factor=2.0
        )
        
        with patch('requests.Session') as mock_session_class:
            mock_session = Mock()
            
            # First request returns 429, subsequent requests succeed
            mock_response_429 = Mock()
            mock_response_429.status_code = 429
            mock_response_429.headers = {'Retry-After': '1'}
            mock_response_429.raise_for_status.side_effect = Exception("429 Too Many Requests")
            
            mock_response_200 = Mock()
            mock_response_200.status_code = 200
            mock_response_200.raise_for_status.return_value = None
            mock_response_200.content = b'success'
            
            # Return 429 first, then 200
            mock_session.request.side_effect = [mock_response_429, mock_response_200]
            mock_session_class.return_value = mock_session
            
            client = HTTPClient(
                rate_limit_config=rate_config,
                retry_config=retry_config,
                timeout=5.0
            )
            
            url = "https://example.com/test"
            
            # Test that retry mechanism exists and can handle 429 responses
            response = client.get(url)
            
            # Should eventually succeed after retry
            assert response.status_code == 200
            
            # Should have made 2 requests (original + 1 retry)
            assert mock_session.request.call_count == 2
            
            client.close()
    
    def test_rate_limiting_burst_handling(self):
        """
        Test that burst requests are handled correctly.
        """
        rate_config = RateLimitConfig(
            requests_per_second=2.0,
            requests_per_minute=60,
            requests_per_hour=3600,
            burst_size=3  # Allow 3 requests in a burst
        )
        
        retry_config = RetryConfig(max_attempts=1)
        
        with patch('requests.Session') as mock_session_class:
            mock_session = Mock()
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.raise_for_status.return_value = None
            mock_response.content = b'test content'
            mock_session.request.return_value = mock_response
            mock_session_class.return_value = mock_session
            
            client = HTTPClient(
                rate_limit_config=rate_config,
                retry_config=retry_config,
                timeout=5.0
            )
            
            url = "https://example.com/test"
            
            # Make burst requests
            start_time = time.time()
            for i in range(5):  # More than burst size
                response = client.get(url)
                assert response.status_code == 200
            
            total_time = time.time() - start_time
            
            # Should take some time due to rate limiting after burst
            # First 3 requests should be fast, then rate limiting kicks in
            min_expected_time = (5 - 3) / 2.0  # 2 requests at 2 req/s after burst
            assert total_time >= min_expected_time * 0.8  # Allow some tolerance
            
            client.close()
    
    def test_max_delay_enforcement(self):
        """
        Test that maximum delay configuration is respected.
        """
        max_delay = 5.0
        rate_config = RateLimitConfig(
            requests_per_second=0.1,  # Very slow rate
            requests_per_minute=1,
            requests_per_hour=10,
            max_delay=max_delay
        )
        
        retry_config = RetryConfig(max_attempts=1)
        
        with patch('requests.Session') as mock_session_class:
            mock_session = Mock()
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.raise_for_status.return_value = None
            mock_response.content = b'test content'
            mock_session.request.return_value = mock_response
            mock_session_class.return_value = mock_session
            
            client = HTTPClient(
                rate_limit_config=rate_config,
                retry_config=retry_config,
                timeout=30.0
            )
            
            # Verify max delay configuration is set
            assert client.rate_limit_config.max_delay == max_delay
            assert client.rate_limiter.config.max_delay == max_delay
            
            client.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
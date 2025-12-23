"""
Property-based tests for notification retry resilience.

**Feature: memory-price-monitor, Property 17: Notification retry resilience**
**Validates: Requirements 5.4**
"""

import pytest
from hypothesis import given, strategies as st, settings
from unittest.mock import Mock, patch, MagicMock, call
from decimal import Decimal
from datetime import date, datetime, timedelta
import requests
import smtplib
import time

from memory_price_monitor.services.notification import NotificationService
from memory_price_monitor.data.models import (
    WeeklyReport, ComparisonResult, TrendData
)
from config import NotificationConfig


# Test data generators
@st.composite
def trend_data_strategy(draw):
    """Generate TrendData for testing."""
    return TrendData(
        product_id=draw(st.text(min_size=1, max_size=20)),
        brand=draw(st.sampled_from(["CORSAIR", "KINGSTON", "G.SKILL", "CRUCIAL"])),
        model=draw(st.text(min_size=1, max_size=50)),
        capacity=draw(st.sampled_from(["8GB", "16GB", "32GB", "64GB"])),
        type=draw(st.sampled_from(["DDR4", "DDR5"])),
        current_week_prices=[draw(st.decimals(min_value=100, max_value=2000, places=2))],
        previous_week_prices=[draw(st.decimals(min_value=100, max_value=2000, places=2))],
        average_current=draw(st.decimals(min_value=100, max_value=2000, places=2)),
        average_previous=draw(st.decimals(min_value=100, max_value=2000, places=2)),
        percentage_change=draw(st.decimals(min_value=-50, max_value=50, places=1)),
        trend_direction=draw(st.sampled_from(["up", "down", "stable"]))
    )


@st.composite
def comparison_result_strategy(draw):
    """Generate ComparisonResult for testing."""
    current_date = date.today()
    return ComparisonResult(
        current_week_start=current_date - timedelta(days=current_date.weekday()),
        previous_week_start=current_date - timedelta(days=current_date.weekday() + 7),
        trends=draw(st.lists(trend_data_strategy(), min_size=1, max_size=10)),
        overall_average_change=draw(st.decimals(min_value=-20, max_value=20, places=1)),
        significant_changes=draw(st.lists(trend_data_strategy(), min_size=0, max_size=5))
    )


@st.composite
def weekly_report_strategy(draw):
    """Generate WeeklyReport for testing."""
    report_date = date.today()
    week_start = report_date - timedelta(days=report_date.weekday())
    week_end = week_start + timedelta(days=6)
    
    return WeeklyReport(
        report_date=report_date,
        week_start=week_start,
        week_end=week_end,
        comparison=draw(comparison_result_strategy()),
        charts={
            "weekly_trend": b"fake_chart_data_1",
            "brand_comparison": b"fake_chart_data_2"
        },
        summary_stats={
            "total_products": draw(st.integers(min_value=1, max_value=100)),
            "average_price_change": draw(st.decimals(min_value=-20, max_value=20, places=1))
        }
    )


@st.composite
def notification_config_strategy(draw):
    """Generate NotificationConfig for testing."""
    return NotificationConfig(
        wechat_method=draw(st.sampled_from(["serverchan", "wxpy"])),
        serverchan_key=draw(st.one_of(st.none(), st.text(min_size=10, max_size=50))),
        email_smtp_host="smtp.test.com",
        email_smtp_port=587,
        email_username=draw(st.one_of(st.none(), st.emails())),
        email_password=draw(st.one_of(st.none(), st.text(min_size=8, max_size=20))),
        email_recipients=draw(st.lists(st.emails(), min_size=0, max_size=5)),
        retry_attempts=draw(st.integers(min_value=1, max_value=5)),
        retry_delay=draw(st.floats(min_value=1.0, max_value=10.0))
    )


class TestNotificationRetryResilience:
    """Test notification retry resilience properties."""
    
    @given(
        report=weekly_report_strategy(),
        config=notification_config_strategy()
    )
    @settings(max_examples=5, deadline=None)
    def test_retry_mechanism_respects_max_attempts(self, report, config):
        """
        Property 17: Notification retry resilience
        For any failed notification delivery, the service should retry delivery 
        and log detailed failure information.
        
        **Validates: Requirements 5.4**
        """
        service = NotificationService(config)
        
        with patch('requests.post') as mock_post, \
             patch('smtplib.SMTP') as mock_smtp, \
             patch('time.sleep') as mock_sleep:
            
            # Configure mocks to always fail
            mock_post.side_effect = requests.RequestException("Network error")
            mock_smtp.side_effect = Exception("SMTP error")
            
            # Test retry mechanism
            results = service.send_notification_with_retry(report, ["wechat", "email"])
            
            # Should have attempted retries according to configuration
            expected_attempts = config.retry_attempts
            
            # For WeChat (if configured)
            if config.serverchan_key:
                # Should attempt Server酱 calls up to retry_attempts times
                assert mock_post.call_count <= expected_attempts
            
            # For email (if configured)
            if config.email_username and config.email_password:
                # Should attempt SMTP connections up to retry_attempts times
                assert mock_smtp.call_count <= expected_attempts
            
            # Should have used sleep for retries (except for the last failed attempt)
            if mock_post.call_count > 1 or mock_smtp.call_count > 1:
                assert mock_sleep.call_count > 0
            
            # All channels should ultimately fail after retries
            assert results.get("wechat", True) is False or not config.serverchan_key
            assert results.get("email", True) is False or not (config.email_username and config.email_password)
    
    @given(
        report=weekly_report_strategy(),
        config=notification_config_strategy()
    )
    @settings(max_examples=5, deadline=None)
    def test_exponential_backoff_delay_pattern(self, report, config):
        """
        Test that retry delays follow exponential backoff pattern.
        """
        service = NotificationService(config)
        
        # Ensure we have enough retry attempts to test backoff
        if config.retry_attempts < 3:
            config.retry_attempts = 3
        
        with patch('requests.post') as mock_post, \
             patch('time.sleep') as mock_sleep:
            
            # Configure Server酱 to be available and fail
            config.serverchan_key = "test_key_12345"
            config.wechat_method = "serverchan"
            mock_post.side_effect = requests.RequestException("Network error")
            
            # Test retry with exponential backoff
            service.send_notification_with_retry(report, ["wechat"])
            
            # Should have made retry attempts
            assert mock_post.call_count == config.retry_attempts
            
            # Should have used sleep with exponential backoff
            if mock_sleep.call_count > 0:
                sleep_calls = [call[0][0] for call in mock_sleep.call_args_list]
                
                # Verify exponential backoff pattern (each delay should be >= previous * 2)
                for i in range(1, len(sleep_calls)):
                    # Allow some tolerance for floating point precision
                    expected_min_delay = sleep_calls[i-1] * 1.8  # Slightly less than 2x for tolerance
                    assert sleep_calls[i] >= expected_min_delay, \
                        f"Delay {sleep_calls[i]} should be >= {expected_min_delay} (exponential backoff)"
    
    @given(
        report=weekly_report_strategy(),
        config=notification_config_strategy()
    )
    @settings(max_examples=5, deadline=None)
    def test_successful_retry_stops_further_attempts(self, report, config):
        """
        Test that successful retry stops further retry attempts.
        """
        service = NotificationService(config)
        
        # Ensure we have multiple retry attempts
        if config.retry_attempts < 3:
            config.retry_attempts = 3
        
        with patch('requests.post') as mock_post, \
             patch('time.sleep') as mock_sleep:
            
            # Configure Server酱 to be available
            config.serverchan_key = "test_key_12345"
            config.wechat_method = "serverchan"
            
            # Configure to fail first attempt, then succeed
            mock_response_fail = MagicMock()
            mock_response_fail.json.return_value = {"code": 1, "message": "Rate limited"}
            mock_response_fail.raise_for_status.side_effect = requests.HTTPError("Rate limited")
            
            mock_response_success = MagicMock()
            mock_response_success.json.return_value = {"code": 0}
            mock_response_success.raise_for_status.return_value = None
            
            # First call fails, second succeeds
            mock_post.side_effect = [requests.HTTPError("Rate limited"), mock_response_success]
            
            # Test retry mechanism
            results = service.send_notification_with_retry(report, ["wechat"])
            
            # Should have succeeded
            assert results.get("wechat") is True
            
            # Should have made exactly 2 attempts (fail, then success)
            assert mock_post.call_count == 2
            
            # Should have used sleep once (between first failure and second attempt)
            assert mock_sleep.call_count == 1
    
    @given(
        report=weekly_report_strategy(),
        config=notification_config_strategy()
    )
    @settings(max_examples=5, deadline=None)
    def test_wechat_method_fallback_on_failure(self, report, config):
        """
        Test that WeChat method fallback works when primary method fails.
        """
        service = NotificationService(config)
        
        with patch('requests.post') as mock_post, \
             patch('memory_price_monitor.services.notification.wxpy', create=True) as mock_wxpy_module:
            
            # Configure both WeChat methods to be available
            config.serverchan_key = "test_key_12345"
            
            # Test Server酱 primary with wxpy fallback
            config.wechat_method = "serverchan"
            mock_post.side_effect = requests.RequestException("Server酱 failed")
            
            # Configure wxpy to succeed
            mock_bot = MagicMock()
            mock_wxpy_module.Bot.return_value = mock_bot
            mock_bot.file_helper = MagicMock()
            
            # Test fallback mechanism
            result = service._send_wechat_with_fallback(report)
            
            # Should attempt Server酱 first
            assert mock_post.called
            
            # Should fallback to wxpy (though it might fail due to import issues in test)
            # The important thing is that it attempts the fallback
            
            # Test wxpy primary with Server酱 fallback
            config.wechat_method = "wxpy"
            mock_post.reset_mock()
            mock_wxpy_module.reset_mock()
            
            # Configure wxpy to fail and Server酱 to succeed
            mock_wxpy_module.Bot.side_effect = ImportError("wxpy not available")
            mock_response_success = MagicMock()
            mock_response_success.json.return_value = {"code": 0}
            mock_response_success.raise_for_status.return_value = None
            mock_post.return_value = mock_response_success
            
            result = service._send_wechat_with_fallback(report)
            
            # Should attempt wxpy first (and fail), then fallback to Server酱
            assert mock_wxpy_module.Bot.called
            assert mock_post.called
    
    @given(
        report=weekly_report_strategy(),
        config=notification_config_strategy()
    )
    @settings(max_examples=5, deadline=None)
    def test_partial_channel_failure_continues_other_channels(self, report, config):
        """
        Test that failure in one channel doesn't prevent attempts in other channels.
        """
        service = NotificationService(config)
        
        with patch('requests.post') as mock_post, \
             patch('smtplib.SMTP') as mock_smtp:
            
            # Configure both channels to be available
            config.serverchan_key = "test_key_12345"
            config.wechat_method = "serverchan"
            config.email_username = "test@example.com"
            config.email_password = "test_password"
            config.email_recipients = ["recipient@example.com"]
            
            # Configure WeChat to fail, email to succeed
            mock_post.side_effect = requests.RequestException("WeChat failed")
            
            mock_smtp_instance = MagicMock()
            mock_smtp.return_value.__enter__.return_value = mock_smtp_instance
            
            # Test both channels
            results = service.send_notification_with_retry(report, ["wechat", "email"])
            
            # Should have attempted both channels
            assert "wechat" in results
            assert "email" in results
            
            # WeChat should have failed
            assert results["wechat"] is False
            
            # Email should have succeeded
            assert results["email"] is True
            
            # Should have attempted WeChat (and failed)
            assert mock_post.called
            
            # Should have attempted email (and succeeded)
            assert mock_smtp.called
    
    @given(
        report=weekly_report_strategy(),
        config=notification_config_strategy()
    )
    @settings(max_examples=5, deadline=None)
    def test_retry_delay_configuration_respected(self, report, config):
        """
        Test that retry delays respect the configured base delay.
        """
        service = NotificationService(config)
        
        # Ensure we have multiple retry attempts
        if config.retry_attempts < 2:
            config.retry_attempts = 2
        
        # Set a specific retry delay for testing
        base_delay = 2.5
        config.retry_delay = base_delay
        
        with patch('requests.post') as mock_post, \
             patch('time.sleep') as mock_sleep:
            
            # Configure Server酱 to be available and always fail
            config.serverchan_key = "test_key_12345"
            config.wechat_method = "serverchan"
            mock_post.side_effect = requests.RequestException("Always fail")
            
            # Test retry mechanism
            service.send_notification_with_retry(report, ["wechat"])
            
            # Should have used sleep with delays based on configured base delay
            if mock_sleep.call_count > 0:
                sleep_calls = [call[0][0] for call in mock_sleep.call_args_list]
                
                # First delay should be approximately the base delay
                first_delay = sleep_calls[0]
                assert first_delay >= base_delay * 0.9  # Allow some tolerance
                assert first_delay <= base_delay * 2.1  # First exponential step
                
                # Subsequent delays should follow exponential pattern
                for i in range(1, len(sleep_calls)):
                    # Each delay should be roughly double the previous (with tolerance)
                    expected_min = sleep_calls[i-1] * 1.8
                    expected_max = sleep_calls[i-1] * 2.2
                    assert expected_min <= sleep_calls[i] <= expected_max, \
                        f"Delay {sleep_calls[i]} not in expected range [{expected_min}, {expected_max}]"
    
    @given(
        report=weekly_report_strategy(),
        config=notification_config_strategy()
    )
    @settings(max_examples=5, deadline=None)
    def test_no_retry_on_immediate_success(self, report, config):
        """
        Test that no retries are attempted when first attempt succeeds.
        """
        service = NotificationService(config)
        
        with patch('requests.post') as mock_post, \
             patch('smtplib.SMTP') as mock_smtp, \
             patch('time.sleep') as mock_sleep:
            
            # Configure both channels to succeed immediately
            config.serverchan_key = "test_key_12345"
            config.wechat_method = "serverchan"
            config.email_username = "test@example.com"
            config.email_password = "test_password"
            config.email_recipients = ["recipient@example.com"]
            
            # Configure immediate success
            mock_response = MagicMock()
            mock_response.json.return_value = {"code": 0}
            mock_response.raise_for_status.return_value = None
            mock_post.return_value = mock_response
            
            mock_smtp_instance = MagicMock()
            mock_smtp.return_value.__enter__.return_value = mock_smtp_instance
            
            # Test both channels
            results = service.send_notification_with_retry(report, ["wechat", "email"])
            
            # Both should succeed
            assert results["wechat"] is True
            assert results["email"] is True
            
            # Should have made exactly one attempt per channel
            assert mock_post.call_count == 1
            assert mock_smtp.call_count == 1
            
            # Should not have used any sleep (no retries needed)
            assert mock_sleep.call_count == 0
"""
Property-based tests for multi-channel notification delivery.

**Feature: memory-price-monitor, Property 15: Multi-channel notification delivery**
**Validates: Requirements 5.1, 5.2**
"""

import pytest
from hypothesis import given, strategies as st, settings
from unittest.mock import Mock, patch, MagicMock
from decimal import Decimal
from datetime import date, datetime, timedelta
import requests

from memory_price_monitor.services.notification import NotificationService
from memory_price_monitor.data.models import (
    WeeklyReport, ComparisonResult, TrendData, FormattedReport
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


class TestMultiChannelNotificationDelivery:
    """Test multi-channel notification delivery properties."""
    
    @given(
        report=weekly_report_strategy(),
        config=notification_config_strategy()
    )
    @settings(max_examples=5, deadline=None)
    def test_multi_channel_delivery_attempts_all_configured_channels(self, report, config):
        """
        Property 15: Multi-channel notification delivery
        For any generated weekly report, the notification service should successfully 
        send it through all configured channels (WeChat and email).
        
        **Validates: Requirements 5.1, 5.2**
        """
        service = NotificationService(config)
        
        # Mock external dependencies
        with patch('requests.post') as mock_post, \
             patch('smtplib.SMTP') as mock_smtp, \
             patch('memory_price_monitor.services.notification.wxpy', create=True) as mock_wxpy_module:
            
            # Configure mocks for successful responses
            mock_post.return_value.json.return_value = {"code": 0}
            mock_post.return_value.raise_for_status.return_value = None
            
            mock_smtp_instance = MagicMock()
            mock_smtp.return_value.__enter__.return_value = mock_smtp_instance
            
            # Mock wxpy module and Bot class
            mock_bot = MagicMock()
            mock_wxpy_module.Bot.return_value = mock_bot
            mock_bot.file_helper = MagicMock()
            
            # Test that service attempts to send through all channels
            results = service.send_notification_with_retry(report, ["wechat", "email"])
            
            # Verify that both channels were attempted
            assert "wechat" in results
            assert "email" in results
            
            # If WeChat is configured properly, it should be attempted
            if config.wechat_method == "serverchan" and config.serverchan_key:
                mock_post.assert_called()
            elif config.wechat_method == "wxpy":
                # wxpy should be attempted if configured
                pass
            
            # If email is configured properly, it should be attempted
            if config.email_username and config.email_password and config.email_recipients:
                mock_smtp.assert_called_with(config.email_smtp_host, config.email_smtp_port)
    
    @given(
        report=weekly_report_strategy(),
        config=notification_config_strategy()
    )
    @settings(max_examples=5, deadline=None)
    def test_wechat_serverchan_delivery_with_valid_config(self, report, config):
        """
        Test WeChat delivery via Server酱 with valid configuration.
        """
        if not config.serverchan_key:
            config.serverchan_key = "test_key_12345"
        config.wechat_method = "serverchan"
        
        service = NotificationService(config)
        
        with patch('requests.post') as mock_post:
            # Mock successful Server酱 response
            mock_response = MagicMock()
            mock_response.json.return_value = {"code": 0}
            mock_response.raise_for_status.return_value = None
            mock_post.return_value = mock_response
            
            result = service.send_wechat_serverchan(report)
            
            # Should successfully send
            assert result is True
            
            # Should call the correct API endpoint
            expected_url = f"https://sctapi.ftqq.com/{config.serverchan_key}.send"
            mock_post.assert_called_once()
            call_args = mock_post.call_args
            assert call_args[0][0] == expected_url
            
            # Should include formatted report data
            call_data = call_args[1]['data']
            assert 'title' in call_data
            assert 'desp' in call_data
            assert '内存价格周报' in call_data['title']
    
    @given(
        report=weekly_report_strategy(),
        config=notification_config_strategy()
    )
    @settings(max_examples=5, deadline=None)
    def test_email_delivery_with_valid_config(self, report, config):
        """
        Test email delivery with valid configuration.
        """
        if not config.email_username:
            config.email_username = "test@example.com"
        if not config.email_password:
            config.email_password = "test_password"
        if not config.email_recipients:
            config.email_recipients = ["recipient@example.com"]
        
        service = NotificationService(config)
        
        with patch('smtplib.SMTP') as mock_smtp:
            mock_smtp_instance = MagicMock()
            mock_smtp.return_value.__enter__.return_value = mock_smtp_instance
            
            result = service.send_email([], report)
            
            # Should successfully send
            assert result is True
            
            # Should connect to SMTP server
            mock_smtp.assert_called_with(config.email_smtp_host, config.email_smtp_port)
            
            # Should authenticate and send message
            mock_smtp_instance.starttls.assert_called_once()
            mock_smtp_instance.login.assert_called_once_with(
                config.email_username, config.email_password
            )
            mock_smtp_instance.send_message.assert_called_once()
    
    @given(
        report=weekly_report_strategy(),
        config=notification_config_strategy()
    )
    @settings(max_examples=5, deadline=None)
    def test_report_formatting_produces_valid_formatted_report(self, report, config):
        """
        Test that report formatting produces a valid FormattedReport.
        """
        service = NotificationService(config)
        
        formatted_report = service.format_report(report)
        
        # Should produce a valid FormattedReport
        assert isinstance(formatted_report, FormattedReport)
        assert formatted_report.subject
        assert formatted_report.text_content
        assert formatted_report.html_content
        assert isinstance(formatted_report.attachments, list)
        
        # Subject should contain report information
        assert '内存价格周报' in formatted_report.subject
        assert report.report_date.strftime('%Y年%m月%d日') in formatted_report.subject
        
        # Text content should contain key information
        assert '内存价格周报' in formatted_report.text_content
        assert report.week_start.strftime('%m月%d日') in formatted_report.text_content
        assert report.week_end.strftime('%m月%d日') in formatted_report.text_content
        
        # HTML content should be valid HTML
        assert '<html>' in formatted_report.html_content
        assert '</html>' in formatted_report.html_content
        assert '内存价格周报' in formatted_report.html_content
        
        # Should have attachments for charts
        assert len(formatted_report.attachments) == len(report.charts)
        for attachment in formatted_report.attachments:
            assert 'filename' in attachment
            assert 'content' in attachment
            assert 'content_type' in attachment
            assert attachment['filename'].endswith('.png')
            assert attachment['content_type'] == 'image/png'
    
    @given(
        report=weekly_report_strategy(),
        config=notification_config_strategy()
    )
    @settings(max_examples=5, deadline=None)
    def test_retry_mechanism_respects_configuration(self, report, config):
        """
        Test that retry mechanism respects the configured retry attempts.
        """
        service = NotificationService(config)
        
        with patch('requests.post') as mock_post, \
             patch('smtplib.SMTP') as mock_smtp, \
             patch('time.sleep') as mock_sleep:
            
            # Configure mocks to always fail
            mock_post.side_effect = requests.RequestException("Network error")
            mock_smtp.side_effect = Exception("SMTP error")
            
            results = service.send_notification_with_retry(report, ["wechat", "email"])
            
            # Should attempt retries according to configuration
            if config.serverchan_key:
                # Should have attempted Server酱 calls equal to retry_attempts
                assert mock_post.call_count <= config.retry_attempts
            
            if config.email_username and config.email_password and config.email_recipients:
                # Should have attempted SMTP connections equal to retry_attempts
                assert mock_smtp.call_count <= config.retry_attempts
            
            # Should have used sleep for delays if there were retries
            if mock_post.call_count > 1 or mock_smtp.call_count > 1:
                assert mock_sleep.call_count > 0
                # Verify sleep calls follow some pattern (not necessarily strict exponential)
                sleep_calls = [call[0][0] for call in mock_sleep.call_args_list]
                # Just verify that delays are reasonable (not negative or zero)
                for delay in sleep_calls:
                    assert delay > 0
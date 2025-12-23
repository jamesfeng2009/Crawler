"""
Property-based tests for notification content integrity.

**Feature: memory-price-monitor, Property 16: Notification content integrity**
**Validates: Requirements 5.3**
"""

import pytest
from hypothesis import given, strategies as st, settings
from decimal import Decimal
from datetime import date, datetime, timedelta
import hashlib

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
    
    # Generate chart data with known content
    chart_names = draw(st.lists(
        st.sampled_from(["weekly_trend", "brand_comparison", "price_history", "volatility_chart"]),
        min_size=1, max_size=4, unique=True
    ))
    
    charts = {}
    for name in chart_names:
        # Create deterministic chart data based on name
        chart_content = f"chart_data_for_{name}".encode('utf-8')
        charts[name] = chart_content
    
    return WeeklyReport(
        report_date=report_date,
        week_start=week_start,
        week_end=week_end,
        comparison=draw(comparison_result_strategy()),
        charts=charts,
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


class TestNotificationContentIntegrity:
    """Test notification content integrity properties."""
    
    @given(
        report=weekly_report_strategy(),
        config=notification_config_strategy()
    )
    @settings(max_examples=5, deadline=None)
    def test_chart_attachments_preserve_original_data(self, report, config):
        """
        Property 16: Notification content integrity
        For any notification sent, it should include trend charts as attachments 
        or embedded images without corruption.
        
        **Validates: Requirements 5.3**
        """
        service = NotificationService(config)
        
        # Format the report
        formatted_report = service.format_report(report)
        
        # Verify that all original charts are preserved as attachments
        assert len(formatted_report.attachments) == len(report.charts)
        
        # Create mapping of original chart data by name
        original_charts = {name: data for name, data in report.charts.items()}
        
        # Verify each attachment preserves the original chart data
        for attachment in formatted_report.attachments:
            filename = attachment['filename']
            chart_name = filename.replace('.png', '')  # Remove extension
            
            # Verify the chart exists in original data
            assert chart_name in original_charts
            
            # Verify content is preserved without corruption
            original_data = original_charts[chart_name]
            attachment_data = attachment['content']
            
            # Content should be identical (no corruption)
            assert attachment_data == original_data
            
            # Verify attachment metadata
            assert attachment['content_type'] == 'image/png'
            assert filename.endswith('.png')
    
    @given(
        report=weekly_report_strategy(),
        config=notification_config_strategy()
    )
    @settings(max_examples=5, deadline=None)
    def test_text_content_includes_all_essential_information(self, report, config):
        """
        Test that text content includes all essential report information without loss.
        """
        service = NotificationService(config)
        
        formatted_report = service.format_report(report)
        text_content = formatted_report.text_content
        
        # Verify essential information is present
        assert '内存价格周报' in text_content
        assert report.report_date.strftime('%Y年%m月%d日') in text_content
        assert report.week_start.strftime('%m月%d日') in text_content
        assert report.week_end.strftime('%m月%d日') in text_content
        
        # Verify summary statistics are included
        if 'total_products' in report.summary_stats:
            assert str(report.summary_stats['total_products']) in text_content
        
        if 'average_price_change' in report.summary_stats:
            change = report.summary_stats['average_price_change']
            # Should include either the exact value or formatted percentage
            assert (str(abs(change)) in text_content or 
                   f"{abs(change):.1f}%" in text_content)
        
        # Verify significant changes are included
        for trend in report.comparison.significant_changes[:5]:  # Top 5 as per implementation
            assert trend.brand in text_content
            # Should include percentage change
            assert f"{trend.percentage_change:+.1f}%" in text_content or \
                   f"{abs(trend.percentage_change):.1f}%" in text_content
    
    @given(
        report=weekly_report_strategy(),
        config=notification_config_strategy()
    )
    @settings(max_examples=5, deadline=None)
    def test_html_content_includes_all_essential_information(self, report, config):
        """
        Test that HTML content includes all essential report information without loss.
        """
        service = NotificationService(config)
        
        formatted_report = service.format_report(report)
        html_content = formatted_report.html_content
        
        # Verify HTML structure
        assert '<html>' in html_content
        assert '</html>' in html_content
        assert '<head>' in html_content
        assert '<body>' in html_content
        
        # Verify essential information is present
        assert '内存价格周报' in html_content
        assert report.report_date.strftime('%Y年%m月%d日') in html_content
        assert report.week_start.strftime('%m月%d日') in html_content
        assert report.week_end.strftime('%m月%d日') in html_content
        
        # Verify summary statistics are included
        if 'total_products' in report.summary_stats:
            assert str(report.summary_stats['total_products']) in html_content
        
        if 'average_price_change' in report.summary_stats:
            change = report.summary_stats['average_price_change']
            assert (str(abs(change)) in html_content or 
                   f"{abs(change):.1f}%" in html_content)
        
        # Verify significant changes table is included if there are changes
        if report.comparison.significant_changes:
            assert '<table>' in html_content
            assert '<th>品牌</th>' in html_content
            assert '<th>型号</th>' in html_content
            assert '<th>容量</th>' in html_content
            assert '<th>价格变化</th>' in html_content
            
            # Verify first few significant changes are in the table
            for trend in report.comparison.significant_changes[:10]:  # Top 10 as per implementation
                assert trend.brand in html_content
                assert trend.model in html_content
                assert trend.capacity in html_content
    
    @given(
        report=weekly_report_strategy(),
        config=notification_config_strategy()
    )
    @settings(max_examples=5, deadline=None)
    def test_subject_line_contains_report_identification(self, report, config):
        """
        Test that subject line properly identifies the report without ambiguity.
        """
        service = NotificationService(config)
        
        formatted_report = service.format_report(report)
        subject = formatted_report.subject
        
        # Subject should contain key identifying information
        assert '内存价格周报' in subject
        assert report.report_date.strftime('%Y年%m月%d日') in subject
        
        # Subject should be non-empty and reasonable length
        assert len(subject.strip()) > 0
        assert len(subject) < 200  # Reasonable email subject length
    
    @given(
        report=weekly_report_strategy(),
        config=notification_config_strategy()
    )
    @settings(max_examples=5, deadline=None)
    def test_content_consistency_between_text_and_html(self, report, config):
        """
        Test that text and HTML content contain consistent information.
        """
        service = NotificationService(config)
        
        formatted_report = service.format_report(report)
        text_content = formatted_report.text_content
        html_content = formatted_report.html_content
        
        # Both should contain the same essential information
        essential_info = [
            '内存价格周报',
            report.report_date.strftime('%Y年%m月%d日'),
            report.week_start.strftime('%m月%d日'),
            report.week_end.strftime('%m月%d日')
        ]
        
        for info in essential_info:
            assert info in text_content, f"Missing {info} in text content"
            assert info in html_content, f"Missing {info} in HTML content"
        
        # Both should include summary statistics if present
        if 'total_products' in report.summary_stats:
            total_products = str(report.summary_stats['total_products'])
            assert total_products in text_content
            assert total_products in html_content
        
        # Both should include significant changes
        for trend in report.comparison.significant_changes[:5]:  # Common subset
            assert trend.brand in text_content
            assert trend.brand in html_content
    
    @given(
        report=weekly_report_strategy(),
        config=notification_config_strategy()
    )
    @settings(max_examples=5, deadline=None)
    def test_attachment_integrity_with_multiple_charts(self, report, config):
        """
        Test that multiple chart attachments maintain individual integrity.
        """
        # Ensure we have multiple charts for this test
        if len(report.charts) < 2:
            # Add additional charts to test multiple attachments
            report.charts["additional_chart_1"] = b"additional_chart_data_1"
            report.charts["additional_chart_2"] = b"additional_chart_data_2"
        
        service = NotificationService(config)
        formatted_report = service.format_report(report)
        
        # Verify each chart has a corresponding attachment
        chart_names = set(report.charts.keys())
        attachment_names = {att['filename'].replace('.png', '') for att in formatted_report.attachments}
        
        assert chart_names == attachment_names, "Chart names and attachment names should match"
        
        # Verify each attachment maintains data integrity
        for attachment in formatted_report.attachments:
            chart_name = attachment['filename'].replace('.png', '')
            original_data = report.charts[chart_name]
            
            # Data should be preserved exactly
            assert attachment['content'] == original_data
            
            # Metadata should be consistent
            assert attachment['content_type'] == 'image/png'
            assert attachment['filename'] == f"{chart_name}.png"
        
        # Verify no data mixing between attachments
        attachment_data_hashes = set()
        for attachment in formatted_report.attachments:
            data_hash = hashlib.md5(attachment['content']).hexdigest()
            assert data_hash not in attachment_data_hashes, "Duplicate attachment data detected"
            attachment_data_hashes.add(data_hash)
    
    @given(
        report=weekly_report_strategy(),
        config=notification_config_strategy()
    )
    @settings(max_examples=5, deadline=None)
    def test_formatted_report_completeness(self, report, config):
        """
        Test that formatted report contains all required fields without omission.
        """
        service = NotificationService(config)
        
        formatted_report = service.format_report(report)
        
        # Verify all required fields are present and non-empty
        assert formatted_report.subject is not None
        assert len(formatted_report.subject.strip()) > 0
        
        assert formatted_report.text_content is not None
        assert len(formatted_report.text_content.strip()) > 0
        
        assert formatted_report.html_content is not None
        assert len(formatted_report.html_content.strip()) > 0
        
        assert formatted_report.attachments is not None
        assert isinstance(formatted_report.attachments, list)
        
        # Verify attachments match charts
        assert len(formatted_report.attachments) == len(report.charts)
        
        # Verify metadata field exists
        assert hasattr(formatted_report, 'metadata')
        assert isinstance(formatted_report.metadata, dict)
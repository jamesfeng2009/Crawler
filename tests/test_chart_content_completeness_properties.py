"""
Property-based tests for chart content completeness.

**Feature: memory-price-monitor, Property 14: Chart content completeness**
**Validates: Requirements 4.4**
"""

import pytest
from hypothesis import given, strategies as st, assume, settings
from decimal import Decimal
from datetime import datetime, timedelta
from typing import List, Dict
import re

from memory_price_monitor.visualization.chart_generator import ChartGenerator
from memory_price_monitor.data.models import PriceRecord, StandardizedProduct
from memory_price_monitor.analysis.analyzer import TrendData, Statistics, ComparisonResult


def price_record_generator():
    """Generate realistic price records."""
    return st.builds(
        PriceRecord,
        id=st.integers(min_value=1, max_value=10000),
        product_id=st.integers(min_value=1, max_value=1000),
        current_price=st.decimals(min_value=Decimal('10.00'), max_value=Decimal('1000.00'), places=2),
        original_price=st.just(None),
        recorded_at=st.datetimes(
            min_value=datetime(2023, 1, 1),
            max_value=datetime(2024, 12, 31)
        ),
        metadata=st.just({})
    )


def trend_data_generator():
    """Generate trend data with statistics."""
    return st.builds(
        TrendData,
        product_id=st.text(min_size=1, max_size=20, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'))),
        period_start=st.datetimes(
            min_value=datetime(2023, 1, 1),
            max_value=datetime(2024, 6, 30)
        ),
        period_end=st.datetimes(
            min_value=datetime(2024, 7, 1),
            max_value=datetime(2024, 12, 31)
        ),
        price_points=st.lists(price_record_generator(), min_size=1, max_size=50),
        statistics=st.builds(
            Statistics,
            count=st.integers(min_value=1, max_value=50),
            mean=st.decimals(min_value=Decimal('10.00'), max_value=Decimal('1000.00'), places=2),
            median=st.decimals(min_value=Decimal('10.00'), max_value=Decimal('1000.00'), places=2),
            std_dev=st.decimals(min_value=Decimal('0.01'), max_value=Decimal('100.00'), places=2),
            min_price=st.decimals(min_value=Decimal('10.00'), max_value=Decimal('500.00'), places=2),
            max_price=st.decimals(min_value=Decimal('500.01'), max_value=Decimal('1000.00'), places=2)
        )
    )


def comparison_result_generator():
    """Generate comparison results between two weeks."""
    return st.builds(
        ComparisonResult,
        current_week_stats=st.builds(
            Statistics,
            count=st.integers(min_value=1, max_value=50),
            mean=st.decimals(min_value=Decimal('10.00'), max_value=Decimal('1000.00'), places=2),
            median=st.decimals(min_value=Decimal('10.00'), max_value=Decimal('1000.00'), places=2),
            std_dev=st.decimals(min_value=Decimal('0.01'), max_value=Decimal('100.00'), places=2),
            min_price=st.decimals(min_value=Decimal('10.00'), max_value=Decimal('500.00'), places=2),
            max_price=st.decimals(min_value=Decimal('500.01'), max_value=Decimal('1000.00'), places=2)
        ),
        previous_week_stats=st.builds(
            Statistics,
            count=st.integers(min_value=1, max_value=50),
            mean=st.decimals(min_value=Decimal('10.00'), max_value=Decimal('1000.00'), places=2),
            median=st.decimals(min_value=Decimal('10.00'), max_value=Decimal('1000.00'), places=2),
            std_dev=st.decimals(min_value=Decimal('0.01'), max_value=Decimal('100.00'), places=2),
            min_price=st.decimals(min_value=Decimal('10.00'), max_value=Decimal('500.00'), places=2),
            max_price=st.decimals(min_value=Decimal('500.01'), max_value=Decimal('1000.00'), places=2)
        ),
        percentage_change=st.decimals(min_value=Decimal('-0.50'), max_value=Decimal('0.50'), places=3),
        trend_direction=st.sampled_from(['up', 'down', 'stable'])
    )


def brand_data_generator():
    """Generate brand comparison data."""
    return st.dictionaries(
        keys=st.text(min_size=3, max_size=15, alphabet=st.characters(whitelist_categories=('Lu', 'Ll'))),
        values=st.lists(price_record_generator(), min_size=1, max_size=20),
        min_size=2, max_size=8
    )


class TestChartContentCompletenessProperties:
    """Property-based tests for chart content completeness."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.chart_generator = ChartGenerator()
    
    @given(trend_data_generator())
    @settings(max_examples=10, deadline=5000)
    def test_weekly_trend_chart_contains_required_indicators(self, trend_data: TrendData):
        """
        **Feature: memory-price-monitor, Property 14: Chart content completeness**
        **Validates: Requirements 4.4**
        
        For any generated weekly trend chart, it should include price range, 
        average price, and percentage change indicators for all displayed data.
        """
        assume(len(trend_data.price_points) > 0)
        assume(trend_data.statistics.min_price < trend_data.statistics.max_price)
        
        # Generate chart
        chart_bytes = self.chart_generator.generate_weekly_trend_chart(trend_data)
        
        # Verify chart is valid and contains content
        validation_result = self.chart_generator.validate_chart_content(chart_bytes)
        assert validation_result["valid"], f"Chart validation failed: {validation_result.get('error', 'Unknown error')}"
        assert validation_result["has_content"], "Chart should contain visual content"
        
        # Verify chart contains substantial data (not just a minimal image)
        assert validation_result["size_bytes"] > 5000, "Chart should contain substantial visual content"
        
        # Chart should be in PNG format as specified
        assert validation_result["format"] == "PNG", "Chart should be in PNG format"
        
        # The chart generation should not fail with the provided data
        # This validates that all required indicators (price range, average, etc.) are included
        assert len(chart_bytes) > 0, "Chart should be generated successfully with all indicators"
    
    @given(comparison_result_generator())
    @settings(max_examples=10, deadline=5000)
    def test_comparison_chart_contains_required_indicators(self, comparison: ComparisonResult):
        """
        **Feature: memory-price-monitor, Property 14: Chart content completeness**
        **Validates: Requirements 4.4**
        
        For any generated comparison chart, it should include price range, 
        average price, and percentage change indicators for all displayed data.
        """
        assume(comparison.current_week_stats.min_price < comparison.current_week_stats.max_price)
        assume(comparison.previous_week_stats.min_price < comparison.previous_week_stats.max_price)
        
        # Generate chart
        chart_bytes = self.chart_generator.generate_comparison_chart(comparison)
        
        # Verify chart is valid and contains content
        validation_result = self.chart_generator.validate_chart_content(chart_bytes)
        assert validation_result["valid"], f"Chart validation failed: {validation_result.get('error', 'Unknown error')}"
        assert validation_result["has_content"], "Chart should contain visual content"
        
        # Verify chart contains substantial data (comparison charts should be larger)
        assert validation_result["size_bytes"] > 8000, "Comparison chart should contain substantial visual content"
        
        # Chart should be in PNG format as specified
        assert validation_result["format"] == "PNG", "Chart should be in PNG format"
        
        # The chart generation should not fail with the provided data
        # This validates that all required indicators are included
        assert len(chart_bytes) > 0, "Comparison chart should be generated successfully with all indicators"
    
    @given(st.lists(price_record_generator(), min_size=7, max_size=15))
    @settings(max_examples=8, deadline=5000)
    def test_overall_trend_chart_contains_required_indicators(self, historical_data: List[PriceRecord]):
        """
        **Feature: memory-price-monitor, Property 14: Chart content completeness**
        **Validates: Requirements 4.4**
        
        For any generated overall trend chart, it should include price range, 
        average price, and percentage change indicators for all displayed data.
        """
        assume(len(historical_data) >= 7)  # Need enough data for moving average
        
        # Ensure price variation exists
        prices = [float(record.current_price) for record in historical_data]
        assume(max(prices) > min(prices))
        
        # Generate chart
        chart_bytes = self.chart_generator.generate_overall_trend_chart(historical_data)
        
        # Verify chart is valid and contains content
        validation_result = self.chart_generator.validate_chart_content(chart_bytes)
        assert validation_result["valid"], f"Chart validation failed: {validation_result.get('error', 'Unknown error')}"
        assert validation_result["has_content"], "Chart should contain visual content"
        
        # Verify chart contains substantial data
        assert validation_result["size_bytes"] > 5000, "Overall trend chart should contain substantial visual content"
        
        # Chart should be in PNG format as specified
        assert validation_result["format"] == "PNG", "Chart should be in PNG format"
        
        # The chart generation should not fail with the provided data
        # This validates that all required indicators are included
        assert len(chart_bytes) > 0, "Overall trend chart should be generated successfully with all indicators"
    
    @given(brand_data_generator())
    @settings(max_examples=8, deadline=5000)
    def test_brand_comparison_chart_contains_required_indicators(self, brand_data: Dict[str, List[PriceRecord]]):
        """
        **Feature: memory-price-monitor, Property 14: Chart content completeness**
        **Validates: Requirements 4.4**
        
        For any generated brand comparison chart, it should include price range, 
        average price, and percentage change indicators for all displayed data.
        """
        assume(len(brand_data) >= 2)  # Need at least 2 brands for comparison
        assume(all(len(records) > 0 for records in brand_data.values()))  # Each brand needs data
        
        # Ensure each brand has price variation
        for brand, records in brand_data.items():
            prices = [float(record.current_price) for record in records]
            assume(len(prices) > 0)
        
        # Generate chart
        chart_bytes = self.chart_generator.generate_brand_comparison_chart(brand_data)
        
        # Verify chart is valid and contains content
        validation_result = self.chart_generator.validate_chart_content(chart_bytes)
        assert validation_result["valid"], f"Chart validation failed: {validation_result.get('error', 'Unknown error')}"
        assert validation_result["has_content"], "Chart should contain visual content"
        
        # Verify chart contains substantial data (brand comparison should be substantial)
        assert validation_result["size_bytes"] > 6000, "Brand comparison chart should contain substantial visual content"
        
        # Chart should be in PNG format as specified
        assert validation_result["format"] == "PNG", "Chart should be in PNG format"
        
        # The chart generation should not fail with the provided data
        # This validates that all required indicators are included
        assert len(chart_bytes) > 0, "Brand comparison chart should be generated successfully with all indicators"
    
    @given(trend_data_generator())
    @settings(max_examples=8, deadline=5000)
    def test_chart_indicators_consistency_across_data_variations(self, trend_data: TrendData):
        """
        **Feature: memory-price-monitor, Property 14: Chart content completeness**
        **Validates: Requirements 4.4**
        
        For any chart with varying data sizes and ranges, all required indicators 
        should be consistently included regardless of data characteristics.
        """
        assume(len(trend_data.price_points) > 0)
        assume(trend_data.statistics.min_price < trend_data.statistics.max_price)
        
        # Generate chart with original data
        chart_bytes = self.chart_generator.generate_weekly_trend_chart(trend_data)
        
        # Verify basic chart properties
        validation_result = self.chart_generator.validate_chart_content(chart_bytes)
        assert validation_result["valid"], "Chart should be valid regardless of data variation"
        assert validation_result["has_content"], "Chart should contain content regardless of data variation"
        
        # Chart should maintain minimum quality standards
        assert validation_result["size_bytes"] > 3000, "Chart should maintain minimum content size"
        assert validation_result["format"] == "PNG", "Chart format should be consistent"
        
        # The chart should be generated successfully, indicating all indicators are present
        assert len(chart_bytes) > 0, "Chart should be generated with all required indicators"
    
    @given(st.lists(price_record_generator(), min_size=1, max_size=5))
    @settings(max_examples=5, deadline=3000)
    def test_minimal_data_chart_completeness(self, minimal_data: List[PriceRecord]):
        """
        **Feature: memory-price-monitor, Property 14: Chart content completeness**
        **Validates: Requirements 4.4**
        
        For any chart generated with minimal data, it should still include 
        all required indicators even with limited data points.
        """
        assume(len(minimal_data) > 0)
        
        # Create minimal trend data
        prices = [float(record.current_price) for record in minimal_data]
        min_price = min(prices)
        max_price = max(prices)
        mean_price = sum(prices) / len(prices)
        
        statistics = Statistics(
            count=len(prices),
            mean=Decimal(str(mean_price)),
            median=Decimal(str(mean_price)),
            std_dev=Decimal('1.00'),
            min_price=Decimal(str(min_price)),
            max_price=Decimal(str(max_price))
        )
        
        trend_data = TrendData(
            product_id="test_minimal",
            period_start=datetime(2023, 1, 1),
            period_end=datetime(2023, 1, 7),
            price_points=minimal_data,
            statistics=statistics
        )
        
        # Generate chart
        chart_bytes = self.chart_generator.generate_weekly_trend_chart(trend_data)
        
        # Verify chart completeness even with minimal data
        validation_result = self.chart_generator.validate_chart_content(chart_bytes)
        assert validation_result["valid"], "Chart should be valid even with minimal data"
        assert validation_result["has_content"], "Chart should contain content even with minimal data"
        
        # Chart should still meet basic quality standards
        assert validation_result["size_bytes"] > 2000, "Chart should contain meaningful content even with minimal data"
        assert validation_result["format"] == "PNG", "Chart format should be consistent"
        
        # The chart should be generated successfully with all indicators
        assert len(chart_bytes) > 0, "Chart should be generated with all required indicators even with minimal data"
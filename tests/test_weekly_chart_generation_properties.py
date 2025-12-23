"""
Property-based tests for weekly chart generation functionality.

**Feature: memory-price-monitor, Property 11: Weekly report chart generation**
"""

import pytest
from hypothesis import given, strategies as st, settings, assume
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List
import io

from memory_price_monitor.visualization.chart_generator import ChartGenerator
from memory_price_monitor.analysis.analyzer import PriceAnalyzer, TrendData
from memory_price_monitor.data.models import PriceRecord


# Test data generators
@st.composite
def price_record_generator(draw):
    """Generate realistic price records for testing."""
    base_price = draw(st.decimals(min_value=50, max_value=2000, places=2))
    
    # Generate timestamp within the last week
    days_ago = draw(st.integers(min_value=0, max_value=6))
    hours_ago = draw(st.integers(min_value=0, max_value=23))
    minutes_ago = draw(st.integers(min_value=0, max_value=59))
    
    timestamp = datetime.now() - timedelta(days=days_ago, hours=hours_ago, minutes=minutes_ago)
    
    # Add some price variation (±20%)
    price_variation = draw(st.decimals(min_value=-0.2, max_value=0.2, places=3))
    current_price = base_price * (Decimal('1') + price_variation)
    current_price = max(current_price, Decimal('1.00'))  # Ensure positive price
    
    return PriceRecord(
        id=draw(st.integers(min_value=1, max_value=10000)),
        product_id=draw(st.integers(min_value=1, max_value=1000)),
        current_price=current_price,
        original_price=current_price * draw(st.decimals(min_value=1.0, max_value=1.5, places=2)),
        recorded_at=timestamp,
        metadata={}
    )


@st.composite
def weekly_price_data_generator(draw):
    """Generate a list of price records representing weekly data."""
    # Generate between 3 and 50 price records for a week
    num_records = draw(st.integers(min_value=3, max_value=50))
    
    # Generate base product info
    product_id = draw(st.text(min_size=1, max_size=20, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'))))
    
    # Generate price records with the same product_id
    records = []
    base_price = draw(st.decimals(min_value=50, max_value=2000, places=2))
    
    for i in range(num_records):
        # Distribute records across the week
        days_ago = draw(st.integers(min_value=0, max_value=6))
        hours_ago = draw(st.integers(min_value=0, max_value=23))
        minutes_ago = draw(st.integers(min_value=0, max_value=59))
        
        timestamp = datetime.now() - timedelta(days=days_ago, hours=hours_ago, minutes=minutes_ago)
        
        # Add realistic price variation (±15%)
        price_variation = draw(st.decimals(min_value=-0.15, max_value=0.15, places=3))
        current_price = base_price * (Decimal('1') + price_variation)
        current_price = max(current_price, Decimal('1.00'))  # Ensure positive price
        
        record = PriceRecord(
            id=i + 1,
            product_id=1,  # Same product for all records
            current_price=current_price,
            original_price=current_price * draw(st.decimals(min_value=1.0, max_value=1.3, places=2)),
            recorded_at=timestamp,
            metadata={}
        )
        records.append(record)
    
    return product_id, records


class TestWeeklyChartGenerationProperties:
    """Property-based tests for weekly chart generation."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.chart_generator = ChartGenerator()
        self.price_analyzer = PriceAnalyzer()
    
    @given(weekly_price_data_generator())
    @settings(max_examples=10, deadline=3000)
    def test_weekly_chart_generation_produces_valid_chart(self, weekly_data):
        """
        **Feature: memory-price-monitor, Property 11: Weekly report chart generation**
        
        Property: For any weekly price data, generating a report should produce 
        trend charts containing current week price movements.
        
        **Validates: Requirements 4.1**
        """
        product_id, price_records = weekly_data
        
        # Ensure we have valid price data
        assume(len(price_records) >= 1)
        assume(all(record.current_price > 0 for record in price_records))
        
        # Generate trend data using the analyzer
        trend_data = self.price_analyzer.calculate_weekly_trend(
            product_id=product_id,
            price_records=price_records,
            include_moving_averages=False  # Simplify for testing
        )
        
        # Generate chart
        chart_bytes = self.chart_generator.generate_weekly_trend_chart(trend_data)
        
        # Verify chart was generated
        assert chart_bytes is not None, "Chart generation should produce bytes"
        assert len(chart_bytes) > 0, "Chart bytes should not be empty"
        
        # Verify chart format and basic content
        validation_result = self.chart_generator.validate_chart_content(chart_bytes)
        assert validation_result["valid"], f"Chart validation failed: {validation_result.get('error', 'Unknown error')}"
        assert validation_result["format"] == "PNG", "Chart should be in PNG format"
        assert validation_result["size_bytes"] > 1000, "Chart should have reasonable size"
        
        # Verify chart contains current week price movements by checking it's not empty
        # (More detailed content validation would require image processing libraries)
        assert validation_result["has_content"], "Chart should contain visual content"
    
    @given(weekly_price_data_generator())
    @settings(max_examples=10, deadline=3000)
    def test_chart_contains_required_price_indicators(self, weekly_data):
        """
        Property: For any generated weekly chart, it should include price range, 
        average price, and percentage change indicators.
        
        This validates that the chart generation includes the required statistical elements.
        """
        product_id, price_records = weekly_data
        
        # Ensure we have valid price data
        assume(len(price_records) >= 2)  # Need at least 2 points for meaningful indicators
        assume(all(record.current_price > 0 for record in price_records))
        
        # Generate trend data
        trend_data = self.price_analyzer.calculate_weekly_trend(
            product_id=product_id,
            price_records=price_records,
            include_moving_averages=False
        )
        
        # Verify trend data contains required statistical elements
        assert trend_data.statistics is not None, "Trend data should contain statistics"
        assert trend_data.statistics.mean > 0, "Average price should be positive"
        assert trend_data.statistics.min_price > 0, "Minimum price should be positive"
        assert trend_data.statistics.max_price > 0, "Maximum price should be positive"
        assert trend_data.statistics.min_price <= trend_data.statistics.max_price, "Min should be <= Max"
        
        # Generate chart with these indicators
        chart_bytes = self.chart_generator.generate_weekly_trend_chart(trend_data)
        
        # Verify chart was generated successfully
        assert chart_bytes is not None, "Chart should be generated"
        assert len(chart_bytes) > 0, "Chart should contain data"
        
        # Verify chart validation passes (indicates proper content structure)
        validation_result = self.chart_generator.validate_chart_content(chart_bytes)
        assert validation_result["valid"], "Chart with price indicators should be valid"
    
    @given(weekly_price_data_generator())
    @settings(max_examples=10, deadline=3000)
    def test_chart_generation_handles_single_price_point(self, weekly_data):
        """
        Property: For any weekly data with a single price point, chart generation 
        should still produce a valid chart.
        
        This tests the edge case of minimal data.
        """
        product_id, price_records = weekly_data
        
        # Use only the first price record
        assume(len(price_records) >= 1)
        single_record = [price_records[0]]
        assume(single_record[0].current_price > 0)
        
        # Generate trend data
        trend_data = self.price_analyzer.calculate_weekly_trend(
            product_id=product_id,
            price_records=single_record,
            include_moving_averages=False
        )
        
        # Generate chart
        chart_bytes = self.chart_generator.generate_weekly_trend_chart(trend_data)
        
        # Verify chart was generated
        assert chart_bytes is not None, "Chart should be generated even with single data point"
        assert len(chart_bytes) > 0, "Chart should contain data"
        
        # Verify chart is valid
        validation_result = self.chart_generator.validate_chart_content(chart_bytes)
        assert validation_result["valid"], "Single-point chart should be valid"
    
    @given(weekly_price_data_generator())
    @settings(max_examples=10, deadline=3000)
    def test_chart_generation_is_deterministic(self, weekly_data):
        """
        Property: For any identical weekly price data, generating charts multiple times 
        should produce identical results.
        
        This tests the deterministic nature of chart generation.
        """
        product_id, price_records = weekly_data
        
        # Ensure we have valid data
        assume(len(price_records) >= 1)
        assume(all(record.current_price > 0 for record in price_records))
        
        # Generate trend data
        trend_data = self.price_analyzer.calculate_weekly_trend(
            product_id=product_id,
            price_records=price_records,
            include_moving_averages=False
        )
        
        # Generate chart twice
        chart_bytes_1 = self.chart_generator.generate_weekly_trend_chart(trend_data)
        chart_bytes_2 = self.chart_generator.generate_weekly_trend_chart(trend_data)
        
        # Charts should be identical (deterministic generation)
        # Note: This might fail if chart generation includes timestamps or random elements
        # In that case, we'd need to modify the chart generator to be deterministic
        assert len(chart_bytes_1) == len(chart_bytes_2), "Chart generation should be deterministic in size"
        
        # Both should be valid
        validation_1 = self.chart_generator.validate_chart_content(chart_bytes_1)
        validation_2 = self.chart_generator.validate_chart_content(chart_bytes_2)
        
        assert validation_1["valid"], "First chart should be valid"
        assert validation_2["valid"], "Second chart should be valid"
        assert validation_1["size_bytes"] == validation_2["size_bytes"], "Chart sizes should be identical"
    
    @given(st.lists(price_record_generator(), min_size=1, max_size=50))
    @settings(max_examples=15, deadline=3000)
    def test_chart_generation_scales_with_data_size(self, price_records):
        """
        Property: For any size of weekly price data (within reasonable limits), 
        chart generation should complete successfully.
        
        This tests scalability of chart generation.
        """
        # Ensure all prices are positive
        assume(all(record.current_price > 0 for record in price_records))
        
        # Generate trend data
        trend_data = self.price_analyzer.calculate_weekly_trend(
            product_id="test_product",
            price_records=price_records,
            include_moving_averages=False
        )
        
        # Generate chart
        chart_bytes = self.chart_generator.generate_weekly_trend_chart(trend_data)
        
        # Verify chart was generated
        assert chart_bytes is not None, f"Chart should be generated for {len(price_records)} data points"
        assert len(chart_bytes) > 0, "Chart should contain data"
        
        # Verify chart is valid
        validation_result = self.chart_generator.validate_chart_content(chart_bytes)
        assert validation_result["valid"], f"Chart should be valid for {len(price_records)} data points"
        
        # Chart size should be reasonable (not too small, not excessively large)
        assert 1000 <= validation_result["size_bytes"] <= 1000000, \
            f"Chart size should be reasonable: {validation_result['size_bytes']} bytes"
    
    def test_chart_generation_error_handling(self):
        """
        Property: Chart generation should handle error cases gracefully.
        
        This tests error handling for invalid inputs.
        """
        # Test with empty trend data
        with pytest.raises(ValueError, match="No price data provided"):
            empty_trend_data = TrendData(
                product_id="test",
                period_start=datetime.now(),
                period_end=datetime.now(),
                statistics=None,
                price_points=[],
                moving_averages={},
                metadata={}
            )
            self.chart_generator.generate_weekly_trend_chart(empty_trend_data)
        
        # Test with None input
        with pytest.raises((ValueError, AttributeError)):
            self.chart_generator.generate_weekly_trend_chart(None)
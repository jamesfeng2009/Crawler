"""
Property-based tests for price pattern detection accuracy.

**Feature: memory-price-monitor, Property 26: Price pattern detection accuracy**
**Validates: Requirements 7.4**
"""

import pytest
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import List
from hypothesis import given, strategies as st, assume
from hypothesis import settings
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from memory_price_monitor.data.models import PriceRecord
from memory_price_monitor.analysis.analyzer import PriceAnalyzer, ComparisonResult, Statistics


# Hypothesis strategies for generating test data
@st.composite
def price_record_strategy(draw, base_date=None, price_range=(1.0, 10000.0)):
    """Generate a price record with realistic data."""
    if base_date is None:
        base_date = datetime(2024, 1, 1)
    
    # Generate price within range
    price = draw(st.decimals(
        min_value=Decimal(str(price_range[0])), 
        max_value=Decimal(str(price_range[1])), 
        places=2
    ))
    
    # Generate timestamp within a reasonable range
    timestamp_offset = draw(st.integers(min_value=0, max_value=86400 * 7))  # Within a week
    timestamp = base_date + timedelta(seconds=timestamp_offset)
    
    return PriceRecord(
        id=draw(st.integers(min_value=1, max_value=100000)),
        product_id=draw(st.integers(min_value=1, max_value=1000)),
        current_price=price,
        original_price=price * draw(st.decimals(min_value=Decimal('1.0'), max_value=Decimal('2.0'), places=2)),
        recorded_at=timestamp,
        metadata={}
    )


@st.composite
def price_records_with_change_strategy(draw, base_price=None, change_percentage=None):
    """Generate two weeks of price records with a specific percentage change."""
    if base_price is None:
        base_price = draw(st.decimals(min_value=Decimal('10.00'), max_value=Decimal('1000.00'), places=2))
    
    if change_percentage is None:
        # Generate a significant change (above 10% threshold)
        change_percentage = draw(st.decimals(
            min_value=Decimal('-0.50'), max_value=Decimal('0.50'), places=3
        ))
    
    # Generate previous week records with base price
    previous_week = []
    base_date = datetime(2024, 1, 1)
    num_prev_records = draw(st.integers(min_value=3, max_value=10))
    
    for i in range(num_prev_records):
        # Add small random variation around base price (±5%)
        price_variation = draw(st.decimals(min_value=Decimal('-0.05'), max_value=Decimal('0.05'), places=3))
        price = base_price * (Decimal('1') + price_variation)
        price = price.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        
        record = PriceRecord(
            id=i + 1,
            product_id=1,
            current_price=price,
            original_price=price,
            recorded_at=base_date + timedelta(days=i),
            metadata={}
        )
        previous_week.append(record)
    
    # Generate current week records with changed price
    current_week = []
    new_base_price = base_price * (Decimal('1') + change_percentage)
    new_base_price = new_base_price.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    
    num_curr_records = draw(st.integers(min_value=3, max_value=10))
    
    for i in range(num_curr_records):
        # Add small random variation around new base price (±5%)
        price_variation = draw(st.decimals(min_value=Decimal('-0.05'), max_value=Decimal('0.05'), places=3))
        price = new_base_price * (Decimal('1') + price_variation)
        price = price.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        
        record = PriceRecord(
            id=num_prev_records + i + 1,
            product_id=1,
            current_price=price,
            original_price=price,
            recorded_at=base_date + timedelta(days=7 + i),
            metadata={}
        )
        current_week.append(record)
    
    return previous_week, current_week, change_percentage


@st.composite
def comparison_result_strategy(draw):
    """Generate a comparison result for testing."""
    # Generate two sets of statistics
    current_mean = draw(st.decimals(min_value=Decimal('10.00'), max_value=Decimal('1000.00'), places=2))
    previous_mean = draw(st.decimals(min_value=Decimal('10.00'), max_value=Decimal('1000.00'), places=2))
    
    # Calculate percentage change
    percentage_change = ((current_mean - previous_mean) / previous_mean).quantize(
        Decimal('0.0001'), rounding=ROUND_HALF_UP
    )
    
    # Generate realistic statistics
    current_stats = Statistics(
        mean=current_mean,
        median=current_mean,
        min_price=current_mean * Decimal('0.9'),
        max_price=current_mean * Decimal('1.1'),
        std_dev=current_mean * draw(st.decimals(min_value=Decimal('0.01'), max_value=Decimal('0.30'), places=3)),
        count=draw(st.integers(min_value=5, max_value=20))
    )
    
    previous_stats = Statistics(
        mean=previous_mean,
        median=previous_mean,
        min_price=previous_mean * Decimal('0.9'),
        max_price=previous_mean * Decimal('1.1'),
        std_dev=previous_mean * draw(st.decimals(min_value=Decimal('0.01'), max_value=Decimal('0.30'), places=3)),
        count=draw(st.integers(min_value=5, max_value=20))
    )
    
    return ComparisonResult(
        current_week_stats=current_stats,
        previous_week_stats=previous_stats,
        average_price_difference=current_mean - previous_mean,
        percentage_change=percentage_change,
        trend_direction=""  # Will be calculated in __post_init__
    )


class TestPricePatternDetectionAccuracy:
    """Test price pattern detection accuracy property."""
    
    @given(
        base_price=st.decimals(min_value=Decimal('50.00'), max_value=Decimal('500.00'), places=2),
        significant_change=st.decimals(min_value=Decimal('0.15'), max_value=Decimal('0.50'), places=3)
    )
    @settings(max_examples=100)
    def test_significant_price_increase_detection(self, base_price, significant_change):
        """
        **Feature: memory-price-monitor, Property 26: Price pattern detection accuracy**
        
        For any price data analysis, significant price changes should be identified 
        based on configured thresholds with no false negatives.
        
        Test that significant price increases are always detected.
        
        **Validates: Requirements 7.4**
        """
        analyzer = PriceAnalyzer()
        
        # Create comparison with significant increase
        current_mean = base_price * (Decimal('1') + significant_change)
        previous_mean = base_price
        
        current_stats = Statistics(
            mean=current_mean,
            median=current_mean,
            min_price=current_mean * Decimal('0.95'),
            max_price=current_mean * Decimal('1.05'),
            std_dev=current_mean * Decimal('0.05'),
            count=10
        )
        
        previous_stats = Statistics(
            mean=previous_mean,
            median=previous_mean,
            min_price=previous_mean * Decimal('0.95'),
            max_price=previous_mean * Decimal('1.05'),
            std_dev=previous_mean * Decimal('0.05'),
            count=10
        )
        
        comparison = ComparisonResult(
            current_week_stats=current_stats,
            previous_week_stats=previous_stats,
            average_price_difference=current_mean - previous_mean,
            percentage_change=significant_change,
            trend_direction=""
        )
        
        # Test with default threshold (10%)
        alerts = analyzer.detect_significant_changes(comparison)
        
        # Should detect the significant change
        assert len(alerts) > 0
        assert any("increased" in alert for alert in alerts)
        assert any(f"{significant_change * 100:.1f}%" in alert for alert in alerts)
    
    @given(
        base_price=st.decimals(min_value=Decimal('50.00'), max_value=Decimal('500.00'), places=2),
        significant_change=st.decimals(min_value=Decimal('0.15'), max_value=Decimal('0.50'), places=3)
    )
    @settings(max_examples=100)
    def test_significant_price_decrease_detection(self, base_price, significant_change):
        """
        Test that significant price decreases are always detected.
        """
        analyzer = PriceAnalyzer()
        
        # Create comparison with significant decrease
        current_mean = base_price * (Decimal('1') - significant_change)
        previous_mean = base_price
        
        current_stats = Statistics(
            mean=current_mean,
            median=current_mean,
            min_price=current_mean * Decimal('0.95'),
            max_price=current_mean * Decimal('1.05'),
            std_dev=current_mean * Decimal('0.05'),
            count=10
        )
        
        previous_stats = Statistics(
            mean=previous_mean,
            median=previous_mean,
            min_price=previous_mean * Decimal('0.95'),
            max_price=previous_mean * Decimal('1.05'),
            std_dev=previous_mean * Decimal('0.05'),
            count=10
        )
        
        comparison = ComparisonResult(
            current_week_stats=current_stats,
            previous_week_stats=previous_stats,
            average_price_difference=current_mean - previous_mean,
            percentage_change=-significant_change,
            trend_direction=""
        )
        
        # Test with default threshold (10%)
        alerts = analyzer.detect_significant_changes(comparison)
        
        # Should detect the significant change
        assert len(alerts) > 0
        assert any("decreased" in alert for alert in alerts)
        assert any(f"{significant_change * 100:.1f}%" in alert for alert in alerts)
    
    @given(
        base_price=st.decimals(min_value=Decimal('50.00'), max_value=Decimal('500.00'), places=2),
        small_change=st.decimals(min_value=Decimal('0.001'), max_value=Decimal('0.05'), places=3)
    )
    @settings(max_examples=100)
    def test_insignificant_price_change_not_detected(self, base_price, small_change):
        """
        Test that insignificant price changes are not detected as significant.
        """
        analyzer = PriceAnalyzer()
        
        # Create comparison with insignificant change
        current_mean = base_price * (Decimal('1') + small_change)
        previous_mean = base_price
        
        current_stats = Statistics(
            mean=current_mean,
            median=current_mean,
            min_price=current_mean * Decimal('0.95'),
            max_price=current_mean * Decimal('1.05'),
            std_dev=current_mean * Decimal('0.05'),
            count=10
        )
        
        previous_stats = Statistics(
            mean=previous_mean,
            median=previous_mean,
            min_price=previous_mean * Decimal('0.95'),
            max_price=previous_mean * Decimal('1.05'),
            std_dev=previous_mean * Decimal('0.05'),
            count=10
        )
        
        comparison = ComparisonResult(
            current_week_stats=current_stats,
            previous_week_stats=previous_stats,
            average_price_difference=current_mean - previous_mean,
            percentage_change=small_change,
            trend_direction=""
        )
        
        # Test with default threshold (10%)
        alerts = analyzer.detect_significant_changes(comparison)
        
        # Should not detect significant price change
        price_change_alerts = [alert for alert in alerts if "price change detected" in alert]
        assert len(price_change_alerts) == 0
    
    @given(
        comparison=comparison_result_strategy(),
        threshold=st.decimals(min_value=Decimal('0.05'), max_value=Decimal('0.30'), places=3)
    )
    @settings(max_examples=100)
    def test_threshold_based_detection_accuracy(self, comparison, threshold):
        """
        Test that pattern detection respects custom thresholds accurately.
        """
        analyzer = PriceAnalyzer()
        
        # Get alerts with custom threshold
        alerts = analyzer.detect_significant_changes(comparison, threshold)
        
        # Check if change exceeds threshold
        abs_change = abs(comparison.percentage_change)
        should_detect = abs_change >= threshold
        
        # Verify detection accuracy
        price_change_alerts = [alert for alert in alerts if "price change detected" in alert]
        
        if should_detect:
            # Should have detected the change
            assert len(price_change_alerts) > 0
            # Alert should contain the correct percentage
            assert any(f"{abs_change * 100:.1f}%" in alert for alert in price_change_alerts)
        else:
            # Should not have detected the change
            assert len(price_change_alerts) == 0
    
    @given(comparison=comparison_result_strategy())
    @settings(max_examples=100)
    def test_volatility_pattern_detection(self, comparison):
        """
        Test that high volatility patterns are detected correctly.
        """
        analyzer = PriceAnalyzer()
        
        # Modify comparison to have high volatility
        high_volatility_std = comparison.current_week_stats.mean * Decimal('0.25')  # 25% volatility
        
        high_volatility_stats = Statistics(
            mean=comparison.current_week_stats.mean,
            median=comparison.current_week_stats.median,
            min_price=comparison.current_week_stats.min_price,
            max_price=comparison.current_week_stats.max_price,
            std_dev=high_volatility_std,
            count=comparison.current_week_stats.count
        )
        
        high_volatility_comparison = ComparisonResult(
            current_week_stats=high_volatility_stats,
            previous_week_stats=comparison.previous_week_stats,
            average_price_difference=comparison.average_price_difference,
            percentage_change=comparison.percentage_change,
            trend_direction=comparison.trend_direction
        )
        
        alerts = analyzer.detect_significant_changes(high_volatility_comparison)
        
        # Should detect high volatility
        volatility_alerts = [alert for alert in alerts if "volatility" in alert]
        assert len(volatility_alerts) > 0
        assert any("25." in alert for alert in volatility_alerts)  # Should mention ~25% volatility
    
    @given(comparison=comparison_result_strategy())
    @settings(max_examples=100)
    def test_wide_price_range_pattern_detection(self, comparison):
        """
        Test that wide price range patterns are detected correctly.
        """
        analyzer = PriceAnalyzer()
        
        # Modify comparison to have wide price range
        mean_price = comparison.current_week_stats.mean
        wide_range_min = mean_price * Decimal('0.65')  # 35% below mean
        wide_range_max = mean_price * Decimal('1.35')  # 35% above mean
        
        wide_range_stats = Statistics(
            mean=mean_price,
            median=comparison.current_week_stats.median,
            min_price=wide_range_min,
            max_price=wide_range_max,
            std_dev=comparison.current_week_stats.std_dev,
            count=comparison.current_week_stats.count
        )
        
        wide_range_comparison = ComparisonResult(
            current_week_stats=wide_range_stats,
            previous_week_stats=comparison.previous_week_stats,
            average_price_difference=comparison.average_price_difference,
            percentage_change=comparison.percentage_change,
            trend_direction=comparison.trend_direction
        )
        
        alerts = analyzer.detect_significant_changes(wide_range_comparison)
        
        # Should detect wide price range
        range_alerts = [alert for alert in alerts if "range" in alert]
        assert len(range_alerts) > 0
        
        # Calculate expected range
        expected_range = wide_range_max - wide_range_min
        assert any(f"${expected_range:.2f}" in alert for alert in range_alerts)
    
    @given(
        previous_week=st.lists(
            price_record_strategy(price_range=(50.0, 500.0)), 
            min_size=5, max_size=15
        ),
        current_week=st.lists(
            price_record_strategy(price_range=(50.0, 500.0)), 
            min_size=5, max_size=15
        )
    )
    @settings(max_examples=10)
    def test_end_to_end_pattern_detection(self, previous_week, current_week):
        """
        Test end-to-end pattern detection with real price records.
        """
        analyzer = PriceAnalyzer()
        
        # Ensure we have valid data
        assume(len(previous_week) > 0 and len(current_week) > 0)
        
        # Calculate comparison
        comparison = analyzer.compare_weeks(current_week, previous_week)
        
        # Detect patterns
        alerts = analyzer.detect_significant_changes(comparison)
        
        # Verify alerts are properly formatted
        for alert in alerts:
            assert isinstance(alert, str)
            assert len(alert) > 0
            # Should contain meaningful information
            assert any(keyword in alert.lower() for keyword in [
                'price', 'change', 'volatility', 'range', 'detected'
            ])
    
    @given(comparison=comparison_result_strategy())
    @settings(max_examples=10)
    def test_pattern_detection_consistency(self, comparison):
        """
        Test that pattern detection is consistent across multiple calls.
        """
        analyzer = PriceAnalyzer()
        
        # Run detection multiple times
        alerts1 = analyzer.detect_significant_changes(comparison)
        alerts2 = analyzer.detect_significant_changes(comparison)
        alerts3 = analyzer.detect_significant_changes(comparison)
        
        # Results should be identical
        assert alerts1 == alerts2 == alerts3
    
    @given(
        threshold1=st.decimals(min_value=Decimal('0.05'), max_value=Decimal('0.15'), places=3),
        threshold2=st.decimals(min_value=Decimal('0.20'), max_value=Decimal('0.40'), places=3)
    )
    @settings(max_examples=10)
    def test_threshold_ordering_property(self, threshold1, threshold2):
        """
        Test that lower thresholds detect more or equal patterns than higher thresholds.
        """
        assume(threshold1 < threshold2)
        
        analyzer = PriceAnalyzer()
        
        # Create a comparison with moderate change
        base_price = Decimal('100.00')
        change_percentage = (threshold1 + threshold2) / 2  # Between the two thresholds
        
        current_mean = base_price * (Decimal('1') + change_percentage)
        
        current_stats = Statistics(
            mean=current_mean,
            median=current_mean,
            min_price=current_mean * Decimal('0.95'),
            max_price=current_mean * Decimal('1.05'),
            std_dev=current_mean * Decimal('0.05'),
            count=10
        )
        
        previous_stats = Statistics(
            mean=base_price,
            median=base_price,
            min_price=base_price * Decimal('0.95'),
            max_price=base_price * Decimal('1.05'),
            std_dev=base_price * Decimal('0.05'),
            count=10
        )
        
        comparison = ComparisonResult(
            current_week_stats=current_stats,
            previous_week_stats=previous_stats,
            average_price_difference=current_mean - base_price,
            percentage_change=change_percentage,
            trend_direction=""
        )
        
        # Get alerts with both thresholds
        alerts_low = analyzer.detect_significant_changes(comparison, threshold1)
        alerts_high = analyzer.detect_significant_changes(comparison, threshold2)
        
        # Count price change alerts specifically
        price_alerts_low = [a for a in alerts_low if "price change detected" in a]
        price_alerts_high = [a for a in alerts_high if "price change detected" in a]
        
        # Lower threshold should detect more or equal alerts
        assert len(price_alerts_low) >= len(price_alerts_high)
    
    def test_pattern_detection_error_handling(self):
        """
        Test that pattern detection handles error conditions gracefully.
        """
        analyzer = PriceAnalyzer()
        
        # Test with None
        with pytest.raises((ValueError, TypeError, AttributeError)):
            analyzer.detect_significant_changes(None)
    
    @given(
        base_price=st.decimals(min_value=Decimal('10.00'), max_value=Decimal('1000.00'), places=2),
        zero_change=st.just(Decimal('0.00'))
    )
    @settings(max_examples=10)
    def test_no_change_pattern_detection(self, base_price, zero_change):
        """
        Test pattern detection when there's no price change.
        """
        analyzer = PriceAnalyzer()
        
        # Create comparison with no change
        stats = Statistics(
            mean=base_price,
            median=base_price,
            min_price=base_price,
            max_price=base_price,
            std_dev=Decimal('0.00'),
            count=10
        )
        
        comparison = ComparisonResult(
            current_week_stats=stats,
            previous_week_stats=stats,
            average_price_difference=zero_change,
            percentage_change=zero_change,
            trend_direction="stable"
        )
        
        alerts = analyzer.detect_significant_changes(comparison)
        
        # Should not detect any significant price changes
        price_change_alerts = [alert for alert in alerts if "price change detected" in alert]
        assert len(price_change_alerts) == 0
        
        # Should not detect volatility (std_dev is 0)
        volatility_alerts = [alert for alert in alerts if "volatility" in alert]
        assert len(volatility_alerts) == 0
        
        # Should not detect wide range (min == max)
        range_alerts = [alert for alert in alerts if "range" in alert]
        assert len(range_alerts) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
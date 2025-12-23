"""
Property-based tests for week-over-week comparison accuracy.

**Feature: memory-price-monitor, Property 12: Week-over-week comparison accuracy**
**Validates: Requirements 4.2**
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
def price_record_strategy(draw, base_date=None, price_range=(10.0, 1000.0)):
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
def week_price_records_strategy(draw, week_start=None, min_records=1, max_records=50):
    """Generate a week's worth of price records."""
    if week_start is None:
        week_start = datetime(2024, 1, 1)
    
    # Generate number of records for the week
    num_records = draw(st.integers(min_value=min_records, max_value=max_records))
    
    # Generate base price for the week (to ensure some consistency)
    base_price = draw(st.decimals(min_value=Decimal('10.00'), max_value=Decimal('1000.00'), places=2))
    
    # Generate price variation factor (how much prices can vary within the week)
    variation_factor = draw(st.decimals(min_value=Decimal('0.05'), max_value=Decimal('0.30'), places=2))
    
    records = []
    for i in range(num_records):
        # Generate price with some variation around base price
        price_variation = draw(st.decimals(
            min_value=-variation_factor, 
            max_value=variation_factor, 
            places=3
        ))
        price = base_price * (Decimal('1') + price_variation)
        price = max(price, Decimal('0.01'))  # Ensure positive price
        
        # Generate timestamp within the week
        day_offset = draw(st.integers(min_value=0, max_value=6))
        hour_offset = draw(st.integers(min_value=0, max_value=23))
        minute_offset = draw(st.integers(min_value=0, max_value=59))
        
        timestamp = week_start + timedelta(
            days=day_offset, 
            hours=hour_offset, 
            minutes=minute_offset
        )
        
        record = PriceRecord(
            id=i + 1,
            product_id=1,  # Same product for all records in the week
            current_price=price,
            original_price=price * draw(st.decimals(min_value=Decimal('1.0'), max_value=Decimal('1.5'), places=2)),
            recorded_at=timestamp,
            metadata={}
        )
        records.append(record)
    
    return records


@st.composite
def consecutive_weeks_strategy(draw):
    """Generate two consecutive weeks of price data."""
    # Start with a random week
    week1_start = draw(st.datetimes(
        min_value=datetime(2024, 1, 1),
        max_value=datetime(2024, 12, 1)
    ))
    week2_start = week1_start + timedelta(days=7)
    
    # Generate records for both weeks
    week1_records = draw(week_price_records_strategy(week_start=week1_start, min_records=1, max_records=30))
    week2_records = draw(week_price_records_strategy(week_start=week2_start, min_records=1, max_records=30))
    
    return week1_records, week2_records


class TestWeekOverWeekComparisonAccuracy:
    """Test week-over-week comparison accuracy property."""
    
    @given(consecutive_weeks=consecutive_weeks_strategy())
    @settings(max_examples=100)
    def test_week_over_week_comparison_mathematical_accuracy(self, consecutive_weeks):
        """
        **Feature: memory-price-monitor, Property 12: Week-over-week comparison accuracy**
        
        For any two consecutive weeks of price data, the comparison should calculate 
        mathematically correct average price differences and percentage changes.
        
        **Validates: Requirements 4.2**
        """
        previous_week, current_week = consecutive_weeks
        
        analyzer = PriceAnalyzer()
        
        # Perform the comparison
        comparison = analyzer.compare_weeks(current_week, previous_week)
        
        # Verify the comparison result structure
        assert isinstance(comparison, ComparisonResult)
        assert isinstance(comparison.current_week_stats, Statistics)
        assert isinstance(comparison.previous_week_stats, Statistics)
        assert isinstance(comparison.average_price_difference, Decimal)
        assert isinstance(comparison.percentage_change, Decimal)
        assert comparison.trend_direction in ['up', 'down', 'stable']
        
        # Manually calculate expected values for verification
        current_prices = [float(record.current_price) for record in current_week]
        previous_prices = [float(record.current_price) for record in previous_week]
        
        expected_current_mean = Decimal(str(sum(current_prices) / len(current_prices))).quantize(
            Decimal('0.01'), rounding=ROUND_HALF_UP
        )
        expected_previous_mean = Decimal(str(sum(previous_prices) / len(previous_prices))).quantize(
            Decimal('0.01'), rounding=ROUND_HALF_UP
        )
        
        # Verify average price calculations
        assert comparison.current_week_stats.mean == expected_current_mean
        assert comparison.previous_week_stats.mean == expected_previous_mean
        
        # Verify price difference calculation
        expected_difference = expected_current_mean - expected_previous_mean
        assert comparison.average_price_difference == expected_difference
        
        # Verify percentage change calculation
        if expected_previous_mean != 0:
            expected_percentage = (expected_difference / expected_previous_mean).quantize(
                Decimal('0.0001'), rounding=ROUND_HALF_UP
            )
            assert comparison.percentage_change == expected_percentage
        else:
            assert comparison.percentage_change == Decimal('0')
        
        # Verify trend direction logic
        if comparison.percentage_change > Decimal('0.01'):
            assert comparison.trend_direction == 'up'
        elif comparison.percentage_change < Decimal('-0.01'):
            assert comparison.trend_direction == 'down'
        else:
            assert comparison.trend_direction == 'stable'
    
    @given(consecutive_weeks=consecutive_weeks_strategy())
    @settings(max_examples=10)
    def test_comparison_symmetry_property(self, consecutive_weeks):
        """
        Test that comparison results maintain mathematical symmetry.
        If A compared to B shows +X% change, then B compared to A should show approximately -X% change.
        """
        week1, week2 = consecutive_weeks
        
        analyzer = PriceAnalyzer()
        
        # Compare week2 to week1 (forward)
        forward_comparison = analyzer.compare_weeks(week2, week1)
        
        # Compare week1 to week2 (reverse)
        reverse_comparison = analyzer.compare_weeks(week1, week2)
        
        # The percentage changes should be approximately opposite
        # (not exactly due to different denominators, but the relationship should hold)
        forward_pct = forward_comparison.percentage_change
        reverse_pct = reverse_comparison.percentage_change
        
        # If both are non-zero, they should have opposite signs
        if forward_pct != 0 and reverse_pct != 0:
            assert (forward_pct > 0) != (reverse_pct > 0), "Percentage changes should have opposite signs"
        
        # The price differences should be exact opposites
        assert forward_comparison.average_price_difference == -reverse_comparison.average_price_difference
    
    @given(week_records=week_price_records_strategy(min_records=2, max_records=20))
    @settings(max_examples=10)
    def test_comparison_with_identical_weeks(self, week_records):
        """
        Test comparison behavior when both weeks have identical data.
        """
        analyzer = PriceAnalyzer()
        
        # Use the same data for both weeks
        comparison = analyzer.compare_weeks(week_records, week_records)
        
        # All changes should be zero
        assert comparison.average_price_difference == Decimal('0')
        assert comparison.percentage_change == Decimal('0')
        assert comparison.trend_direction == 'stable'
        
        # Statistics should be identical
        assert comparison.current_week_stats.mean == comparison.previous_week_stats.mean
        assert comparison.current_week_stats.median == comparison.previous_week_stats.median
        assert comparison.current_week_stats.min_price == comparison.previous_week_stats.min_price
        assert comparison.current_week_stats.max_price == comparison.previous_week_stats.max_price
        assert comparison.current_week_stats.count == comparison.previous_week_stats.count
    
    @given(
        current_week=week_price_records_strategy(min_records=1, max_records=10),
        previous_week=week_price_records_strategy(min_records=1, max_records=10)
    )
    @settings(max_examples=10)
    def test_comparison_precision_consistency(self, current_week, previous_week):
        """
        Test that comparison calculations maintain consistent precision.
        """
        analyzer = PriceAnalyzer()
        
        comparison = analyzer.compare_weeks(current_week, previous_week)
        
        # All monetary values should be rounded to 2 decimal places
        assert comparison.current_week_stats.mean.as_tuple().exponent >= -2
        assert comparison.previous_week_stats.mean.as_tuple().exponent >= -2
        assert comparison.average_price_difference.as_tuple().exponent >= -2
        
        # Percentage should be rounded to 4 decimal places (0.01%)
        assert comparison.percentage_change.as_tuple().exponent >= -4
        
        # Verify that calculations are internally consistent
        calculated_diff = comparison.current_week_stats.mean - comparison.previous_week_stats.mean
        assert comparison.average_price_difference == calculated_diff
    
    @given(consecutive_weeks=consecutive_weeks_strategy())
    @settings(max_examples=10)
    def test_comparison_handles_edge_cases(self, consecutive_weeks):
        """
        Test that comparison handles edge cases gracefully.
        """
        previous_week, current_week = consecutive_weeks
        
        analyzer = PriceAnalyzer()
        
        # Test with normal data
        comparison = analyzer.compare_weeks(current_week, previous_week)
        assert comparison is not None
        
        # Test with single record weeks
        single_current = [current_week[0]] if current_week else [PriceRecord(
            id=1, product_id=1, current_price=Decimal('100.00'), recorded_at=datetime.now()
        )]
        single_previous = [previous_week[0]] if previous_week else [PriceRecord(
            id=2, product_id=1, current_price=Decimal('90.00'), recorded_at=datetime.now() - timedelta(days=7)
        )]
        
        single_comparison = analyzer.compare_weeks(single_current, single_previous)
        assert single_comparison is not None
        assert single_comparison.current_week_stats.count == 1
        assert single_comparison.previous_week_stats.count == 1
        
        # Standard deviation should be 0 for single records
        assert single_comparison.current_week_stats.std_dev == Decimal('0.00')
        assert single_comparison.previous_week_stats.std_dev == Decimal('0.00')
    
    def test_comparison_error_handling(self):
        """
        Test that comparison properly handles error conditions.
        """
        analyzer = PriceAnalyzer()
        
        # Test with empty current week
        with pytest.raises(ValueError, match="Current week data is required"):
            analyzer.compare_weeks([], [PriceRecord(id=1, product_id=1, current_price=Decimal('100.00'))])
        
        # Test with empty previous week
        with pytest.raises(ValueError, match="Previous week data is required"):
            analyzer.compare_weeks([PriceRecord(id=1, product_id=1, current_price=Decimal('100.00'))], [])
        
        # Test with both empty
        with pytest.raises(ValueError):
            analyzer.compare_weeks([], [])


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
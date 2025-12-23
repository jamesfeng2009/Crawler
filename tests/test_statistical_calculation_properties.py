"""
Property-based tests for statistical calculation correctness.

**Feature: memory-price-monitor, Property 24: Statistical calculation correctness**
**Validates: Requirements 7.2**
"""

import pytest
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import List
from hypothesis import given, strategies as st, assume
from hypothesis import settings
from pathlib import Path
import sys
import statistics

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from memory_price_monitor.data.models import PriceRecord
from memory_price_monitor.analysis.analyzer import PriceAnalyzer, Statistics, MovingAverageData


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
    timestamp_offset = draw(st.integers(min_value=0, max_value=86400 * 30))  # Within a month
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
def price_records_list_strategy(draw, min_records=1, max_records=100):
    """Generate a list of price records."""
    num_records = draw(st.integers(min_value=min_records, max_value=max_records))
    
    # Generate base date
    base_date = draw(st.datetimes(
        min_value=datetime(2024, 1, 1),
        max_value=datetime(2024, 12, 1)
    ))
    
    records = []
    for i in range(num_records):
        record = draw(price_record_strategy(base_date=base_date))
        record.id = i + 1  # Ensure unique IDs
        records.append(record)
    
    return records


class TestStatisticalCalculationCorrectness:
    """Test statistical calculation correctness property."""
    
    @given(records=price_records_list_strategy(min_records=1, max_records=50))
    @settings(max_examples=100)
    def test_statistical_calculation_mathematical_accuracy(self, records):
        """
        **Feature: memory-price-monitor, Property 24: Statistical calculation correctness**
        
        For any price data set, calculated moving averages and volatility metrics 
        should be mathematically accurate.
        
        **Validates: Requirements 7.2**
        """
        analyzer = PriceAnalyzer()
        
        # Calculate statistics using the analyzer
        stats = analyzer.calculate_statistics(records)
        
        # Verify the statistics structure
        assert isinstance(stats, Statistics)
        assert isinstance(stats.mean, Decimal)
        assert isinstance(stats.median, Decimal)
        assert isinstance(stats.min_price, Decimal)
        assert isinstance(stats.max_price, Decimal)
        assert isinstance(stats.std_dev, Decimal)
        assert isinstance(stats.count, int)
        
        # Extract prices for manual calculation
        prices = [float(record.current_price) for record in records]
        
        # Verify count
        assert stats.count == len(records)
        
        # Verify mean calculation
        expected_mean = Decimal(str(statistics.mean(prices))).quantize(
            Decimal('0.01'), rounding=ROUND_HALF_UP
        )
        assert stats.mean == expected_mean
        
        # Verify median calculation
        expected_median = Decimal(str(statistics.median(prices))).quantize(
            Decimal('0.01'), rounding=ROUND_HALF_UP
        )
        assert stats.median == expected_median
        
        # Verify min/max calculations
        expected_min = Decimal(str(min(prices))).quantize(
            Decimal('0.01'), rounding=ROUND_HALF_UP
        )
        expected_max = Decimal(str(max(prices))).quantize(
            Decimal('0.01'), rounding=ROUND_HALF_UP
        )
        assert stats.min_price == expected_min
        assert stats.max_price == expected_max
        
        # Verify standard deviation calculation
        if len(prices) > 1:
            expected_std = Decimal(str(statistics.stdev(prices))).quantize(
                Decimal('0.01'), rounding=ROUND_HALF_UP
            )
            assert stats.std_dev == expected_std
        else:
            # Single value should have zero standard deviation
            assert stats.std_dev == Decimal('0.00')
    
    @given(records=price_records_list_strategy(min_records=2, max_records=20))
    @settings(max_examples=10)
    def test_statistical_calculation_consistency(self, records):
        """
        Test that statistical calculations are consistent across multiple calls.
        """
        analyzer = PriceAnalyzer()
        
        # Calculate statistics multiple times
        stats1 = analyzer.calculate_statistics(records)
        stats2 = analyzer.calculate_statistics(records)
        
        # Results should be identical
        assert stats1.mean == stats2.mean
        assert stats1.median == stats2.median
        assert stats1.min_price == stats2.min_price
        assert stats1.max_price == stats2.max_price
        assert stats1.std_dev == stats2.std_dev
        assert stats1.count == stats2.count
    
    @given(records=price_records_list_strategy(min_records=1, max_records=30))
    @settings(max_examples=10)
    def test_statistical_calculation_invariants(self, records):
        """
        Test that statistical calculations maintain mathematical invariants.
        """
        analyzer = PriceAnalyzer()
        stats = analyzer.calculate_statistics(records)
        
        # Basic invariants
        assert stats.count > 0
        assert stats.min_price <= stats.max_price
        assert stats.min_price <= stats.mean <= stats.max_price
        assert stats.min_price <= stats.median <= stats.max_price
        assert stats.std_dev >= Decimal('0')
        
        # All prices should be positive
        assert stats.min_price > Decimal('0')
        assert stats.max_price > Decimal('0')
        assert stats.mean > Decimal('0')
        assert stats.median > Decimal('0')
        
        # Standard deviation should be zero for single records
        if stats.count == 1:
            assert stats.std_dev == Decimal('0.00')
            assert stats.min_price == stats.max_price == stats.mean == stats.median
    
    @given(
        base_price=st.decimals(min_value=Decimal('10.00'), max_value=Decimal('1000.00'), places=2),
        num_records=st.integers(min_value=5, max_value=20)
    )
    @settings(max_examples=10)
    def test_statistical_calculation_with_identical_prices(self, base_price, num_records):
        """
        Test statistical calculations with identical price values.
        """
        # Create records with identical prices
        records = []
        base_date = datetime(2024, 1, 1)
        
        for i in range(num_records):
            record = PriceRecord(
                id=i + 1,
                product_id=1,
                current_price=base_price,
                original_price=base_price,
                recorded_at=base_date + timedelta(hours=i),
                metadata={}
            )
            records.append(record)
        
        analyzer = PriceAnalyzer()
        stats = analyzer.calculate_statistics(records)
        
        # All central tendency measures should equal the base price
        assert stats.mean == base_price
        assert stats.median == base_price
        assert stats.min_price == base_price
        assert stats.max_price == base_price
        
        # Standard deviation should be zero
        assert stats.std_dev == Decimal('0.00')
        
        # Count should match
        assert stats.count == num_records
    
    @given(records=price_records_list_strategy(min_records=1, max_records=50))
    @settings(max_examples=10)
    def test_statistical_calculation_precision_consistency(self, records):
        """
        Test that statistical calculations maintain consistent precision.
        """
        analyzer = PriceAnalyzer()
        stats = analyzer.calculate_statistics(records)
        
        # All monetary values should be rounded to 2 decimal places
        assert stats.mean.as_tuple().exponent >= -2
        assert stats.median.as_tuple().exponent >= -2
        assert stats.min_price.as_tuple().exponent >= -2
        assert stats.max_price.as_tuple().exponent >= -2
        assert stats.std_dev.as_tuple().exponent >= -2
        
        # Verify that precision is exactly 2 decimal places for monetary values
        assert len(str(stats.mean).split('.')[-1]) <= 2
        assert len(str(stats.median).split('.')[-1]) <= 2
        assert len(str(stats.min_price).split('.')[-1]) <= 2
        assert len(str(stats.max_price).split('.')[-1]) <= 2
        assert len(str(stats.std_dev).split('.')[-1]) <= 2
    
    @given(
        records1=price_records_list_strategy(min_records=2, max_records=10),
        records2=price_records_list_strategy(min_records=2, max_records=10)
    )
    @settings(max_examples=10)
    def test_statistical_calculation_additivity_properties(self, records1, records2):
        """
        Test mathematical properties when combining datasets.
        """
        analyzer = PriceAnalyzer()
        
        # Calculate statistics for individual datasets
        stats1 = analyzer.calculate_statistics(records1)
        stats2 = analyzer.calculate_statistics(records2)
        
        # Calculate statistics for combined dataset
        combined_records = records1 + records2
        combined_stats = analyzer.calculate_statistics(combined_records)
        
        # Count should be additive
        assert combined_stats.count == stats1.count + stats2.count
        
        # Min should be the minimum of both mins
        expected_min = min(stats1.min_price, stats2.min_price)
        assert combined_stats.min_price == expected_min
        
        # Max should be the maximum of both maxes
        expected_max = max(stats1.max_price, stats2.max_price)
        assert combined_stats.max_price == expected_max
        
        # Combined mean should be within the range of individual means
        min_mean = min(stats1.mean, stats2.mean)
        max_mean = max(stats1.mean, stats2.mean)
        assert min_mean <= combined_stats.mean <= max_mean
    
    def test_statistical_calculation_error_handling(self):
        """
        Test that statistical calculations properly handle error conditions.
        """
        analyzer = PriceAnalyzer()
        
        # Test with empty list
        with pytest.raises(ValueError, match="No records provided for statistics calculation"):
            analyzer.calculate_statistics([])
        
        # Test with None
        with pytest.raises((ValueError, TypeError)):
            analyzer.calculate_statistics(None)
    
    @given(
        price_value=st.decimals(min_value=Decimal('0.01'), max_value=Decimal('999999.99'), places=2)
    )
    @settings(max_examples=10)
    def test_single_record_statistics(self, price_value):
        """
        Test statistical calculations with a single price record.
        """
        record = PriceRecord(
            id=1,
            product_id=1,
            current_price=price_value,
            original_price=price_value,
            recorded_at=datetime.now(),
            metadata={}
        )
        
        analyzer = PriceAnalyzer()
        stats = analyzer.calculate_statistics([record])
        
        # All measures should equal the single price
        assert stats.mean == price_value
        assert stats.median == price_value
        assert stats.min_price == price_value
        assert stats.max_price == price_value
        assert stats.std_dev == Decimal('0.00')
        assert stats.count == 1
    
    @given(records=price_records_list_strategy(min_records=10, max_records=50))
    @settings(max_examples=10)
    def test_statistical_calculation_order_independence(self, records):
        """
        Test that statistical calculations are independent of record order.
        """
        import random
        
        analyzer = PriceAnalyzer()
        
        # Calculate statistics with original order
        original_stats = analyzer.calculate_statistics(records)
        
        # Shuffle the records and calculate again
        shuffled_records = records.copy()
        random.shuffle(shuffled_records)
        shuffled_stats = analyzer.calculate_statistics(shuffled_records)
        
        # Results should be identical regardless of order
        assert original_stats.mean == shuffled_stats.mean
        assert original_stats.median == shuffled_stats.median
        assert original_stats.min_price == shuffled_stats.min_price
        assert original_stats.max_price == shuffled_stats.max_price
        assert original_stats.std_dev == shuffled_stats.std_dev
        assert original_stats.count == shuffled_stats.count
    
    @given(
        records=price_records_list_strategy(min_records=7, max_records=30),
        period=st.integers(min_value=3, max_value=7)
    )
    @settings(max_examples=100)
    def test_moving_average_calculation_accuracy(self, records, period):
        """
        **Feature: memory-price-monitor, Property 24: Statistical calculation correctness**
        
        For any price data set, calculated moving averages should be mathematically accurate.
        
        **Validates: Requirements 7.2**
        """
        # Ensure we have enough records for the period
        assume(len(records) >= period)
        
        analyzer = PriceAnalyzer()
        
        # Sort records by timestamp for consistent calculation
        sorted_records = sorted(records, key=lambda r: r.recorded_at)
        
        # Calculate moving average
        ma_data = analyzer.calculate_moving_average(sorted_records, period)
        
        # Verify structure
        assert isinstance(ma_data, MovingAverageData)
        assert ma_data.period == period
        assert len(ma_data.values) == len(sorted_records) - period + 1
        assert len(ma_data.timestamps) == len(ma_data.values)
        
        # Verify each moving average value manually
        for i, ma_value in enumerate(ma_data.values):
            # Get the window of records for this MA point
            window_start = i
            window_end = i + period
            window_records = sorted_records[window_start:window_end]
            
            # Calculate expected average
            window_prices = [float(record.current_price) for record in window_records]
            expected_avg = Decimal(str(statistics.mean(window_prices))).quantize(
                Decimal('0.01'), rounding=ROUND_HALF_UP
            )
            
            # Verify the calculated MA value matches expected
            assert ma_value == expected_avg
            
            # Verify timestamp corresponds to the last record in the window
            assert ma_data.timestamps[i] == window_records[-1].recorded_at
    
    @given(
        records=price_records_list_strategy(min_records=10, max_records=30),
        period=st.integers(min_value=3, max_value=10)
    )
    @settings(max_examples=10)
    def test_exponential_moving_average_properties(self, records, period):
        """
        Test exponential moving average calculation properties.
        """
        assume(len(records) >= period)
        
        analyzer = PriceAnalyzer()
        sorted_records = sorted(records, key=lambda r: r.recorded_at)
        
        # Calculate EMA
        ema_data = analyzer.calculate_exponential_moving_average(sorted_records, period)
        
        # Verify structure
        assert isinstance(ema_data, MovingAverageData)
        assert ema_data.period == period
        assert len(ema_data.values) == len(sorted_records) - period + 1
        assert len(ema_data.timestamps) == len(ema_data.values)
        
        # EMA should be more responsive than SMA (generally)
        sma_data = analyzer.calculate_moving_average(sorted_records, period)
        
        # Both should have same number of values
        assert len(ema_data.values) == len(sma_data.values)
        
        # All values should be positive
        for value in ema_data.values:
            assert value > Decimal('0')
    
    @given(
        records=price_records_list_strategy(min_records=15, max_records=50)
    )
    @settings(max_examples=10)
    def test_long_term_trends_calculation(self, records):
        """
        Test long-term trend calculation with multiple moving averages.
        """
        analyzer = PriceAnalyzer()
        
        # Calculate long-term trends
        trend_data = analyzer.calculate_long_term_trends(records, ma_periods=[7, 14], include_ema=True)
        
        # Should have both SMA and EMA for each period
        expected_keys = ['SMA_7', 'SMA_14', 'EMA_7', 'EMA_14']
        
        # Check which keys should exist based on record count
        for key in expected_keys:
            period = int(key.split('_')[1])
            if len(records) >= period:
                assert key in trend_data
                assert isinstance(trend_data[key], MovingAverageData)
                assert trend_data[key].period == period
    
    @given(
        base_price=st.decimals(min_value=Decimal('10.00'), max_value=Decimal('100.00'), places=2),
        num_records=st.integers(min_value=20, max_value=40),
        period=st.integers(min_value=5, max_value=10)
    )
    @settings(max_examples=10)
    def test_moving_average_smoothing_property(self, base_price, num_records, period):
        """
        Test that moving averages provide smoothing effect on volatile data.
        """
        # Create records with high volatility around base price
        records = []
        base_date = datetime(2024, 1, 1)
        
        for i in range(num_records):
            # Add significant random variation (±20%)
            variation = Decimal(str((i % 3 - 1) * 0.2))  # -20%, 0%, +20% pattern
            price = base_price * (Decimal('1') + variation)
            price = price.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            
            record = PriceRecord(
                id=i + 1,
                product_id=1,
                current_price=price,
                original_price=price,
                recorded_at=base_date + timedelta(hours=i),
                metadata={}
            )
            records.append(record)
        
        analyzer = PriceAnalyzer()
        
        # Calculate moving average
        ma_data = analyzer.calculate_moving_average(records, period)
        
        # Moving average should be less volatile than original prices
        original_prices = [record.current_price for record in records]
        original_std = Decimal(str(statistics.stdev([float(p) for p in original_prices]))).quantize(
            Decimal('0.01'), rounding=ROUND_HALF_UP
        )
        
        ma_std = Decimal(str(statistics.stdev([float(v) for v in ma_data.values]))).quantize(
            Decimal('0.01'), rounding=ROUND_HALF_UP
        )
        
        # Moving average should generally be less volatile (allow some tolerance)
        assert ma_std <= original_std * Decimal('1.1')  # Allow 10% tolerance
    
    @given(
        records=price_records_list_strategy(min_records=20, max_records=40)
    )
    @settings(max_examples=10)
    def test_trend_signals_generation(self, records):
        """
        Test trend signal generation based on moving average crossovers.
        """
        assume(len(records) >= 14)  # Need enough for both short and long MA
        
        analyzer = PriceAnalyzer()
        
        # Generate trend signals
        signals = analyzer.get_trend_signals(records, short_period=7, long_period=14)
        
        # Verify signal structure
        assert 'current_signal' in signals
        assert 'trend_strength' in signals
        assert 'crossover_signals' in signals
        assert 'short_ma_current' in signals
        assert 'long_ma_current' in signals
        assert 'analysis_timestamp' in signals
        
        # Current signal should be either 'bullish' or 'bearish'
        if signals['current_signal'] is not None:
            assert signals['current_signal'] in ['bullish', 'bearish']
        
        # Trend strength should be non-negative
        assert signals['trend_strength'] >= Decimal('0')
        
        # Crossover signals should be a list
        assert isinstance(signals['crossover_signals'], list)
        
        # Current MA values should be Decimal
        if signals['short_ma_current'] is not None:
            assert isinstance(signals['short_ma_current'], Decimal)
        if signals['long_ma_current'] is not None:
            assert isinstance(signals['long_ma_current'], Decimal)
    
    @given(
        records=price_records_list_strategy(min_records=5, max_records=20),
        period1=st.integers(min_value=3, max_value=7),
        period2=st.integers(min_value=3, max_value=7)
    )
    @settings(max_examples=10)
    def test_moving_average_consistency(self, records, period1, period2):
        """
        Test that moving average calculations are consistent across multiple calls.
        """
        assume(len(records) >= max(period1, period2))
        
        analyzer = PriceAnalyzer()
        
        # Calculate moving averages multiple times
        ma1_first = analyzer.calculate_moving_average(records, period1)
        ma1_second = analyzer.calculate_moving_average(records, period1)
        
        # Results should be identical
        assert ma1_first.period == ma1_second.period
        assert ma1_first.values == ma1_second.values
        assert ma1_first.timestamps == ma1_second.timestamps
    
    def test_moving_average_error_handling(self):
        """
        Test that moving average calculations handle error conditions properly.
        """
        analyzer = PriceAnalyzer()
        
        # Test with empty list
        with pytest.raises(ValueError, match="No records provided"):
            analyzer.calculate_moving_average([], 5)
        
        # Test with invalid period
        records = [PriceRecord(
            id=1, product_id=1, current_price=Decimal('100.00'),
            original_price=Decimal('100.00'), recorded_at=datetime.now(), metadata={}
        )]
        
        with pytest.raises(ValueError, match="Period must be positive"):
            analyzer.calculate_moving_average(records, 0)
        
        with pytest.raises(ValueError, match="Period must be positive"):
            analyzer.calculate_moving_average(records, -1)
        
        # Test with insufficient records
        with pytest.raises(ValueError, match="Not enough records"):
            analyzer.calculate_moving_average(records, 5)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
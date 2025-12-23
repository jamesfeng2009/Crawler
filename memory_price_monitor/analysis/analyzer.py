"""
Price analysis engine for trend calculations and comparisons.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, timedelta
from statistics import mean, median, stdev
import math

from memory_price_monitor.data.models import PriceRecord


@dataclass
class Statistics:
    """Statistical data for price analysis."""
    mean: Decimal
    median: Decimal
    min_price: Decimal
    max_price: Decimal
    std_dev: Decimal
    count: int
    
    def __post_init__(self):
        """Ensure all values are Decimal."""
        if not isinstance(self.mean, Decimal):
            self.mean = Decimal(str(self.mean))
        if not isinstance(self.median, Decimal):
            self.median = Decimal(str(self.median))
        if not isinstance(self.min_price, Decimal):
            self.min_price = Decimal(str(self.min_price))
        if not isinstance(self.max_price, Decimal):
            self.max_price = Decimal(str(self.max_price))
        if not isinstance(self.std_dev, Decimal):
            self.std_dev = Decimal(str(self.std_dev))


@dataclass
class MovingAverageData:
    """Moving average calculation results."""
    period: int
    values: List[Decimal]
    timestamps: List[datetime]
    
    def __post_init__(self):
        """Ensure all values are Decimal."""
        self.values = [Decimal(str(v)) if not isinstance(v, Decimal) else v for v in self.values]


@dataclass
class TrendData:
    """Trend analysis data."""
    product_id: str
    period_start: datetime
    period_end: datetime
    statistics: Statistics
    price_points: List[PriceRecord] = field(default_factory=list)
    moving_averages: Dict[int, MovingAverageData] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ComparisonResult:
    """Result of week-over-week comparison."""
    current_week_stats: Statistics
    previous_week_stats: Statistics
    average_price_difference: Decimal
    percentage_change: Decimal
    trend_direction: str  # 'up', 'down', 'stable'
    
    def __post_init__(self):
        """Calculate derived fields."""
        # Ensure Decimal types
        if not isinstance(self.average_price_difference, Decimal):
            self.average_price_difference = Decimal(str(self.average_price_difference))
        if not isinstance(self.percentage_change, Decimal):
            self.percentage_change = Decimal(str(self.percentage_change))
        
        # Determine trend direction
        if self.percentage_change > Decimal('0.01'):  # > 1%
            self.trend_direction = 'up'
        elif self.percentage_change < Decimal('-0.01'):  # < -1%
            self.trend_direction = 'down'
        else:
            self.trend_direction = 'stable'


class PriceAnalyzer:
    """Price analysis engine for trend calculations and comparisons."""
    
    def __init__(self):
        """Initialize price analyzer."""
        self.stability_threshold = Decimal('0.01')  # 1% threshold for stability
    
    def calculate_weekly_trend(self, product_id: str, price_records: List[PriceRecord], 
                              include_moving_averages: bool = True,
                              ma_periods: List[int] = None) -> TrendData:
        """
        Calculate weekly trend for a product.
        
        Args:
            product_id: Product identifier
            price_records: List of price records for the week
            include_moving_averages: Whether to calculate moving averages
            ma_periods: List of periods for moving averages (default: [3, 7, 14])
            
        Returns:
            Trend data with statistics and optional moving averages
        """
        if not price_records:
            raise ValueError("No price records provided")
        
        if ma_periods is None:
            ma_periods = [3, 7, 14]  # Default periods: 3-day, 7-day, 14-day
        
        # Sort records by timestamp
        sorted_records = sorted(price_records, key=lambda r: r.recorded_at)
        
        # Calculate period bounds
        period_start = sorted_records[0].recorded_at
        period_end = sorted_records[-1].recorded_at
        
        # Calculate statistics
        statistics = self.calculate_statistics(price_records)
        
        # Calculate moving averages if requested
        moving_averages = {}
        if include_moving_averages:
            for period in ma_periods:
                if len(sorted_records) >= period:
                    ma_data = self.calculate_moving_average(sorted_records, period)
                    moving_averages[period] = ma_data
        
        return TrendData(
            product_id=product_id,
            period_start=period_start,
            period_end=period_end,
            statistics=statistics,
            price_points=sorted_records,
            moving_averages=moving_averages,
            metadata={
                'analysis_timestamp': datetime.now(),
                'record_count': len(price_records),
                'ma_periods_calculated': list(moving_averages.keys())
            }
        )
    
    def compare_weeks(self, current_week: List[PriceRecord], 
                     previous_week: List[PriceRecord]) -> ComparisonResult:
        """
        Compare two consecutive weeks of price data.
        
        Args:
            current_week: Current week price records
            previous_week: Previous week price records
            
        Returns:
            Comparison result with statistics and changes
        """
        if not current_week:
            raise ValueError("Current week data is required")
        if not previous_week:
            raise ValueError("Previous week data is required")
        
        # Calculate statistics for both weeks
        current_stats = self.calculate_statistics(current_week)
        previous_stats = self.calculate_statistics(previous_week)
        
        # Calculate price difference and percentage change
        avg_price_diff = current_stats.mean - previous_stats.mean
        
        # Avoid division by zero
        if previous_stats.mean == 0:
            percentage_change = Decimal('0')
        else:
            percentage_change = (avg_price_diff / previous_stats.mean).quantize(
                Decimal('0.0001'), rounding=ROUND_HALF_UP
            )
        
        return ComparisonResult(
            current_week_stats=current_stats,
            previous_week_stats=previous_stats,
            average_price_difference=avg_price_diff,
            percentage_change=percentage_change,
            trend_direction=""  # Will be calculated in __post_init__
        )
    
    def calculate_statistics(self, records: List[PriceRecord]) -> Statistics:
        """
        Calculate statistical metrics for price records.
        
        Args:
            records: List of price records
            
        Returns:
            Statistical data
        """
        if not records:
            raise ValueError("No records provided for statistics calculation")
        
        # Extract prices as floats for statistical calculations
        prices = [float(record.current_price) for record in records]
        
        # Calculate statistics
        mean_price = Decimal(str(mean(prices))).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        median_price = Decimal(str(median(prices))).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        min_price = Decimal(str(min(prices))).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        max_price = Decimal(str(max(prices))).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        
        # Calculate standard deviation (handle single value case)
        if len(prices) > 1:
            std_deviation = Decimal(str(stdev(prices))).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        else:
            std_deviation = Decimal('0.00')
        
        return Statistics(
            mean=mean_price,
            median=median_price,
            min_price=min_price,
            max_price=max_price,
            std_dev=std_deviation,
            count=len(records)
        )
    
    def detect_significant_changes(self, comparison: ComparisonResult, 
                                 threshold: Decimal = None) -> List[str]:
        """
        Detect significant price changes based on comparison.
        
        Args:
            comparison: Week-over-week comparison result
            threshold: Significance threshold (default: 10%)
            
        Returns:
            List of detected changes/alerts
        """
        if threshold is None:
            threshold = Decimal('0.10')  # 10% default threshold
        
        alerts = []
        
        # Check for significant price changes
        abs_change = abs(comparison.percentage_change)
        if abs_change >= threshold:
            direction = "increased" if comparison.percentage_change > 0 else "decreased"
            alerts.append(
                f"Significant price change detected: {direction} by "
                f"{abs_change * 100:.1f}% (${comparison.average_price_difference:.2f})"
            )
        
        # Check for high volatility
        current_volatility = comparison.current_week_stats.std_dev / comparison.current_week_stats.mean
        if current_volatility > Decimal('0.20'):  # 20% volatility threshold
            alerts.append(f"High price volatility detected: {current_volatility * 100:.1f}%")
        
        # Check for unusual price ranges
        current_range = comparison.current_week_stats.max_price - comparison.current_week_stats.min_price
        current_range_pct = current_range / comparison.current_week_stats.mean
        if current_range_pct > Decimal('0.30'):  # 30% range threshold
            alerts.append(f"Wide price range detected: ${current_range:.2f} ({current_range_pct * 100:.1f}%)")
        
        return alerts
    
    def calculate_moving_average(self, records: List[PriceRecord], period: int) -> MovingAverageData:
        """
        Calculate moving average for a given period.
        
        Args:
            records: List of price records (should be sorted by timestamp)
            period: Number of periods for moving average
            
        Returns:
            Moving average data with values and timestamps
        """
        if not records:
            raise ValueError("No records provided for moving average calculation")
        
        if period <= 0:
            raise ValueError("Period must be positive")
        
        if len(records) < period:
            raise ValueError(f"Not enough records ({len(records)}) for period {period}")
        
        # Ensure records are sorted by timestamp
        sorted_records = sorted(records, key=lambda r: r.recorded_at)
        
        ma_values = []
        ma_timestamps = []
        
        # Calculate moving average for each valid position
        for i in range(period - 1, len(sorted_records)):
            # Get the window of records for this moving average point
            window_records = sorted_records[i - period + 1:i + 1]
            
            # Calculate average price for this window
            window_prices = [float(record.current_price) for record in window_records]
            avg_price = Decimal(str(mean(window_prices))).quantize(
                Decimal('0.01'), rounding=ROUND_HALF_UP
            )
            
            ma_values.append(avg_price)
            ma_timestamps.append(sorted_records[i].recorded_at)
        
        return MovingAverageData(
            period=period,
            values=ma_values,
            timestamps=ma_timestamps
        )
    
    def calculate_exponential_moving_average(self, records: List[PriceRecord], 
                                           period: int, alpha: Optional[Decimal] = None) -> MovingAverageData:
        """
        Calculate exponential moving average (EMA) for a given period.
        
        Args:
            records: List of price records (should be sorted by timestamp)
            period: Number of periods for EMA calculation
            alpha: Smoothing factor (default: 2/(period+1))
            
        Returns:
            Exponential moving average data
        """
        if not records:
            raise ValueError("No records provided for EMA calculation")
        
        if period <= 0:
            raise ValueError("Period must be positive")
        
        if len(records) < period:
            raise ValueError(f"Not enough records ({len(records)}) for period {period}")
        
        # Calculate alpha if not provided
        if alpha is None:
            alpha = Decimal('2') / (Decimal(str(period)) + Decimal('1'))
        
        # Ensure records are sorted by timestamp
        sorted_records = sorted(records, key=lambda r: r.recorded_at)
        
        ema_values = []
        ema_timestamps = []
        
        # Initialize EMA with simple moving average of first 'period' values
        initial_prices = [float(record.current_price) for record in sorted_records[:period]]
        ema = Decimal(str(mean(initial_prices))).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        
        # Add the initial EMA value
        ema_values.append(ema)
        ema_timestamps.append(sorted_records[period - 1].recorded_at)
        
        # Calculate EMA for remaining records
        for i in range(period, len(sorted_records)):
            current_price = sorted_records[i].current_price
            ema = (alpha * current_price + (Decimal('1') - alpha) * ema).quantize(
                Decimal('0.01'), rounding=ROUND_HALF_UP
            )
            
            ema_values.append(ema)
            ema_timestamps.append(sorted_records[i].recorded_at)
        
        return MovingAverageData(
            period=period,
            values=ema_values,
            timestamps=ema_timestamps
        )
    
    def calculate_long_term_trends(self, records: List[PriceRecord], 
                                 ma_periods: List[int] = None,
                                 include_ema: bool = False) -> Dict[str, MovingAverageData]:
        """
        Calculate long-term trend indicators including multiple moving averages.
        
        Args:
            records: List of price records (should be sorted by timestamp)
            ma_periods: List of periods for moving averages (default: [7, 14, 30])
            include_ema: Whether to include exponential moving averages
            
        Returns:
            Dictionary of moving average data keyed by type and period
        """
        if not records:
            raise ValueError("No records provided for long-term trend calculation")
        
        if ma_periods is None:
            ma_periods = [7, 14, 30]  # Default: weekly, bi-weekly, monthly
        
        # Ensure records are sorted by timestamp
        sorted_records = sorted(records, key=lambda r: r.recorded_at)
        
        trend_data = {}
        
        # Calculate simple moving averages
        for period in ma_periods:
            if len(sorted_records) >= period:
                ma_data = self.calculate_moving_average(sorted_records, period)
                trend_data[f'SMA_{period}'] = ma_data
        
        # Calculate exponential moving averages if requested
        if include_ema:
            for period in ma_periods:
                if len(sorted_records) >= period:
                    ema_data = self.calculate_exponential_moving_average(sorted_records, period)
                    trend_data[f'EMA_{period}'] = ema_data
        
        return trend_data
    
    def get_trend_signals(self, records: List[PriceRecord], 
                         short_period: int = 7, long_period: int = 14) -> Dict[str, Any]:
        """
        Generate trend signals based on moving average crossovers.
        
        Args:
            records: List of price records
            short_period: Period for short-term moving average
            long_period: Period for long-term moving average
            
        Returns:
            Dictionary containing trend signals and analysis
        """
        if not records:
            raise ValueError("No records provided for trend signal calculation")
        
        if len(records) < max(short_period, long_period):
            raise ValueError(f"Not enough records for trend analysis")
        
        # Calculate moving averages
        short_ma = self.calculate_moving_average(records, short_period)
        long_ma = self.calculate_moving_average(records, long_period)
        
        # Find crossover points
        signals = []
        current_signal = None
        
        # Align the moving averages (long MA starts later)
        offset = long_period - short_period
        
        for i in range(len(long_ma.values)):
            short_idx = i + offset
            if short_idx < len(short_ma.values):
                short_val = short_ma.values[short_idx]
                long_val = long_ma.values[i]
                
                # Determine signal
                if short_val > long_val:
                    signal = 'bullish'  # Short MA above long MA
                else:
                    signal = 'bearish'  # Short MA below long MA
                
                # Check for crossover
                if current_signal and current_signal != signal:
                    signals.append({
                        'timestamp': long_ma.timestamps[i],
                        'type': 'crossover',
                        'from': current_signal,
                        'to': signal,
                        'short_ma': short_val,
                        'long_ma': long_val
                    })
                
                current_signal = signal
        
        # Calculate current trend strength
        if len(long_ma.values) > 0 and len(short_ma.values) > 0:
            latest_short = short_ma.values[-1]
            latest_long = long_ma.values[-1]
            trend_strength = abs((latest_short - latest_long) / latest_long) * 100
        else:
            trend_strength = Decimal('0')
        
        return {
            'current_signal': current_signal,
            'trend_strength': trend_strength.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP),
            'crossover_signals': signals,
            'short_ma_current': short_ma.values[-1] if short_ma.values else None,
            'long_ma_current': long_ma.values[-1] if long_ma.values else None,
            'analysis_timestamp': datetime.now()
        }
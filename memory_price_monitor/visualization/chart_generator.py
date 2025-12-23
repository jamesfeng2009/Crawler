"""
Chart generation service for price trend visualization.
"""

import io
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from decimal import Decimal

from memory_price_monitor.analysis.analyzer import TrendData, ComparisonResult
from memory_price_monitor.data.models import PriceRecord


class ChartGenerator:
    """Chart generation service using Matplotlib."""
    
    def __init__(self):
        """Initialize chart generator with default settings."""
        # Set default style
        plt.style.use('default')
        self.figure_size = (12, 8)
        self.dpi = 100
        self.colors = {
            'primary': '#2E86AB',
            'secondary': '#A23B72',
            'accent': '#F18F01',
            'success': '#C73E1D',
            'background': '#F5F5F5',
            'text': '#333333'
        }
    
    def generate_weekly_trend_chart(self, data: TrendData) -> bytes:
        """
        Generate weekly trend chart showing current week price movements.
        
        Args:
            data: Trend data containing price points and statistics
            
        Returns:
            Chart image as bytes
        """
        if not data.price_points:
            raise ValueError("No price data provided for chart generation")
        
        # Create figure and axis
        fig, ax = plt.subplots(figsize=self.figure_size, dpi=self.dpi)
        
        # Extract data for plotting
        timestamps = [record.recorded_at for record in data.price_points]
        prices = [float(record.current_price) for record in data.price_points]
        
        # Plot main price line
        ax.plot(timestamps, prices, 
               color=self.colors['primary'], 
               linewidth=2, 
               marker='o', 
               markersize=4,
               label='Price')
        
        # Add average price line
        avg_price = float(data.statistics.mean)
        ax.axhline(y=avg_price, 
                  color=self.colors['secondary'], 
                  linestyle='--', 
                  alpha=0.7,
                  label=f'Average: ${avg_price:.2f}')
        
        # Add price range indicators
        min_price = float(data.statistics.min_price)
        max_price = float(data.statistics.max_price)
        
        ax.axhline(y=min_price, 
                  color=self.colors['success'], 
                  linestyle=':', 
                  alpha=0.5,
                  label=f'Min: ${min_price:.2f}')
        
        ax.axhline(y=max_price, 
                  color=self.colors['accent'], 
                  linestyle=':', 
                  alpha=0.5,
                  label=f'Max: ${max_price:.2f}')
        
        # Format chart
        ax.set_title(f'Weekly Price Trend - Product {data.product_id}', 
                    fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('Date', fontsize=12)
        ax.set_ylabel('Price ($)', fontsize=12)
        
        # Format x-axis dates
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        
        # Add grid
        ax.grid(True, alpha=0.3)
        
        # Add legend
        ax.legend(loc='upper right')
        
        # Add statistics text box
        stats_text = (
            f'Statistics:\n'
            f'Count: {data.statistics.count}\n'
            f'Std Dev: ${data.statistics.std_dev:.2f}\n'
            f'Range: ${max_price - min_price:.2f}'
        )
        
        ax.text(0.02, 0.98, stats_text,
               transform=ax.transAxes,
               verticalalignment='top',
               bbox=dict(boxstyle='round', facecolor='white', alpha=0.8),
               fontsize=10)
        
        # Tight layout
        plt.tight_layout()
        
        # Save to bytes
        img_buffer = io.BytesIO()
        plt.savefig(img_buffer, format='png', dpi=self.dpi, bbox_inches='tight')
        img_buffer.seek(0)
        chart_bytes = img_buffer.getvalue()
        
        # Clean up
        plt.close(fig)
        
        return chart_bytes
    
    def generate_comparison_chart(self, comparison: ComparisonResult) -> bytes:
        """
        Generate comparison chart between two weeks.
        
        Args:
            comparison: Comparison result with current and previous week stats
            
        Returns:
            Chart image as bytes
        """
        # Create figure with subplots
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8), dpi=self.dpi)
        
        # Data for comparison
        weeks = ['Previous Week', 'Current Week']
        means = [float(comparison.previous_week_stats.mean), 
                float(comparison.current_week_stats.mean)]
        mins = [float(comparison.previous_week_stats.min_price), 
               float(comparison.current_week_stats.min_price)]
        maxs = [float(comparison.previous_week_stats.max_price), 
               float(comparison.current_week_stats.max_price)]
        
        # Bar chart comparison
        x_pos = range(len(weeks))
        width = 0.25
        
        ax1.bar([x - width for x in x_pos], means, width, 
               label='Average', color=self.colors['primary'], alpha=0.8)
        ax1.bar(x_pos, mins, width, 
               label='Minimum', color=self.colors['success'], alpha=0.8)
        ax1.bar([x + width for x in x_pos], maxs, width, 
               label='Maximum', color=self.colors['accent'], alpha=0.8)
        
        ax1.set_title('Week-over-Week Price Comparison', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Week')
        ax1.set_ylabel('Price ($)')
        ax1.set_xticks(x_pos)
        ax1.set_xticklabels(weeks)
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # Percentage change visualization
        change_pct = float(comparison.percentage_change) * 100
        change_color = self.colors['accent'] if change_pct > 0 else self.colors['success']
        
        ax2.bar(['Price Change'], [abs(change_pct)], 
               color=change_color, alpha=0.8)
        ax2.set_title(f'Price Change: {change_pct:+.1f}%', fontsize=14, fontweight='bold')
        ax2.set_ylabel('Change (%)')
        ax2.grid(True, alpha=0.3)
        
        # Add change direction indicator
        direction_text = f"Trend: {comparison.trend_direction.upper()}"
        ax2.text(0.5, 0.95, direction_text,
                transform=ax2.transAxes,
                horizontalalignment='center',
                verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8),
                fontsize=12, fontweight='bold')
        
        # Add statistics
        stats_text = (
            f'Current Week:\n'
            f'Mean: ${comparison.current_week_stats.mean:.2f}\n'
            f'Count: {comparison.current_week_stats.count}\n'
            f'Std Dev: ${comparison.current_week_stats.std_dev:.2f}\n\n'
            f'Previous Week:\n'
            f'Mean: ${comparison.previous_week_stats.mean:.2f}\n'
            f'Count: {comparison.previous_week_stats.count}\n'
            f'Std Dev: ${comparison.previous_week_stats.std_dev:.2f}'
        )
        
        fig.text(0.02, 0.98, stats_text,
                transform=fig.transFigure,
                verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8),
                fontsize=10)
        
        plt.tight_layout()
        
        # Save to bytes
        img_buffer = io.BytesIO()
        plt.savefig(img_buffer, format='png', dpi=self.dpi, bbox_inches='tight')
        img_buffer.seek(0)
        chart_bytes = img_buffer.getvalue()
        
        # Clean up
        plt.close(fig)
        
        return chart_bytes
    
    def generate_overall_trend_chart(self, historical_data: List[PriceRecord]) -> bytes:
        """
        Generate overall trend chart for historical data.
        
        Args:
            historical_data: List of historical price records
            
        Returns:
            Chart image as bytes
        """
        if not historical_data:
            raise ValueError("No historical data provided for chart generation")
        
        # Sort data by timestamp
        sorted_data = sorted(historical_data, key=lambda x: x.recorded_at)
        
        # Create figure
        fig, ax = plt.subplots(figsize=self.figure_size, dpi=self.dpi)
        
        # Extract data
        timestamps = [record.recorded_at for record in sorted_data]
        prices = [float(record.current_price) for record in sorted_data]
        
        # Plot price trend
        ax.plot(timestamps, prices, 
               color=self.colors['primary'], 
               linewidth=1.5, 
               alpha=0.8,
               label='Price History')
        
        # Add moving average if enough data points
        if len(prices) >= 7:
            # Calculate 7-day moving average
            ma_prices = []
            ma_timestamps = []
            
            for i in range(6, len(prices)):
                ma_price = sum(prices[i-6:i+1]) / 7
                ma_prices.append(ma_price)
                ma_timestamps.append(timestamps[i])
            
            ax.plot(ma_timestamps, ma_prices,
                   color=self.colors['secondary'],
                   linewidth=2,
                   alpha=0.9,
                   label='7-Day Moving Average')
        
        # Format chart
        ax.set_title('Historical Price Trend', fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('Date', fontsize=12)
        ax.set_ylabel('Price ($)', fontsize=12)
        
        # Format x-axis
        if len(timestamps) > 30:
            ax.xaxis.set_major_locator(mdates.WeekdayLocator())
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        else:
            ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        
        # Add grid and legend
        ax.grid(True, alpha=0.3)
        ax.legend()
        
        # Add summary statistics
        min_price = min(prices)
        max_price = max(prices)
        avg_price = sum(prices) / len(prices)
        
        stats_text = (
            f'Summary:\n'
            f'Data Points: {len(prices)}\n'
            f'Average: ${avg_price:.2f}\n'
            f'Range: ${min_price:.2f} - ${max_price:.2f}\n'
            f'Volatility: ${max_price - min_price:.2f}'
        )
        
        ax.text(0.02, 0.98, stats_text,
               transform=ax.transAxes,
               verticalalignment='top',
               bbox=dict(boxstyle='round', facecolor='white', alpha=0.8),
               fontsize=10)
        
        plt.tight_layout()
        
        # Save to bytes
        img_buffer = io.BytesIO()
        plt.savefig(img_buffer, format='png', dpi=self.dpi, bbox_inches='tight')
        img_buffer.seek(0)
        chart_bytes = img_buffer.getvalue()
        
        # Clean up
        plt.close(fig)
        
        return chart_bytes
    
    def generate_brand_comparison_chart(self, brand_data: Dict[str, List[PriceRecord]]) -> bytes:
        """
        Generate brand comparison chart.
        
        Args:
            brand_data: Dictionary mapping brand names to their price records
            
        Returns:
            Chart image as bytes
        """
        if not brand_data:
            raise ValueError("No brand data provided for chart generation")
        
        # Create figure
        fig, ax = plt.subplots(figsize=self.figure_size, dpi=self.dpi)
        
        # Prepare data for comparison
        brands = list(brand_data.keys())
        avg_prices = []
        min_prices = []
        max_prices = []
        
        for brand, records in brand_data.items():
            if records:
                prices = [float(record.current_price) for record in records]
                avg_prices.append(sum(prices) / len(prices))
                min_prices.append(min(prices))
                max_prices.append(max(prices))
            else:
                avg_prices.append(0)
                min_prices.append(0)
                max_prices.append(0)
        
        # Create grouped bar chart
        x_pos = range(len(brands))
        width = 0.25
        
        ax.bar([x - width for x in x_pos], avg_prices, width, 
               label='Average Price', color=self.colors['primary'], alpha=0.8)
        ax.bar(x_pos, min_prices, width, 
               label='Minimum Price', color=self.colors['success'], alpha=0.8)
        ax.bar([x + width for x in x_pos], max_prices, width, 
               label='Maximum Price', color=self.colors['accent'], alpha=0.8)
        
        # Format chart
        ax.set_title('Brand Price Comparison', fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('Brand', fontsize=12)
        ax.set_ylabel('Price ($)', fontsize=12)
        ax.set_xticks(x_pos)
        ax.set_xticklabels(brands, rotation=45, ha='right')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        # Add value labels on bars
        for i, (avg, min_p, max_p) in enumerate(zip(avg_prices, min_prices, max_prices)):
            if avg > 0:
                ax.text(i - width, avg + max(avg_prices) * 0.01, f'${avg:.0f}', 
                       ha='center', va='bottom', fontsize=9)
            if min_p > 0:
                ax.text(i, min_p + max(avg_prices) * 0.01, f'${min_p:.0f}', 
                       ha='center', va='bottom', fontsize=9)
            if max_p > 0:
                ax.text(i + width, max_p + max(avg_prices) * 0.01, f'${max_p:.0f}', 
                       ha='center', va='bottom', fontsize=9)
        
        plt.tight_layout()
        
        # Save to bytes
        img_buffer = io.BytesIO()
        plt.savefig(img_buffer, format='png', dpi=self.dpi, bbox_inches='tight')
        img_buffer.seek(0)
        chart_bytes = img_buffer.getvalue()
        
        # Clean up
        plt.close(fig)
        
        return chart_bytes
    
    def validate_chart_content(self, chart_bytes: bytes) -> Dict[str, Any]:
        """
        Validate that chart contains required content elements.
        
        Args:
            chart_bytes: Chart image as bytes
            
        Returns:
            Dictionary with validation results
        """
        # Basic validation - check if bytes represent a valid PNG
        if not chart_bytes:
            return {"valid": False, "error": "Empty chart data"}
        
        # Check PNG header
        png_header = b'\x89PNG\r\n\x1a\n'
        if not chart_bytes.startswith(png_header):
            return {"valid": False, "error": "Invalid PNG format"}
        
        # Check minimum size (should be reasonable for a chart)
        if len(chart_bytes) < 1000:  # Less than 1KB is suspicious
            return {"valid": False, "error": "Chart data too small"}
        
        return {
            "valid": True,
            "size_bytes": len(chart_bytes),
            "format": "PNG",
            "has_content": True
        }
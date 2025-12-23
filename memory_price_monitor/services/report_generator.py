"""
Weekly report generation service.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from datetime import datetime, date, timedelta
from decimal import Decimal
import logging
from collections import defaultdict

from memory_price_monitor.data.models import WeeklyReport, ComparisonResult, TrendData, PriceRecord, StandardizedProduct
from memory_price_monitor.data.repository import PriceRepository
from memory_price_monitor.analysis.analyzer import PriceAnalyzer
from memory_price_monitor.visualization.chart_generator import ChartGenerator
from memory_price_monitor.utils.errors import ValidationError


logger = logging.getLogger(__name__)


@dataclass
class GroupedData:
    """Data grouped by brand and specifications."""
    brand: str
    capacity: str
    type: str
    products: List[StandardizedProduct] = field(default_factory=list)
    price_records: List[PriceRecord] = field(default_factory=list)
    
    def get_group_key(self) -> str:
        """Get unique key for this group."""
        return f"{self.brand}_{self.capacity}_{self.type}"


class WeeklyReportGenerator:
    """Service for generating weekly price reports."""
    
    def __init__(self, repository: PriceRepository, analyzer: PriceAnalyzer, chart_generator: ChartGenerator):
        """
        Initialize report generator.
        
        Args:
            repository: Price data repository
            analyzer: Price analysis engine
            chart_generator: Chart generation service
        """
        self.repository = repository
        self.analyzer = analyzer
        self.chart_generator = chart_generator
    
    def generate_weekly_report(self, report_date: date = None) -> WeeklyReport:
        """
        Generate comprehensive weekly report.
        
        Args:
            report_date: Date for the report (defaults to current date)
            
        Returns:
            Complete weekly report with analysis and charts
        """
        if report_date is None:
            report_date = date.today()
        
        logger.info(f"Generating weekly report for {report_date}")
        
        # Calculate week boundaries
        week_start, week_end = self._get_week_boundaries(report_date)
        previous_week_start, previous_week_end = self._get_week_boundaries(report_date - timedelta(days=7))
        
        # Get data for both weeks
        current_week_data = self._get_week_data(week_start, week_end)
        previous_week_data = self._get_week_data(previous_week_start, previous_week_end)
        
        # Group data by brand and specifications
        current_grouped = self._group_data_by_specs(current_week_data)
        previous_grouped = self._group_data_by_specs(previous_week_data)
        
        # Generate comparison analysis
        comparison = self._generate_comparison(current_grouped, previous_grouped, week_start, previous_week_start)
        
        # Generate charts
        charts = self._generate_charts(current_grouped, previous_grouped, comparison)
        
        # Calculate summary statistics
        summary_stats = self._calculate_summary_stats(current_grouped, previous_grouped, comparison)
        
        return WeeklyReport(
            report_date=report_date,
            week_start=week_start,
            week_end=week_end,
            comparison=comparison,
            charts=charts,
            summary_stats=summary_stats,
            metadata={
                'generation_timestamp': datetime.now(),
                'current_week_products': len(current_week_data),
                'previous_week_products': len(previous_week_data),
                'groups_analyzed': len(current_grouped)
            }
        )
    
    def _get_week_boundaries(self, reference_date: date) -> tuple[date, date]:
        """
        Get start and end dates for the week containing the reference date.
        
        Args:
            reference_date: Reference date within the week
            
        Returns:
            Tuple of (week_start, week_end) dates
        """
        # Find Monday of the week (weekday() returns 0 for Monday)
        days_since_monday = reference_date.weekday()
        week_start = reference_date - timedelta(days=days_since_monday)
        week_end = week_start + timedelta(days=6)  # Sunday
        
        return week_start, week_end
    
    def _get_week_data(self, start_date: date, end_date: date) -> List[PriceRecord]:
        """
        Get all price records for a specific week.
        
        Args:
            start_date: Week start date
            end_date: Week end date
            
        Returns:
            List of price records for the week
        """
        try:
            # Get all products to query their price history
            all_products = self.repository.get_products_by_specs()
            
            week_data = []
            for product in all_products:
                # Get price history for this product during the week
                price_history = self.repository.get_price_history(
                    product.product_id, 
                    start_date, 
                    end_date,
                    product.source
                )
                week_data.extend(price_history)
            
            logger.debug(f"Retrieved {len(week_data)} price records for week {start_date} to {end_date}")
            return week_data
            
        except Exception as e:
            logger.error(f"Failed to get week data: {e}")
            raise ValidationError(
                "Failed to retrieve week data",
                {"start_date": start_date, "end_date": end_date, "error": str(e)}
            )
    
    def _group_data_by_specs(self, price_records: List[PriceRecord]) -> Dict[str, GroupedData]:
        """
        Group price records by brand and specifications.
        
        Args:
            price_records: List of price records to group
            
        Returns:
            Dictionary mapping group keys to grouped data
        """
        groups = defaultdict(lambda: GroupedData("", "", ""))
        
        # Get product information for each price record
        for record in price_records:
            try:
                # Find the product for this price record
                product = self._get_product_for_record(record)
                if not product:
                    continue
                
                # Create group key
                group_key = f"{product.brand}_{product.capacity}_{product.type}"
                
                # Initialize or update group
                if group_key not in groups:
                    groups[group_key] = GroupedData(
                        brand=product.brand,
                        capacity=product.capacity,
                        type=product.type
                    )
                
                # Add product and record to group
                group = groups[group_key]
                if product not in group.products:
                    group.products.append(product)
                group.price_records.append(record)
                
            except Exception as e:
                logger.warning(f"Failed to group record {record.id}: {e}")
                continue
        
        logger.debug(f"Grouped data into {len(groups)} specification groups")
        return dict(groups)
    
    def _get_product_for_record(self, record: PriceRecord) -> Optional[StandardizedProduct]:
        """
        Get product information for a price record.
        
        Args:
            record: Price record
            
        Returns:
            StandardizedProduct if found, None otherwise
        """
        try:
            # Query database to get product info by record's product_id
            # This is a simplified approach - in practice, you might want to cache this
            query = """
            SELECT source, product_id, brand, model, capacity, frequency, type, url
            FROM products
            WHERE id = %s
            """
            
            result = self.repository.db_manager.execute_query(query, (record.product_id,))
            
            if not result:
                return None
            
            row = result[0]
            return StandardizedProduct(
                source=row[0],
                product_id=row[1],
                brand=row[2],
                model=row[3],
                capacity=row[4],
                frequency=row[5] or "",
                type=row[6],
                current_price=record.current_price,
                original_price=record.original_price or record.current_price,
                url=row[7],
                timestamp=record.recorded_at,
                metadata=record.metadata
            )
            
        except Exception as e:
            logger.warning(f"Failed to get product for record {record.id}: {e}")
            return None
    
    def _generate_comparison(
        self, 
        current_grouped: Dict[str, GroupedData],
        previous_grouped: Dict[str, GroupedData],
        current_week_start: date,
        previous_week_start: date
    ) -> ComparisonResult:
        """
        Generate week-over-week comparison analysis.
        
        Args:
            current_grouped: Current week grouped data
            previous_grouped: Previous week grouped data
            current_week_start: Current week start date
            previous_week_start: Previous week start date
            
        Returns:
            Comparison result with trend analysis
        """
        trends = []
        
        # Analyze each group that exists in both weeks
        for group_key, current_group in current_grouped.items():
            if group_key in previous_grouped:
                previous_group = previous_grouped[group_key]
                
                try:
                    # Compare the two weeks for this group
                    comparison = self.analyzer.compare_weeks(
                        current_group.price_records,
                        previous_group.price_records
                    )
                    
                    # Create trend data
                    trend = TrendData(
                        product_id=group_key,
                        brand=current_group.brand,
                        model=f"{current_group.capacity} {current_group.type}",
                        capacity=current_group.capacity,
                        type=current_group.type,
                        current_week_prices=[r.current_price for r in current_group.price_records],
                        previous_week_prices=[r.current_price for r in previous_group.price_records],
                        average_current=comparison.current_week_stats.mean,
                        average_previous=comparison.previous_week_stats.mean,
                        percentage_change=comparison.percentage_change,
                        trend_direction=comparison.trend_direction
                    )
                    
                    trends.append(trend)
                    
                except Exception as e:
                    logger.warning(f"Failed to analyze group {group_key}: {e}")
                    continue
        
        # Calculate overall statistics
        if trends:
            overall_changes = [trend.percentage_change for trend in trends]
            overall_average_change = sum(overall_changes) / len(overall_changes)
            
            # Identify significant changes (>10% change)
            significant_threshold = Decimal('0.10')
            significant_changes = [
                trend for trend in trends 
                if abs(trend.percentage_change) >= significant_threshold
            ]
        else:
            overall_average_change = Decimal('0')
            significant_changes = []
        
        return ComparisonResult(
            current_week_start=current_week_start,
            previous_week_start=previous_week_start,
            trends=trends,
            overall_average_change=overall_average_change,
            significant_changes=significant_changes
        )
    
    def _generate_charts(
        self,
        current_grouped: Dict[str, GroupedData],
        previous_grouped: Dict[str, GroupedData],
        comparison: ComparisonResult
    ) -> Dict[str, bytes]:
        """
        Generate charts for the weekly report.
        
        Args:
            current_grouped: Current week grouped data
            previous_grouped: Previous week grouped data
            comparison: Comparison analysis results
            
        Returns:
            Dictionary mapping chart names to chart image bytes
        """
        charts = {}
        
        try:
            # Generate overall trend comparison chart
            if comparison.trends:
                charts['overall_comparison'] = self._generate_overall_comparison_chart(comparison)
            
            # Generate brand comparison chart
            brand_data = self._prepare_brand_data(current_grouped)
            if brand_data:
                charts['brand_comparison'] = self.chart_generator.generate_brand_comparison_chart(brand_data)
            
            # Generate capacity comparison chart
            capacity_data = self._prepare_capacity_data(current_grouped)
            if capacity_data:
                charts['capacity_comparison'] = self._generate_capacity_comparison_chart(capacity_data)
            
            # Generate significant changes chart if any
            if comparison.significant_changes:
                charts['significant_changes'] = self._generate_significant_changes_chart(comparison.significant_changes)
            
            logger.info(f"Generated {len(charts)} charts for weekly report")
            
        except Exception as e:
            logger.error(f"Failed to generate charts: {e}")
            # Return empty charts dict rather than failing the entire report
            charts = {}
        
        return charts
    
    def _generate_overall_comparison_chart(self, comparison: ComparisonResult) -> bytes:
        """Generate overall week-over-week comparison chart."""
        # Create a mock comparison result for chart generation
        # This is a simplified version - you might want to aggregate the data differently
        if not comparison.trends:
            raise ValueError("No trends available for chart generation")
        
        # Use the first trend's comparison data as representative
        # In a real implementation, you might want to aggregate all trends
        first_trend = comparison.trends[0]
        
        from memory_price_monitor.analysis.analyzer import ComparisonResult as AnalyzerComparison
        
        chart_comparison = AnalyzerComparison(
            current_week_stats=self.analyzer.calculate_statistics(
                [PriceRecord(current_price=price) for price in first_trend.current_week_prices]
            ),
            previous_week_stats=self.analyzer.calculate_statistics(
                [PriceRecord(current_price=price) for price in first_trend.previous_week_prices]
            ),
            average_price_difference=first_trend.average_current - first_trend.average_previous,
            percentage_change=first_trend.percentage_change,
            trend_direction=first_trend.trend_direction
        )
        
        return self.chart_generator.generate_comparison_chart(chart_comparison)
    
    def _prepare_brand_data(self, grouped_data: Dict[str, GroupedData]) -> Dict[str, List[PriceRecord]]:
        """Prepare brand data for chart generation."""
        brand_data = defaultdict(list)
        
        for group in grouped_data.values():
            brand_data[group.brand].extend(group.price_records)
        
        return dict(brand_data)
    
    def _prepare_capacity_data(self, grouped_data: Dict[str, GroupedData]) -> Dict[str, List[PriceRecord]]:
        """Prepare capacity data for chart generation."""
        capacity_data = defaultdict(list)
        
        for group in grouped_data.values():
            capacity_data[group.capacity].extend(group.price_records)
        
        return dict(capacity_data)
    
    def _generate_capacity_comparison_chart(self, capacity_data: Dict[str, List[PriceRecord]]) -> bytes:
        """Generate capacity comparison chart."""
        # Use the brand comparison chart generator with capacity data
        return self.chart_generator.generate_brand_comparison_chart(capacity_data)
    
    def _generate_significant_changes_chart(self, significant_changes: List[TrendData]) -> bytes:
        """Generate chart showing significant price changes."""
        # Create a simple bar chart showing percentage changes
        import matplotlib.pyplot as plt
        import io
        
        if not significant_changes:
            raise ValueError("No significant changes to chart")
        
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Prepare data
        labels = [f"{trend.brand}\n{trend.capacity}" for trend in significant_changes]
        changes = [float(trend.percentage_change) * 100 for trend in significant_changes]
        colors = ['red' if change < 0 else 'green' for change in changes]
        
        # Create bar chart
        bars = ax.bar(labels, changes, color=colors, alpha=0.7)
        
        # Format chart
        ax.set_title('Significant Price Changes (>10%)', fontsize=16, fontweight='bold')
        ax.set_ylabel('Price Change (%)')
        ax.axhline(y=0, color='black', linestyle='-', alpha=0.3)
        ax.grid(True, alpha=0.3)
        
        # Add value labels on bars
        for bar, change in zip(bars, changes):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height + (1 if height > 0 else -3),
                   f'{change:+.1f}%', ha='center', va='bottom' if height > 0 else 'top')
        
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        
        # Save to bytes
        img_buffer = io.BytesIO()
        plt.savefig(img_buffer, format='png', dpi=100, bbox_inches='tight')
        img_buffer.seek(0)
        chart_bytes = img_buffer.getvalue()
        
        plt.close(fig)
        return chart_bytes
    
    def _calculate_summary_stats(
        self,
        current_grouped: Dict[str, GroupedData],
        previous_grouped: Dict[str, GroupedData],
        comparison: ComparisonResult
    ) -> Dict[str, Any]:
        """
        Calculate summary statistics for the report.
        
        Args:
            current_grouped: Current week grouped data
            previous_grouped: Previous week grouped data
            comparison: Comparison analysis results
            
        Returns:
            Dictionary with summary statistics
        """
        # Count statistics
        total_current_records = sum(len(group.price_records) for group in current_grouped.values())
        total_previous_records = sum(len(group.price_records) for group in previous_grouped.values())
        
        # Price statistics
        all_current_prices = []
        for group in current_grouped.values():
            all_current_prices.extend([r.current_price for r in group.price_records])
        
        if all_current_prices:
            avg_current_price = sum(all_current_prices) / len(all_current_prices)
            min_current_price = min(all_current_prices)
            max_current_price = max(all_current_prices)
        else:
            avg_current_price = min_current_price = max_current_price = Decimal('0')
        
        # Trend statistics
        up_trends = len([t for t in comparison.trends if t.trend_direction == 'up'])
        down_trends = len([t for t in comparison.trends if t.trend_direction == 'down'])
        stable_trends = len([t for t in comparison.trends if t.trend_direction == 'stable'])
        
        return {
            'total_groups_analyzed': len(comparison.trends),
            'current_week_records': total_current_records,
            'previous_week_records': total_previous_records,
            'average_current_price': float(avg_current_price),
            'min_current_price': float(min_current_price),
            'max_current_price': float(max_current_price),
            'overall_average_change_percent': float(comparison.overall_average_change * 100),
            'significant_changes_count': len(comparison.significant_changes),
            'trends_up': up_trends,
            'trends_down': down_trends,
            'trends_stable': stable_trends,
            'unique_brands': len(set(group.brand for group in current_grouped.values())),
            'unique_capacities': len(set(group.capacity for group in current_grouped.values())),
            'unique_types': len(set(group.type for group in current_grouped.values()))
        }
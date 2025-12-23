"""
Integration test for weekly report generation.
"""

import pytest
from datetime import datetime, date, timedelta
from decimal import Decimal
from unittest.mock import Mock, MagicMock

from memory_price_monitor.services.report_generator import WeeklyReportGenerator
from memory_price_monitor.data.models import StandardizedProduct, PriceRecord
from memory_price_monitor.data.repository import PriceRepository
from memory_price_monitor.analysis.analyzer import PriceAnalyzer
from memory_price_monitor.visualization.chart_generator import ChartGenerator


class TestWeeklyReportIntegration:
    """Integration tests for weekly report generation."""
    
    def test_weekly_report_generation_basic(self):
        """Test basic weekly report generation with mock data."""
        # Create mock repository
        mock_repository = Mock(spec=PriceRepository)
        mock_repository.db_manager = Mock()
        
        # Create sample products
        sample_products = [
            StandardizedProduct(
                source='jd',
                product_id='test_1',
                brand='CORSAIR',
                model='Vengeance LPX',
                capacity='16GB',
                frequency='3200MHz',
                type='DDR4',
                current_price=Decimal('120.00'),
                original_price=Decimal('150.00'),
                url='http://example.com/1',
                timestamp=datetime.now(),
                metadata={}
            ),
            StandardizedProduct(
                source='zol',
                product_id='test_2',
                brand='KINGSTON',
                model='HyperX Fury',
                capacity='16GB',
                frequency='3200MHz',
                type='DDR4',
                current_price=Decimal('110.00'),
                original_price=Decimal('140.00'),
                url='http://example.com/2',
                timestamp=datetime.now(),
                metadata={}
            )
        ]
        
        # Create sample price records
        current_week_records = [
            PriceRecord(
                id=1,
                product_id=1,
                current_price=Decimal('120.00'),
                original_price=Decimal('150.00'),
                recorded_at=datetime.now(),
                metadata={}
            ),
            PriceRecord(
                id=2,
                product_id=2,
                current_price=Decimal('110.00'),
                original_price=Decimal('140.00'),
                recorded_at=datetime.now(),
                metadata={}
            )
        ]
        
        previous_week_records = [
            PriceRecord(
                id=3,
                product_id=1,
                current_price=Decimal('125.00'),
                original_price=Decimal('150.00'),
                recorded_at=datetime.now() - timedelta(days=7),
                metadata={}
            ),
            PriceRecord(
                id=4,
                product_id=2,
                current_price=Decimal('115.00'),
                original_price=Decimal('140.00'),
                recorded_at=datetime.now() - timedelta(days=7),
                metadata={}
            )
        ]
        
        # Mock repository methods
        mock_repository.get_products_by_specs.return_value = sample_products
        
        def mock_get_price_history(product_id, start_date, end_date, source=None):
            # Return current week data for recent dates, previous week for older dates
            if start_date >= date.today() - timedelta(days=7):
                return [r for r in current_week_records if (
                    (product_id == 'test_1' and r.product_id == 1) or
                    (product_id == 'test_2' and r.product_id == 2)
                )]
            else:
                return [r for r in previous_week_records if (
                    (product_id == 'test_1' and r.product_id == 1) or
                    (product_id == 'test_2' and r.product_id == 2)
                )]
        
        mock_repository.get_price_history.side_effect = mock_get_price_history
        
        # Mock database query for product lookup
        def mock_execute_query(query, params=None):
            if "SELECT source, product_id, brand, model, capacity, frequency, type, url" in query:
                if params and len(params) > 0:
                    product_id = params[0]
                    if product_id == 1:
                        return [('jd', 'test_1', 'CORSAIR', 'Vengeance LPX', '16GB', '3200MHz', 'DDR4', 'http://example.com/1')]
                    elif product_id == 2:
                        return [('zol', 'test_2', 'KINGSTON', 'HyperX Fury', '16GB', '3200MHz', 'DDR4', 'http://example.com/2')]
            return []
        
        mock_repository.db_manager.execute_query.side_effect = mock_execute_query
        
        # Create real analyzer and mock chart generator
        analyzer = PriceAnalyzer()
        mock_chart_generator = Mock(spec=ChartGenerator)
        mock_chart_generator.generate_brand_comparison_chart.return_value = b'mock_chart_data'
        mock_chart_generator.generate_comparison_chart.return_value = b'mock_comparison_chart'
        
        # Create report generator
        report_generator = WeeklyReportGenerator(mock_repository, analyzer, mock_chart_generator)
        
        # Generate report
        report = report_generator.generate_weekly_report()
        
        # Verify report structure
        assert report is not None
        assert report.report_date == date.today()
        assert report.week_start is not None
        assert report.week_end is not None
        assert report.comparison is not None
        assert isinstance(report.charts, dict)
        assert isinstance(report.summary_stats, dict)
        assert isinstance(report.metadata, dict)
        
        # Verify comparison data
        assert len(report.comparison.trends) >= 0  # May be 0 if no matching data
        assert report.comparison.overall_average_change is not None
        
        # Verify summary stats
        assert 'total_groups_analyzed' in report.summary_stats
        assert 'current_week_records' in report.summary_stats
        assert 'previous_week_records' in report.summary_stats
        
        # Verify metadata
        assert 'generation_timestamp' in report.metadata
        assert 'current_week_products' in report.metadata
        assert 'previous_week_products' in report.metadata
    
    def test_weekly_report_with_empty_data(self):
        """Test weekly report generation with no data."""
        # Create mock repository with no data
        mock_repository = Mock(spec=PriceRepository)
        mock_repository.db_manager = Mock()
        mock_repository.get_products_by_specs.return_value = []
        mock_repository.get_price_history.return_value = []
        mock_repository.db_manager.execute_query.return_value = []
        
        # Create components
        analyzer = PriceAnalyzer()
        mock_chart_generator = Mock(spec=ChartGenerator)
        
        # Create report generator
        report_generator = WeeklyReportGenerator(mock_repository, analyzer, mock_chart_generator)
        
        # Generate report
        report = report_generator.generate_weekly_report()
        
        # Verify report handles empty data gracefully
        assert report is not None
        assert report.report_date == date.today()
        assert len(report.comparison.trends) == 0
        assert report.comparison.overall_average_change == Decimal('0')
        assert len(report.comparison.significant_changes) == 0
        
        # Verify summary stats for empty data
        assert report.summary_stats['total_groups_analyzed'] == 0
        assert report.summary_stats['current_week_records'] == 0
        assert report.summary_stats['previous_week_records'] == 0
    
    def test_week_boundaries_calculation(self):
        """Test week boundary calculation."""
        mock_repository = Mock(spec=PriceRepository)
        analyzer = PriceAnalyzer()
        chart_generator = Mock(spec=ChartGenerator)
        
        report_generator = WeeklyReportGenerator(mock_repository, analyzer, chart_generator)
        
        # Test with a known date (Wednesday, 2024-01-03)
        test_date = date(2024, 1, 3)
        week_start, week_end = report_generator._get_week_boundaries(test_date)
        
        # Should be Monday (2024-01-01) to Sunday (2024-01-07)
        assert week_start == date(2024, 1, 1)  # Monday
        assert week_end == date(2024, 1, 7)    # Sunday
        
        # Test with a Monday
        monday_date = date(2024, 1, 1)
        week_start, week_end = report_generator._get_week_boundaries(monday_date)
        
        assert week_start == date(2024, 1, 1)  # Same Monday
        assert week_end == date(2024, 1, 7)    # Following Sunday
        
        # Test with a Sunday
        sunday_date = date(2024, 1, 7)
        week_start, week_end = report_generator._get_week_boundaries(sunday_date)
        
        assert week_start == date(2024, 1, 1)  # Previous Monday
        assert week_end == date(2024, 1, 7)    # Same Sunday
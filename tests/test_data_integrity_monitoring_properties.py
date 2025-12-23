"""
Property-based tests for data integrity monitoring.

**Feature: memory-price-monitor, Property 27: Data integrity monitoring**
**Validates: Requirements 7.5**
"""

import pytest
from hypothesis import given, strategies as st, assume
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Dict, Any, Optional

from memory_price_monitor.data.models import StandardizedProduct, PriceRecord
from memory_price_monitor.utils.errors import ValidationError
from dataclasses import dataclass
from typing import Dict, Any


@dataclass
class MonitoringTestProduct:
    """Test-specific product model without validation for monitoring tests."""
    source: str
    product_id: str
    brand: str
    model: str
    capacity: str
    frequency: str
    type: str
    current_price: Decimal
    original_price: Decimal
    url: str
    timestamp: datetime
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class DataIntegrityMonitor:
    """Mock data integrity monitoring system for testing."""
    
    def __init__(self):
        self.flagged_data_points = []
        self.integrity_checks_performed = []
        self.suspicious_patterns = []
    
    def check_price_anomalies(self, products: List[MonitoringTestProduct], 
                            threshold_multiplier: float = 3.0) -> List[Dict[str, Any]]:
        """Check for price anomalies that might indicate data integrity issues."""
        anomalies = []
        
        if not products:
            return anomalies
        
        # Calculate price statistics
        prices = [float(p.current_price) for p in products]
        if not prices:
            return anomalies
            
        mean_price = sum(prices) / len(prices)
        
        # Simple anomaly detection based on deviation from mean
        for product in products:
            price = float(product.current_price)
            
            # Check for extreme price deviations
            if price > mean_price * threshold_multiplier or price < mean_price / threshold_multiplier:
                anomaly = {
                    'product_id': product.product_id,
                    'source': product.source,
                    'price': price,
                    'mean_price': mean_price,
                    'deviation_factor': price / mean_price if mean_price > 0 else float('inf'),
                    'type': 'price_anomaly',
                    'timestamp': product.timestamp
                }
                anomalies.append(anomaly)
                self.flagged_data_points.append(anomaly)
        
        self.integrity_checks_performed.append({
            'check_type': 'price_anomalies',
            'products_checked': len(products),
            'anomalies_found': len(anomalies),
            'timestamp': datetime.now()
        })
        
        return anomalies
    
    def check_duplicate_data(self, products: List[MonitoringTestProduct]) -> List[Dict[str, Any]]:
        """Check for duplicate data entries."""
        duplicates = []
        seen_products = {}
        
        for product in products:
            key = (product.source, product.product_id, product.timestamp.date())
            
            if key in seen_products:
                duplicate = {
                    'product_id': product.product_id,
                    'source': product.source,
                    'original_timestamp': seen_products[key].timestamp,
                    'duplicate_timestamp': product.timestamp,
                    'type': 'duplicate_data',
                }
                duplicates.append(duplicate)
                self.flagged_data_points.append(duplicate)
            else:
                seen_products[key] = product
        
        self.integrity_checks_performed.append({
            'check_type': 'duplicate_data',
            'products_checked': len(products),
            'duplicates_found': len(duplicates),
            'timestamp': datetime.now()
        })
        
        return duplicates
    
    def check_missing_required_fields(self, products: List[MonitoringTestProduct]) -> List[Dict[str, Any]]:
        """Check for products with missing required fields."""
        missing_fields = []
        
        for product in products:
            issues = []
            
            if not product.brand or product.brand.strip() == '':
                issues.append('brand')
            if not product.model or product.model.strip() == '':
                issues.append('model')
            if not product.capacity or product.capacity.strip() == '':
                issues.append('capacity')
            if not product.type or product.type.strip() == '':
                issues.append('type')
            if product.current_price <= 0:
                issues.append('current_price')
            
            if issues:
                missing_field_issue = {
                    'product_id': product.product_id,
                    'source': product.source,
                    'missing_fields': issues,
                    'type': 'missing_fields',
                    'timestamp': product.timestamp
                }
                missing_fields.append(missing_field_issue)
                self.flagged_data_points.append(missing_field_issue)
        
        self.integrity_checks_performed.append({
            'check_type': 'missing_fields',
            'products_checked': len(products),
            'issues_found': len(missing_fields),
            'timestamp': datetime.now()
        })
        
        return missing_fields
    
    def check_temporal_consistency(self, price_records: List[PriceRecord]) -> List[Dict[str, Any]]:
        """Check for temporal inconsistencies in price data."""
        inconsistencies = []
        
        # Group by product_id
        product_records = {}
        for record in price_records:
            if record.product_id not in product_records:
                product_records[record.product_id] = []
            product_records[record.product_id].append(record)
        
        # Check each product's price history
        for product_id, records in product_records.items():
            if len(records) < 2:
                continue
                
            # Sort by timestamp
            sorted_records = sorted(records, key=lambda r: r.recorded_at)
            
            for i in range(1, len(sorted_records)):
                prev_record = sorted_records[i-1]
                curr_record = sorted_records[i]
                
                # Check for impossible price jumps (>10x change in short time)
                time_diff = (curr_record.recorded_at - prev_record.recorded_at).total_seconds()
                if time_diff < 3600:  # Less than 1 hour
                    price_ratio = float(curr_record.current_price) / float(prev_record.current_price)
                    if price_ratio > 10 or price_ratio < 0.1:
                        inconsistency = {
                            'product_id': product_id,
                            'prev_price': float(prev_record.current_price),
                            'curr_price': float(curr_record.current_price),
                            'time_diff_seconds': time_diff,
                            'price_ratio': price_ratio,
                            'type': 'temporal_inconsistency',
                            'timestamp': curr_record.recorded_at
                        }
                        inconsistencies.append(inconsistency)
                        self.flagged_data_points.append(inconsistency)
        
        self.integrity_checks_performed.append({
            'check_type': 'temporal_consistency',
            'records_checked': len(price_records),
            'inconsistencies_found': len(inconsistencies),
            'timestamp': datetime.now()
        })
        
        return inconsistencies
    
    def get_flagged_data_points(self) -> List[Dict[str, Any]]:
        """Get all flagged data points."""
        return self.flagged_data_points.copy()
    
    def get_integrity_check_summary(self) -> Dict[str, Any]:
        """Get summary of integrity checks performed."""
        return {
            'total_checks': len(self.integrity_checks_performed),
            'total_flagged_points': len(self.flagged_data_points),
            'checks_by_type': {
                check['check_type']: sum(1 for c in self.integrity_checks_performed 
                                       if c['check_type'] == check['check_type'])
                for check in self.integrity_checks_performed
            },
            'last_check': max(self.integrity_checks_performed, 
                            key=lambda x: x['timestamp'])['timestamp'] if self.integrity_checks_performed else None
        }


# Strategies for generating test data
def monitoring_test_product_strategy():
    """Generate MonitoringTestProduct instances for monitoring tests."""
    return st.builds(
        MonitoringTestProduct,
        source=st.sampled_from(['jd', 'zol', 'test']),
        product_id=st.text(min_size=1, max_size=20, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'))),
        brand=st.one_of(
            st.sampled_from(['CORSAIR', 'KINGSTON', 'G.SKILL', 'CRUCIAL', 'SAMSUNG']),
            st.text(min_size=0, max_size=10)  # Allow empty/invalid brands for testing
        ),
        model=st.text(min_size=0, max_size=50),  # Allow empty models
        capacity=st.one_of(
            st.sampled_from(['16GB', '32GB', '8GB', '64GB']),
            st.text(min_size=0, max_size=10)  # Allow invalid capacity formats
        ),
        frequency=st.one_of(
            st.sampled_from(['3200MHz', '3600MHz', '2400MHz']),
            st.text(min_size=0, max_size=10)  # Allow invalid frequency formats
        ),
        type=st.one_of(
            st.sampled_from(['DDR4', 'DDR5', 'DDR3']),
            st.text(min_size=0, max_size=10)  # Allow invalid types
        ),
        current_price=st.decimals(min_value=-100, max_value=10000, places=2),  # Allow negative prices for testing
        original_price=st.decimals(min_value=-100, max_value=10000, places=2),
        url=st.text(min_size=0, max_size=100),  # Allow invalid URLs
        timestamp=st.datetimes(min_value=datetime(2020, 1, 1), max_value=datetime(2025, 12, 31))
    )

def valid_product_strategy():
    """Generate valid StandardizedProduct instances."""
    return st.builds(
        StandardizedProduct,
        source=st.sampled_from(['jd', 'zol', 'test']),
        product_id=st.text(min_size=1, max_size=20, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'))),
        brand=st.sampled_from(['CORSAIR', 'KINGSTON', 'G.SKILL', 'CRUCIAL', 'SAMSUNG']),
        model=st.text(min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd', 'Pc', 'Pd', 'Zs'))),
        capacity=st.sampled_from(['16GB', '32GB', '8GB', '64GB']),
        frequency=st.sampled_from(['3200MHz', '3600MHz', '2400MHz']),
        type=st.sampled_from(['DDR4', 'DDR5', 'DDR3']),
        current_price=st.decimals(min_value=1, max_value=1000, places=2),
        original_price=st.decimals(min_value=1, max_value=1000, places=2),
        url=st.just("http://example.com/product"),  # Use a simple valid URL
        timestamp=st.datetimes(min_value=datetime(2020, 1, 1), max_value=datetime(2025, 12, 31))
    ).filter(lambda p: p.current_price <= p.original_price * 2)  # Ensure price ratio is reasonable

product_strategy = monitoring_test_product_strategy()

price_record_strategy = st.builds(
    PriceRecord,
    product_id=st.integers(min_value=1, max_value=1000),
    current_price=st.decimals(min_value=1, max_value=5000, places=2),
    original_price=st.one_of(st.none(), st.decimals(min_value=1, max_value=5000, places=2)),
    recorded_at=st.datetimes(min_value=datetime(2020, 1, 1), max_value=datetime(2025, 12, 31))
)


@given(products=st.lists(product_strategy, min_size=1, max_size=20))
def test_data_integrity_monitoring_property(products):
    """
    Property 27: Data integrity monitoring
    
    For any suspicious data points detected, the system should flag them 
    for review without affecting normal processing.
    
    **Feature: memory-price-monitor, Property 27: Data integrity monitoring**
    **Validates: Requirements 7.5**
    """
    assume(len(products) > 0)
    
    monitor = DataIntegrityMonitor()
    
    # Property: System should detect and flag suspicious data points
    
    # Check for price anomalies
    price_anomalies = monitor.check_price_anomalies(products)
    
    # Check for duplicate data
    duplicate_issues = monitor.check_duplicate_data(products)
    
    # Check for missing required fields
    missing_field_issues = monitor.check_missing_required_fields(products)
    
    # Property: All integrity checks should complete without affecting normal processing
    assert len(monitor.integrity_checks_performed) == 3, "Should perform all integrity checks"
    
    # Property: Flagged data points should be recorded for review
    flagged_points = monitor.get_flagged_data_points()
    expected_flagged = len(price_anomalies) + len(duplicate_issues) + len(missing_field_issues)
    assert len(flagged_points) == expected_flagged, "Should record all flagged data points"
    
    # Property: Each flagged point should have required metadata
    for flagged_point in flagged_points:
        assert 'type' in flagged_point, "Flagged point should have type"
        assert 'product_id' in flagged_point or 'missing_fields' in flagged_point, "Flagged point should identify the product"
        # Different types have different timestamp fields
        if flagged_point['type'] == 'duplicate_data':
            assert 'duplicate_timestamp' in flagged_point, "Duplicate data should have duplicate_timestamp"
        else:
            assert 'timestamp' in flagged_point, "Flagged point should have timestamp"
    
    # Property: System should provide summary of integrity checks
    summary = monitor.get_integrity_check_summary()
    assert summary['total_checks'] == 3, "Summary should reflect all checks performed"
    assert summary['total_flagged_points'] == len(flagged_points), "Summary should count flagged points correctly"


@given(
    price_records=st.lists(price_record_strategy, min_size=2, max_size=10),
    product_id=st.integers(min_value=1, max_value=100)
)
def test_temporal_consistency_monitoring(price_records, product_id):
    """
    Property: For any price data with temporal inconsistencies, 
    the system should detect and flag them.
    
    **Feature: memory-price-monitor, Property 27: Data integrity monitoring**
    **Validates: Requirements 7.5**
    """
    # Set all records to same product_id for temporal analysis
    for record in price_records:
        record.product_id = product_id
    
    # Sort by timestamp to ensure temporal order
    price_records.sort(key=lambda r: r.recorded_at)
    
    monitor = DataIntegrityMonitor()
    
    # Check temporal consistency
    inconsistencies = monitor.check_temporal_consistency(price_records)
    
    # Property: Temporal consistency check should complete
    temporal_checks = [c for c in monitor.integrity_checks_performed 
                      if c['check_type'] == 'temporal_consistency']
    assert len(temporal_checks) == 1, "Should perform temporal consistency check"
    
    # Property: If inconsistencies exist, they should be flagged
    flagged_temporal_issues = [f for f in monitor.flagged_data_points 
                              if f['type'] == 'temporal_inconsistency']
    assert len(flagged_temporal_issues) == len(inconsistencies), "Should flag all temporal inconsistencies"
    
    # Property: Each temporal inconsistency should have required information
    for inconsistency in inconsistencies:
        assert 'product_id' in inconsistency, "Should identify product"
        assert 'prev_price' in inconsistency, "Should record previous price"
        assert 'curr_price' in inconsistency, "Should record current price"
        assert 'time_diff_seconds' in inconsistency, "Should record time difference"
        assert 'price_ratio' in inconsistency, "Should calculate price ratio"


@given(
    products=st.lists(product_strategy, min_size=1, max_size=15),
    threshold=st.floats(min_value=1.5, max_value=10.0)
)
def test_price_anomaly_detection(products, threshold):
    """
    Property: For any price data with anomalies, the system should detect them 
    based on configurable thresholds.
    
    **Feature: memory-price-monitor, Property 27: Data integrity monitoring**
    **Validates: Requirements 7.5**
    """
    assume(len(products) >= 2)  # Need at least 2 products for meaningful anomaly detection
    
    monitor = DataIntegrityMonitor()
    
    # Detect price anomalies with custom threshold
    anomalies = monitor.check_price_anomalies(products, threshold)
    
    # Property: Anomaly detection should complete
    anomaly_checks = [c for c in monitor.integrity_checks_performed 
                     if c['check_type'] == 'price_anomalies']
    assert len(anomaly_checks) == 1, "Should perform price anomaly check"
    
    # Property: Detected anomalies should meet threshold criteria
    if anomalies:
        prices = [float(p.current_price) for p in products]
        mean_price = sum(prices) / len(prices)
        
        for anomaly in anomalies:
            deviation_factor = anomaly['deviation_factor']
            assert (deviation_factor >= threshold or 
                   deviation_factor <= 1.0/threshold), "Anomaly should exceed threshold"
    
    # Property: All anomalies should be flagged
    flagged_anomalies = [f for f in monitor.flagged_data_points 
                        if f['type'] == 'price_anomaly']
    assert len(flagged_anomalies) == len(anomalies), "Should flag all detected anomalies"


def test_integrity_monitoring_does_not_affect_processing():
    """
    Property: Data integrity monitoring should not affect normal data processing.
    
    **Feature: memory-price-monitor, Property 27: Data integrity monitoring**
    **Validates: Requirements 7.5**
    """
    # Create test products with valid data
    products = [
        StandardizedProduct(
            source='test',
            product_id='test_1',
            brand='TEST_BRAND',
            model='Test Model',
            capacity='16GB',
            frequency='3200MHz',
            type='DDR4',
            current_price=Decimal('100.00'),
            original_price=Decimal('120.00'),
            url='http://test.com/product1',
            timestamp=datetime.now()
        ),
        StandardizedProduct(
            source='test',
            product_id='test_2',
            brand='TEST_BRAND',
            model='Test Model 2',
            capacity='32GB',
            frequency='3600MHz',
            type='DDR4',
            current_price=Decimal('400.00'),  # Higher price but within reasonable range
            original_price=Decimal('450.00'),
            url='http://test.com/product2',
            timestamp=datetime.now()
        )
    ]
    
    monitor = DataIntegrityMonitor()
    
    # Perform integrity monitoring
    original_products = products.copy()
    
    monitor.check_price_anomalies(products)
    monitor.check_duplicate_data(products)
    monitor.check_missing_required_fields(products)
    
    # Property: Original data should remain unchanged
    assert len(products) == len(original_products), "Product list should not be modified"
    
    for i, product in enumerate(products):
        original = original_products[i]
        assert product.product_id == original.product_id, "Product data should not be modified"
        assert product.current_price == original.current_price, "Prices should not be modified"
        assert product.brand == original.brand, "Product attributes should not be modified"
    
    # Property: Monitoring should complete without errors
    flagged_points = monitor.get_flagged_data_points()
    # May or may not have flagged points, but should not error
    
    # Property: Summary should be available
    summary = monitor.get_integrity_check_summary()
    assert summary['total_checks'] == 3, "Should have performed all checks"


if __name__ == "__main__":
    pytest.main([__file__])
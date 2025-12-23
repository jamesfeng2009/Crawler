"""
Property-based tests for data grouping consistency.

**Feature: memory-price-monitor, Property 13: Data grouping consistency**
**Validates: Requirements 4.3**
"""

import pytest
from hypothesis import given, strategies as st, assume
from decimal import Decimal
from datetime import datetime, date, timedelta
from collections import defaultdict

from memory_price_monitor.services.report_generator import WeeklyReportGenerator, GroupedData
from memory_price_monitor.data.models import PriceRecord, StandardizedProduct
from memory_price_monitor.data.repository import PriceRepository
from memory_price_monitor.analysis.analyzer import PriceAnalyzer
from memory_price_monitor.visualization.chart_generator import ChartGenerator
from memory_price_monitor.data.database import DatabaseManager


# Test data generators
@st.composite
def generate_price_record(draw):
    """Generate a valid PriceRecord."""
    return PriceRecord(
        id=draw(st.integers(min_value=1, max_value=10000)),
        product_id=draw(st.integers(min_value=1, max_value=100)),
        current_price=Decimal(str(draw(st.floats(min_value=10.0, max_value=1000.0)))),
        original_price=Decimal(str(draw(st.floats(min_value=10.0, max_value=1000.0)))),
        recorded_at=draw(st.datetimes(
            min_value=datetime(2023, 1, 1),
            max_value=datetime(2024, 12, 31)
        )),
        metadata=draw(st.dictionaries(
            st.text(min_size=1, max_size=10),
            st.one_of(st.text(), st.integers(), st.floats()),
            min_size=0,
            max_size=3
        ))
    )


@st.composite
def generate_standardized_product(draw):
    """Generate a valid StandardizedProduct."""
    brands = ['CORSAIR', 'KINGSTON', 'G.SKILL', 'CRUCIAL', 'SAMSUNG']
    capacities = ['8GB', '16GB', '32GB', '64GB']
    types = ['DDR4', 'DDR5']
    
    current_price = Decimal(str(draw(st.floats(min_value=50.0, max_value=1000.0))))
    original_price = Decimal(str(draw(st.floats(min_value=float(current_price), max_value=float(current_price) * 1.5))))
    
    # Create product without validation first
    product = StandardizedProduct.__new__(StandardizedProduct)
    product.source = draw(st.sampled_from(['jd', 'zol']))
    product.product_id = draw(st.text(min_size=5, max_size=20, alphabet=st.characters(min_codepoint=48, max_codepoint=122)))
    product.brand = draw(st.sampled_from(brands))
    product.model = draw(st.text(min_size=5, max_size=50, alphabet=st.characters(min_codepoint=48, max_codepoint=122)))
    product.capacity = draw(st.sampled_from(capacities))
    product.frequency = draw(st.sampled_from(['2400MHz', '3200MHz', '3600MHz', '4000MHz']))
    product.type = draw(st.sampled_from(types))
    product.current_price = current_price
    product.original_price = original_price
    product.url = draw(st.text(min_size=10, max_size=100, alphabet=st.characters(min_codepoint=48, max_codepoint=122)))
    product.timestamp = draw(st.datetimes(
        min_value=datetime(2023, 1, 1),
        max_value=datetime(2024, 12, 31)
    ))
    product.metadata = draw(st.dictionaries(
        st.text(min_size=1, max_size=10, alphabet=st.characters(min_codepoint=97, max_codepoint=122)),
        st.one_of(st.text(alphabet=st.characters(min_codepoint=97, max_codepoint=122)), st.integers(), st.floats()),
        min_size=0,
        max_size=3
    ))
    
    return product


class MockDatabaseManager:
    """Mock database manager for testing."""
    
    def __init__(self):
        self.products = {}
        self.next_id = 1
    
    def execute_query(self, query, params=None):
        """Mock query execution."""
        if "SELECT source, product_id, brand, model, capacity, frequency, type, url" in query:
            # Return mock product data based on product_id
            if params and len(params) > 0:
                product_id = params[0]
                if product_id in self.products:
                    product = self.products[product_id]
                    return [(
                        product.source,
                        product.product_id,
                        product.brand,
                        product.model,
                        product.capacity,
                        product.frequency,
                        product.type,
                        product.url
                    )]
        return []
    
    def add_product(self, product: StandardizedProduct):
        """Add a product to the mock database."""
        db_id = self.next_id
        self.next_id += 1
        self.products[db_id] = product
        return db_id


class MockRepository:
    """Mock repository for testing."""
    
    def __init__(self):
        self.db_manager = MockDatabaseManager()
        self.products = []
        self.price_records = []
    
    def get_products_by_specs(self, brand=None, capacity=None, type=None):
        """Mock get products by specs."""
        filtered = self.products
        if brand:
            filtered = [p for p in filtered if p.brand == brand]
        if capacity:
            filtered = [p for p in filtered if p.capacity == capacity]
        if type:
            filtered = [p for p in filtered if p.type == type]
        return filtered
    
    def get_price_history(self, product_id, start_date, end_date, source=None):
        """Mock get price history."""
        # Find the product to get its database ID
        product = next((p for p in self.products if p.product_id == product_id and (not source or p.source == source)), None)
        if not product:
            return []
        
        # Find the database ID for this product
        db_id = None
        for pid, prod in self.db_manager.products.items():
            if prod.product_id == product_id and prod.source == product.source:
                db_id = pid
                break
        
        if db_id is None:
            return []
        
        # Filter price records by product_id and date range
        filtered = []
        for record in self.price_records:
            if (record.product_id == db_id and 
                start_date <= record.recorded_at.date() <= end_date):
                filtered.append(record)
        
        return filtered
    
    def add_product(self, product: StandardizedProduct):
        """Add a product to the mock repository."""
        self.products.append(product)
        db_id = self.db_manager.add_product(product)
        return db_id
    
    def add_price_record(self, record: PriceRecord):
        """Add a price record to the mock repository."""
        self.price_records.append(record)



class TestDataGroupingConsistency:
    """Test data grouping consistency properties."""
    
    @given(st.lists(generate_standardized_product(), min_size=1, max_size=20))
    def test_data_grouping_consistency_no_cross_contamination(self, products):
        """
        **Feature: memory-price-monitor, Property 13: Data grouping consistency**
        **Validates: Requirements 4.3**
        
        For any price trend display, data should be correctly grouped by brand and 
        memory specifications with no cross-contamination.
        """
        # Create mock components
        repository = MockRepository()
        analyzer = PriceAnalyzer()
        chart_generator = ChartGenerator()
        report_generator = WeeklyReportGenerator(repository, analyzer, chart_generator)
        
        # Add products to repository
        product_db_mapping = []
        for product in products:
            db_id = repository.add_product(product)
            product_db_mapping.append((product, db_id))
        
        # Generate price records for each product
        price_records = []
        for product, db_id in product_db_mapping:
            # Create 1-3 price records per product
            for i in range(1, 4):
                record = PriceRecord(
                    id=len(price_records) + 1,
                    product_id=db_id,
                    current_price=product.current_price + Decimal(str(i)),
                    original_price=product.original_price,
                    recorded_at=datetime.now() - timedelta(days=i),
                    metadata={}
                )
                price_records.append(record)
                repository.add_price_record(record)
        
        # Group the data
        grouped_data = report_generator._group_data_by_specs(price_records)
        
        # Verify no cross-contamination: each group should contain only products
        # with the same brand, capacity, and type
        for group_key, group_data in grouped_data.items():
            if not group_data.products:
                continue
                
            # All products in the group should have the same specifications
            expected_brand = group_data.brand
            expected_capacity = group_data.capacity
            expected_type = group_data.type
            
            for product in group_data.products:
                assert product.brand == expected_brand, \
                    f"Cross-contamination: product {product.product_id} has brand {product.brand}, expected {expected_brand}"
                assert product.capacity == expected_capacity, \
                    f"Cross-contamination: product {product.product_id} has capacity {product.capacity}, expected {expected_capacity}"
                assert product.type == expected_type, \
                    f"Cross-contamination: product {product.product_id} has type {product.type}, expected {expected_type}"
            
            # Verify group key matches the specifications
            expected_key = f"{expected_brand}_{expected_capacity}_{expected_type}"
            assert group_key == expected_key, \
                f"Group key mismatch: got {group_key}, expected {expected_key}"
    
    @given(st.lists(generate_standardized_product(), min_size=2, max_size=10))
    def test_data_grouping_completeness(self, products):
        """
        **Feature: memory-price-monitor, Property 13: Data grouping consistency**
        **Validates: Requirements 4.3**
        
        For any set of products, grouping should include all products without loss.
        """
        # Create mock components
        repository = MockRepository()
        analyzer = PriceAnalyzer()
        chart_generator = ChartGenerator()
        report_generator = WeeklyReportGenerator(repository, analyzer, chart_generator)
        
        # Add products to repository
        product_db_mapping = []
        for product in products:
            db_id = repository.add_product(product)
            product_db_mapping.append((product, db_id))
        
        # Generate one price record per product
        price_records = []
        for i, (product, db_id) in enumerate(product_db_mapping):
            record = PriceRecord(
                id=i + 1,
                product_id=db_id,
                current_price=product.current_price,
                original_price=product.original_price,
                recorded_at=datetime.now(),
                metadata={}
            )
            price_records.append(record)
            repository.add_price_record(record)
        
        # Group the data
        grouped_data = report_generator._group_data_by_specs(price_records)
        
        # Count total products in all groups
        total_grouped_products = sum(len(group.products) for group in grouped_data.values())
        
        # All products should be included in groups (no loss)
        assert total_grouped_products == len(products), \
            f"Product loss during grouping: {total_grouped_products} grouped vs {len(products)} original"
        
        # Count total price records in all groups
        total_grouped_records = sum(len(group.price_records) for group in grouped_data.values())
        
        # All price records should be included in groups
        assert total_grouped_records == len(price_records), \
            f"Price record loss during grouping: {total_grouped_records} grouped vs {len(price_records)} original"
    
    @given(st.lists(generate_standardized_product(), min_size=1, max_size=15))
    def test_data_grouping_deterministic(self, products):
        """
        **Feature: memory-price-monitor, Property 13: Data grouping consistency**
        **Validates: Requirements 4.3**
        
        For any set of products, grouping should be deterministic and consistent.
        """
        # Create mock components
        repository = MockRepository()
        analyzer = PriceAnalyzer()
        chart_generator = ChartGenerator()
        report_generator = WeeklyReportGenerator(repository, analyzer, chart_generator)
        
        # Add products to repository
        product_db_mapping = []
        for product in products:
            db_id = repository.add_product(product)
            product_db_mapping.append((product, db_id))
        
        # Generate price records
        price_records = []
        for i, (product, db_id) in enumerate(product_db_mapping):
            record = PriceRecord(
                id=i + 1,
                product_id=db_id,
                current_price=product.current_price,
                original_price=product.original_price,
                recorded_at=datetime.now(),
                metadata={}
            )
            price_records.append(record)
            repository.add_price_record(record)
        
        # Group the data twice
        grouped_data_1 = report_generator._group_data_by_specs(price_records)
        grouped_data_2 = report_generator._group_data_by_specs(price_records)
        
        # Results should be identical
        assert set(grouped_data_1.keys()) == set(grouped_data_2.keys()), \
            "Grouping is not deterministic: different group keys"
        
        for group_key in grouped_data_1.keys():
            group_1 = grouped_data_1[group_key]
            group_2 = grouped_data_2[group_key]
            
            assert group_1.brand == group_2.brand, \
                f"Grouping is not deterministic: brand mismatch for {group_key}"
            assert group_1.capacity == group_2.capacity, \
                f"Grouping is not deterministic: capacity mismatch for {group_key}"
            assert group_1.type == group_2.type, \
                f"Grouping is not deterministic: type mismatch for {group_key}"
            assert len(group_1.products) == len(group_2.products), \
                f"Grouping is not deterministic: product count mismatch for {group_key}"
            assert len(group_1.price_records) == len(group_2.price_records), \
                f"Grouping is not deterministic: price record count mismatch for {group_key}"
    
    @given(st.lists(generate_standardized_product(), min_size=3, max_size=12))
    def test_data_grouping_specification_isolation(self, products):
        """
        **Feature: memory-price-monitor, Property 13: Data grouping consistency**
        **Validates: Requirements 4.3**
        
        For any products with different specifications, they should be in separate groups.
        """
        # Create mock components
        repository = MockRepository()
        analyzer = PriceAnalyzer()
        chart_generator = ChartGenerator()
        report_generator = WeeklyReportGenerator(repository, analyzer, chart_generator)
        
        # Ensure we have products with different specifications
        assume(len(set((p.brand, p.capacity, p.type) for p in products)) >= 2)
        
        # Add products to repository
        product_db_mapping = []
        for product in products:
            db_id = repository.add_product(product)
            product_db_mapping.append((product, db_id))
        
        # Generate price records
        price_records = []
        for i, (product, db_id) in enumerate(product_db_mapping):
            record = PriceRecord(
                id=i + 1,
                product_id=db_id,
                current_price=product.current_price,
                original_price=product.original_price,
                recorded_at=datetime.now(),
                metadata={}
            )
            price_records.append(record)
            repository.add_price_record(record)
        
        # Group the data
        grouped_data = report_generator._group_data_by_specs(price_records)
        
        # Create a mapping of specifications to group keys
        spec_to_groups = defaultdict(list)
        for group_key, group_data in grouped_data.items():
            spec = (group_data.brand, group_data.capacity, group_data.type)
            spec_to_groups[spec].append(group_key)
        
        # Each unique specification should map to exactly one group
        for spec, group_keys in spec_to_groups.items():
            assert len(group_keys) == 1, \
                f"Specification {spec} maps to multiple groups: {group_keys}"
        
        # Products with different specifications should be in different groups
        unique_specs = set((p.brand, p.capacity, p.type) for p in products)
        if len(unique_specs) > 1:
            # We should have multiple groups
            assert len(grouped_data) >= 2, \
                f"Expected multiple groups for {len(unique_specs)} unique specifications, got {len(grouped_data)}"
    
    @given(
        st.lists(generate_standardized_product(), min_size=1, max_size=8),
        st.lists(generate_price_record(), min_size=1, max_size=20)
    )
    def test_data_grouping_price_record_association(self, products, price_records):
        """
        **Feature: memory-price-monitor, Property 13: Data grouping consistency**
        **Validates: Requirements 4.3**
        
        For any price records, they should be associated with the correct product groups.
        """
        # Create mock components
        repository = MockRepository()
        analyzer = PriceAnalyzer()
        chart_generator = ChartGenerator()
        report_generator = WeeklyReportGenerator(repository, analyzer, chart_generator)
        
        # Add products to repository and create mapping
        product_db_mapping = []
        for product in products:
            db_id = repository.add_product(product)
            product_db_mapping.append((product, db_id))
        
        # Assign price records to products (randomly but consistently)
        if not products:
            return  # Skip if no products
        
        for i, record in enumerate(price_records):
            # Assign to a product based on index modulo
            product, db_id = product_db_mapping[i % len(product_db_mapping)]
            record.product_id = db_id
            repository.add_price_record(record)
        
        # Group the data
        grouped_data = report_generator._group_data_by_specs(price_records)
        
        # Verify that each price record is in the correct group
        for group_key, group_data in grouped_data.items():
            for record in group_data.price_records:
                # Find the product this record belongs to
                product = None
                for p, db_id in product_db_mapping:
                    if db_id == record.product_id:
                        product = p
                        break
                
                if product:
                    # Verify the record is in the correct group
                    expected_key = f"{product.brand}_{product.capacity}_{product.type}"
                    assert group_key == expected_key, \
                        f"Price record {record.id} in wrong group: {group_key} vs expected {expected_key}"
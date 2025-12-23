"""
Property-based tests for data standardization.

**Feature: memory-price-monitor, Property 2: Data standardization consistency**
**Validates: Requirements 1.3, 2.2**
"""

import pytest
from datetime import datetime
from decimal import Decimal
from typing import Dict, Any
from hypothesis import given, strategies as st, assume
from hypothesis import settings
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from memory_price_monitor.data.models import DataStandardizer, StandardizedProduct, MemoryType
from memory_price_monitor.utils.errors import ValidationError


# Mock raw product class for testing
class MockRawProduct:
    def __init__(self, product_id: str, url: str, timestamp: datetime, raw_data: Dict[str, Any]):
        self.product_id = product_id
        self.url = url
        self.timestamp = timestamp
        self.raw_data = raw_data


# Hypothesis strategies for generating test data
@st.composite
def valid_price_strategy(draw):
    """Generate valid price values."""
    return draw(st.decimals(min_value=Decimal('0.01'), max_value=Decimal('99999.99'), places=2))


@st.composite
def valid_brand_strategy(draw):
    """Generate valid brand names."""
    brands = ['CORSAIR', 'KINGSTON', 'G.SKILL', 'CRUCIAL', 'SAMSUNG', 'SK HYNIX', 'ADATA', 'TEAMGROUP']
    return draw(st.sampled_from(brands))


@st.composite
def valid_capacity_strategy(draw):
    """Generate valid memory capacity strings."""
    capacities = ['4GB', '8GB', '16GB', '32GB', '64GB', '128GB', '1TB', '2TB']
    return draw(st.sampled_from(capacities))


@st.composite
def valid_frequency_strategy(draw):
    """Generate valid memory frequency strings."""
    frequencies = ['2133MHz', '2400MHz', '2666MHz', '3000MHz', '3200MHz', '3600MHz', '4000MHz', '4800MHz']
    return draw(st.sampled_from(frequencies))


@st.composite
def valid_memory_type_strategy(draw):
    """Generate valid memory types."""
    return draw(st.sampled_from([t.value for t in MemoryType]))


@st.composite
def jd_raw_product_strategy(draw):
    """Generate JD raw product data."""
    brand = draw(valid_brand_strategy())
    capacity = draw(valid_capacity_strategy())
    frequency = draw(valid_frequency_strategy())
    memory_type = draw(valid_memory_type_strategy())
    price = draw(valid_price_strategy())
    original_price = draw(st.decimals(min_value=price, max_value=price * Decimal('2'), places=2))
    
    title = f"{brand} {memory_type} {capacity} {frequency} Memory Module"
    
    raw_data = {
        'brand': brand,
        'title': title,
        'price': float(price),
        'original_price': float(original_price)
    }
    
    product_id = draw(st.text(min_size=5, max_size=20, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'))))
    url = f"https://item.jd.com/{product_id}.html"
    timestamp = draw(st.datetimes(min_value=datetime(2020, 1, 1), max_value=datetime(2024, 12, 31)))
    
    return MockRawProduct(product_id, url, timestamp, raw_data)


@st.composite
def zol_raw_product_strategy(draw):
    """Generate ZOL raw product data."""
    brand = draw(valid_brand_strategy())
    capacity = draw(valid_capacity_strategy())
    frequency = draw(valid_frequency_strategy())
    memory_type = draw(valid_memory_type_strategy())
    price = draw(valid_price_strategy())
    original_price = draw(st.decimals(min_value=price, max_value=price * Decimal('2'), places=2))
    
    raw_data = {
        'brand': brand,
        'name': f"{brand} {memory_type} Memory",
        'specs': {
            'capacity': capacity,
            'frequency': frequency,
            'type': memory_type
        },
        'current_price': float(price),
        'market_price': float(original_price)
    }
    
    product_id = draw(st.text(min_size=5, max_size=20, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'))))
    url = f"https://detail.zol.com.cn/memory/{product_id}.html"
    timestamp = draw(st.datetimes(min_value=datetime(2020, 1, 1), max_value=datetime(2024, 12, 31)))
    
    return MockRawProduct(product_id, url, timestamp, raw_data)


@st.composite
def generic_raw_product_strategy(draw):
    """Generate generic raw product data."""
    brand = draw(valid_brand_strategy())
    capacity = draw(valid_capacity_strategy())
    frequency = draw(valid_frequency_strategy())
    memory_type = draw(valid_memory_type_strategy())
    price = draw(valid_price_strategy())
    original_price = draw(st.decimals(min_value=price, max_value=price * Decimal('2'), places=2))
    
    raw_data = {
        'brand': brand,
        'model': f"{brand} {memory_type} {capacity}",
        'price': float(price),
        'original_price': float(original_price)
    }
    
    product_id = draw(st.text(min_size=5, max_size=20, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'))))
    url = f"https://example.com/product/{product_id}"
    timestamp = draw(st.datetimes(min_value=datetime(2020, 1, 1), max_value=datetime(2024, 12, 31)))
    
    return MockRawProduct(product_id, url, timestamp, raw_data)


class TestDataStandardizationConsistency:
    """Test data standardization consistency property."""
    
    @given(raw_product=jd_raw_product_strategy())
    @settings(max_examples=10)
    def test_jd_data_standardization_consistency(self, raw_product):
        """
        **Feature: memory-price-monitor, Property 2: Data standardization consistency**
        
        For any raw product data from JD source, the Data_Standardizer should convert it 
        to standardized Price_Data format with all required fields populated.
        
        **Validates: Requirements 1.3, 2.2**
        """
        standardizer = DataStandardizer()
        
        # Standardize the raw product data
        standardized = standardizer.standardize(raw_product, 'jd')
        
        # Verify all required fields are populated
        assert standardized.source == 'jd'
        assert standardized.product_id == raw_product.product_id
        assert standardized.brand != ""
        assert standardized.model != ""
        assert standardized.capacity != ""
        assert standardized.type != ""
        assert standardized.current_price > 0
        assert standardized.original_price > 0
        assert standardized.url == raw_product.url
        assert standardized.timestamp == raw_product.timestamp
        
        # Verify data types are correct
        assert isinstance(standardized.current_price, Decimal)
        assert isinstance(standardized.original_price, Decimal)
        assert isinstance(standardized.timestamp, datetime)
        assert isinstance(standardized.metadata, dict)
        
        # Verify price relationship
        assert standardized.current_price <= standardized.original_price * 2
        
        # Verify capacity format
        assert any(unit in standardized.capacity.upper() for unit in ['GB', 'TB'])
        
        # Verify memory type is valid
        valid_types = [t.value for t in MemoryType]
        assert standardized.type.upper() in [t.upper() for t in valid_types]
    
    @given(raw_product=zol_raw_product_strategy())
    @settings(max_examples=10)
    def test_zol_data_standardization_consistency(self, raw_product):
        """
        **Feature: memory-price-monitor, Property 2: Data standardization consistency**
        
        For any raw product data from ZOL source, the Data_Standardizer should convert it 
        to standardized Price_Data format with all required fields populated.
        
        **Validates: Requirements 1.3, 2.2**
        """
        standardizer = DataStandardizer()
        
        # Standardize the raw product data
        standardized = standardizer.standardize(raw_product, 'zol')
        
        # Verify all required fields are populated
        assert standardized.source == 'zol'
        assert standardized.product_id == raw_product.product_id
        assert standardized.brand != ""
        assert standardized.model != ""
        assert standardized.capacity != ""
        assert standardized.type != ""
        assert standardized.current_price > 0
        assert standardized.original_price > 0
        assert standardized.url == raw_product.url
        assert standardized.timestamp == raw_product.timestamp
        
        # Verify data types are correct
        assert isinstance(standardized.current_price, Decimal)
        assert isinstance(standardized.original_price, Decimal)
        assert isinstance(standardized.timestamp, datetime)
        assert isinstance(standardized.metadata, dict)
        
        # Verify price relationship
        assert standardized.current_price <= standardized.original_price * 2
        
        # Verify capacity format
        assert any(unit in standardized.capacity.upper() for unit in ['GB', 'TB'])
        
        # Verify memory type is valid
        valid_types = [t.value for t in MemoryType]
        assert standardized.type.upper() in [t.upper() for t in valid_types]
    
    @given(raw_product=generic_raw_product_strategy(), source=st.text(min_size=1, max_size=20, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'))))
    @settings(max_examples=10)
    def test_generic_data_standardization_consistency(self, raw_product, source):
        """
        **Feature: memory-price-monitor, Property 2: Data standardization consistency**
        
        For any raw product data from any generic source, the Data_Standardizer should convert it 
        to standardized Price_Data format with all required fields populated.
        
        **Validates: Requirements 1.3, 2.2**
        """
        assume(source.lower() not in ['jd', 'zol'])  # Exclude specific sources
        
        standardizer = DataStandardizer()
        
        # Standardize the raw product data
        standardized = standardizer.standardize(raw_product, source)
        
        # Verify all required fields are populated
        assert standardized.source == source
        assert standardized.product_id == raw_product.product_id
        assert standardized.brand != ""
        assert standardized.model != ""
        assert standardized.current_price > 0
        assert standardized.original_price > 0
        assert standardized.url == raw_product.url
        assert standardized.timestamp == raw_product.timestamp
        
        # Verify data types are correct
        assert isinstance(standardized.current_price, Decimal)
        assert isinstance(standardized.original_price, Decimal)
        assert isinstance(standardized.timestamp, datetime)
        assert isinstance(standardized.metadata, dict)
        
        # Verify price relationship
        assert standardized.current_price <= standardized.original_price * 2
    
    @given(raw_product=jd_raw_product_strategy())
    @settings(max_examples=10)
    def test_standardization_idempotency(self, raw_product):
        """
        Test that standardizing the same raw data multiple times produces identical results.
        """
        standardizer = DataStandardizer()
        
        # Standardize the same data multiple times
        result1 = standardizer.standardize(raw_product, 'jd')
        result2 = standardizer.standardize(raw_product, 'jd')
        result3 = standardizer.standardize(raw_product, 'jd')
        
        # All results should be identical
        assert result1.source == result2.source == result3.source
        assert result1.product_id == result2.product_id == result3.product_id
        assert result1.brand == result2.brand == result3.brand
        assert result1.model == result2.model == result3.model
        assert result1.capacity == result2.capacity == result3.capacity
        assert result1.frequency == result2.frequency == result3.frequency
        assert result1.type == result2.type == result3.type
        assert result1.current_price == result2.current_price == result3.current_price
        assert result1.original_price == result2.original_price == result3.original_price
        assert result1.url == result2.url == result3.url
        assert result1.timestamp == result2.timestamp == result3.timestamp
    
    @given(raw_product=jd_raw_product_strategy())
    @settings(max_examples=10)
    def test_validation_consistency(self, raw_product):
        """
        Test that validation results are consistent for standardized products.
        """
        standardizer = DataStandardizer()
        
        # Standardize the raw product data
        standardized = standardizer.standardize(raw_product, 'jd')
        
        # Validate the standardized product
        validation_result = standardizer.validate(standardized)
        
        # If validation passes, the product should be valid
        if validation_result.is_valid:
            # Should not raise ValidationError
            try:
                standardized.validate()
            except ValidationError:
                pytest.fail("Product validation should not fail if standardizer validation passes")
        
        # Validation result should have proper structure
        assert isinstance(validation_result.is_valid, bool)
        assert isinstance(validation_result.errors, list)
        assert isinstance(validation_result.warnings, list)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
"""
Data models and standardization interfaces.
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List
from datetime import datetime, date
from decimal import Decimal
from enum import Enum

from memory_price_monitor.utils.errors import ValidationError


class MemoryType(Enum):
    """Memory type enumeration."""
    DDR3 = "DDR3"
    DDR4 = "DDR4"
    DDR5 = "DDR5"
    LPDDR4 = "LPDDR4"
    LPDDR5 = "LPDDR5"


@dataclass
class StandardizedProduct:
    """Standardized product data model."""
    source: str              # Data source (jd, zol, etc.)
    product_id: str          # Product unique identifier
    brand: str               # Brand name
    model: str               # Product model
    capacity: str            # Memory capacity (e.g., "16GB")
    frequency: str           # Memory frequency (e.g., "3200MHz")
    type: str                # Memory type (DDR4, DDR5, etc.)
    current_price: Decimal   # Current price
    original_price: Decimal  # Original/list price
    url: str                 # Product URL
    timestamp: datetime      # Data collection timestamp
    metadata: Dict[str, Any] = field(default_factory=dict)  # Additional metadata
    
    def __post_init__(self):
        """Validate data after initialization."""
        self.validate()
    
    def validate(self) -> None:
        """
        Validate product data.
        
        Raises:
            ValidationError: If data is invalid
        """
        errors = []
        
        # Required fields validation
        if not self.source:
            errors.append("Source is required")
        if not self.product_id:
            errors.append("Product ID is required")
        if not self.brand:
            errors.append("Brand is required")
        if not self.model:
            errors.append("Model is required")
        if not self.capacity:
            errors.append("Capacity is required")
        if not self.type:
            errors.append("Memory type is required")
        if not self.url:
            errors.append("URL is required")
        
        # Price validation
        if self.current_price <= 0:
            errors.append("Current price must be positive")
        if self.original_price <= 0:
            errors.append("Original price must be positive")
        if self.current_price > self.original_price * 2:
            errors.append("Current price seems unreasonably high compared to original price")
        
        # Format validation
        if self.capacity and not any(unit in self.capacity.upper() for unit in ['GB', 'TB']):
            errors.append("Capacity must include GB or TB unit")
        
        if self.frequency and not any(unit in self.frequency.upper() for unit in ['MHZ', 'GHZ']):
            errors.append("Frequency must include MHz or GHz unit")
        
        # Memory type validation
        valid_types = [t.value for t in MemoryType]
        if self.type.upper() not in [t.upper() for t in valid_types]:
            errors.append(f"Memory type must be one of: {', '.join(valid_types)}")
        
        if errors:
            raise ValidationError(
                "Product validation failed",
                {"errors": errors, "product_id": self.product_id}
            )


@dataclass
class ValidationResult:
    """Result of data validation."""
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


@dataclass
class PriceRecord:
    """Price record for database storage."""
    id: Optional[int] = None
    product_id: int = 0
    current_price: Decimal = Decimal('0')
    original_price: Optional[Decimal] = None
    recorded_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TrendData:
    """Price trend analysis data."""
    product_id: str
    brand: str
    model: str
    capacity: str
    type: str
    current_week_prices: List[Decimal]
    previous_week_prices: List[Decimal]
    average_current: Decimal
    average_previous: Decimal
    percentage_change: Decimal
    trend_direction: str  # "up", "down", "stable"


@dataclass
class ComparisonResult:
    """Week-over-week comparison result."""
    current_week_start: date
    previous_week_start: date
    trends: List[TrendData]
    overall_average_change: Decimal
    significant_changes: List[TrendData]  # Changes > threshold


@dataclass
class WeeklyReport:
    """Weekly price report data structure."""
    report_date: date
    week_start: date
    week_end: date
    comparison: ComparisonResult
    charts: Dict[str, bytes]  # Chart name -> chart image bytes
    summary_stats: Dict[str, Any]
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FormattedReport:
    """Formatted report for notifications."""
    subject: str
    text_content: str
    html_content: str
    attachments: List[Dict[str, Any]] = field(default_factory=list)  # filename, content, content_type
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataStandardizer:
    """Standardizes raw product data from different sources."""
    
    def __init__(self):
        """Initialize data standardizer."""
        self._brand_mappings = self._load_brand_mappings()
        self._capacity_patterns = self._load_capacity_patterns()
        self._frequency_patterns = self._load_frequency_patterns()
    
    def standardize(self, raw_product, source: str) -> StandardizedProduct:
        """
        Convert raw product data to standardized format.
        
        Args:
            raw_product: Raw product data from crawler
            source: Data source name
            
        Returns:
            Standardized product data
            
        Raises:
            ValidationError: If standardization fails
        """
        try:
            # Extract and normalize fields based on source
            if source.lower() == 'jd':
                return self._standardize_jd(raw_product)
            elif source.lower() == 'zol':
                return self._standardize_zol(raw_product)
            else:
                return self._standardize_generic(raw_product, source)
                
        except Exception as e:
            raise ValidationError(
                f"Failed to standardize product from {source}",
                {"error": str(e), "raw_data": str(raw_product)[:500]}
            )
    
    def validate(self, product: StandardizedProduct) -> ValidationResult:
        """
        Validate standardized product data.
        
        Args:
            product: Standardized product to validate
            
        Returns:
            Validation result with errors and warnings
        """
        result = ValidationResult(is_valid=True)
        
        try:
            product.validate()
        except ValidationError as e:
            result.is_valid = False
            result.errors = e.details.get('errors', [str(e)])
        
        # Additional warnings
        if product.current_price < product.original_price * Decimal('0.3'):
            result.warnings.append("Price seems unusually low (>70% discount)")
        
        if not product.frequency:
            result.warnings.append("Memory frequency not specified")
        
        return result
    
    def _standardize_jd(self, raw_product) -> StandardizedProduct:
        """Standardize JD product data."""
        raw_data = raw_product.raw_data
        
        return StandardizedProduct(
            source='jd',
            product_id=raw_product.product_id,
            brand=self._normalize_brand(raw_data.get('brand', '')),
            model=raw_data.get('title', ''),
            capacity=self._extract_capacity(raw_data.get('title', '')),
            frequency=self._extract_frequency(raw_data.get('title', '')),
            type=self._extract_memory_type(raw_data.get('title', '')),
            current_price=Decimal(str(raw_data.get('price', 0))),
            original_price=Decimal(str(raw_data.get('original_price', raw_data.get('price', 0)))),
            url=raw_product.url,
            timestamp=raw_product.timestamp,
            metadata=raw_data
        )
    
    def _standardize_zol(self, raw_product) -> StandardizedProduct:
        """Standardize ZOL product data."""
        raw_data = raw_product.raw_data
        
        # Extract product name/title
        product_name = raw_data.get('name', '')
        
        # Extract capacity from name if not in specs
        capacity = self._extract_capacity(product_name)
        if not capacity and 'specs' in raw_data:
            capacity = self._extract_capacity(raw_data.get('specs', {}).get('capacity', ''))
        
        # Extract frequency from name if not in specs
        frequency = self._extract_frequency(product_name)
        if not frequency and 'specs' in raw_data:
            frequency = self._extract_frequency(raw_data.get('specs', {}).get('frequency', ''))
        
        # Extract memory type from name if not in specs
        memory_type = self._extract_memory_type(product_name)
        if not memory_type and 'specs' in raw_data:
            memory_type = self._extract_memory_type(raw_data.get('specs', {}).get('type', ''))
        
        # Default to DDR4 if no type found (most common)
        if not memory_type:
            memory_type = 'DDR4'
        
        # Ensure we have capacity
        if not capacity:
            capacity = '16GB'  # Default fallback
        
        return StandardizedProduct(
            source='zol_playwright',
            product_id=raw_product.product_id,
            brand=self._normalize_brand(raw_data.get('brand', '')),
            model=product_name,
            capacity=capacity,
            frequency=frequency,
            type=memory_type,
            current_price=Decimal(str(raw_data.get('current_price', 0))),
            original_price=Decimal(str(raw_data.get('market_price', raw_data.get('current_price', 0)))),
            url=raw_product.url,
            timestamp=raw_product.timestamp,
            metadata=raw_data
        )
    
    def _standardize_generic(self, raw_product, source: str) -> StandardizedProduct:
        """Standardize generic product data."""
        raw_data = raw_product.raw_data
        
        return StandardizedProduct(
            source=source,
            product_id=raw_product.product_id,
            brand=self._normalize_brand(raw_data.get('brand', '')),
            model=raw_data.get('model', raw_data.get('title', raw_data.get('name', ''))),
            capacity=self._extract_capacity(str(raw_data)),
            frequency=self._extract_frequency(str(raw_data)),
            type=self._extract_memory_type(str(raw_data)),
            current_price=Decimal(str(raw_data.get('price', raw_data.get('current_price', 0)))),
            original_price=Decimal(str(raw_data.get('original_price', raw_data.get('market_price', raw_data.get('price', 0))))),
            url=raw_product.url,
            timestamp=raw_product.timestamp,
            metadata=raw_data
        )
    
    def _normalize_brand(self, brand: str) -> str:
        """Normalize brand name."""
        brand = brand.strip().upper()
        return self._brand_mappings.get(brand, brand)
    
    def _extract_capacity(self, text: str) -> str:
        """Extract memory capacity from text."""
        import re
        text = text.upper()
        
        # Look for patterns like "16GB", "32G", "1TB", "2×8GB"
        for pattern in self._capacity_patterns:
            match = re.search(pattern, text)
            if match:
                if '×' in pattern:
                    # Handle patterns like "2×8GB" or "16GB（2×8GB）"
                    if '×' in match.group(0):
                        return match.group(0)
                    elif len(match.groups()) >= 2:
                        # Pattern like (\d+)\s*×\s*(\d+)\s*GB
                        return f"{match.group(1)}×{match.group(2)}GB"
                return match.group(0)
        
        return ""
    
    def _extract_frequency(self, text: str) -> str:
        """Extract memory frequency from text."""
        import re
        text = text.upper()
        
        # Look for patterns like "3200MHz", "3200", "DDR4-3200"
        for pattern in self._frequency_patterns:
            match = re.search(pattern, text)
            if match:
                freq = match.group(1) if match.groups() else match.group(0)
                return f"{freq}MHz" if not freq.endswith('MHZ') else freq
        
        return ""
    
    def _extract_memory_type(self, text: str) -> str:
        """Extract memory type from text."""
        text = text.upper()
        
        for mem_type in MemoryType:
            if mem_type.value in text:
                return mem_type.value
        
        return ""
    
    def _load_brand_mappings(self) -> Dict[str, str]:
        """Load brand name mappings for normalization."""
        return {
            # English brands
            'CORSAIR': 'CORSAIR',
            'KINGSTON': 'KINGSTON', 
            'G.SKILL': 'G.SKILL',
            'CRUCIAL': 'CRUCIAL',
            'SAMSUNG': 'SAMSUNG',
            'SK HYNIX': 'SK HYNIX',
            'ADATA': 'ADATA',
            'TEAMGROUP': 'TEAMGROUP',
            'MUSHKIN': 'MUSHKIN',
            'PATRIOT': 'PATRIOT',
            # Chinese brands
            '金士顿': '金士顿',
            '海盗船': '海盗船',
            '芝奇': '芝奇',
            '英睿达': '英睿达',
            '三星': '三星',
            '威刚': '威刚',
            '十铨': '十铨',
            '博帝': '博帝',
            '七彩虹': '七彩虹',
            '影驰': '影驰',
            '阿斯加特': '阿斯加特',
            '科赋': '科赋',
            '瑞势': '瑞势',
            '金泰克': '金泰克',
            '金邦科技': '金邦科技',
            '特科芯': '特科芯',
            '宇瞻': '宇瞻',
            '光威': '光威',
            '现代': '现代',
            '创见': '创见',
            '十铨科技': '十铨科技',
            '金百达': '金百达',
            '金百达银爵': '金百达',
            'PNY': 'PNY',
            '国惠': '国惠',
            '惠普': '惠普',
            '昱联': '昱联',
            '美商海盗船': '海盗船',
            '雷克沙': '雷克沙',
            'ACER宏碁': 'ACER',
            '联想': '联想',
            '玖合': '玖合',
            '佰维': '佰维',
            '酷兽': '酷兽',
            '海力士': 'SK HYNIX',
            '先锋': '先锋',
            '瑾宇': '瑾宇',
            '铭瑄': '铭瑄',
            '沃存': '沃存',
            '长城': '长城',
            '紫光': '紫光',
            '记忆科技': '记忆科技'
        }
    
    def _load_capacity_patterns(self) -> List[str]:
        """Load regex patterns for capacity extraction."""
        return [
            r'\b(\d+(?:\.\d+)?)\s*TB\b',
            r'\b(\d+)\s*GB\b',
            r'\b(\d+)\s*G\b',
            r'(\d+)GB',
            r'(\d+)G(?!Hz)',  # G but not GHz
            r'(\d+×\d+GB)',   # Format like 2×8GB
            r'(\d+)\s*×\s*(\d+)\s*GB'  # Spaced format
        ]
    
    def _load_frequency_patterns(self) -> List[str]:
        """Load regex patterns for frequency extraction."""
        return [
            r'\b(\d+)\s*MHZ\b',
            r'\bDDR[45]-(\d+)\b',
            r'\b(\d{4})\b',  # 4-digit numbers like 3200
            r'(\d+)MHz',
            r'DDR4\s+(\d+)',
            r'DDR5\s+(\d+)'
        ]
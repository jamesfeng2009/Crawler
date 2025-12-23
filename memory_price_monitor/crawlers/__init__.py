"""
Crawler modules for different e-commerce platforms.
"""

from .base import BaseCrawler, CrawlerRegistry, RawProduct, PricePoint, CrawlResult
from .http_client import HTTPClient, RateLimitConfig, RetryConfig, UserAgentRotator
from .jd_crawler import JDCrawler
from .zol_crawler import ZOLCrawler

# Import Playwright crawlers (optional, requires playwright installation)
try:
    from .playwright_jd_crawler import PlaywrightJDCrawler
    from .playwright_zol_crawler import PlaywrightZOLCrawler
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PlaywrightJDCrawler = None
    PlaywrightZOLCrawler = None
    PLAYWRIGHT_AVAILABLE = False

# Create default crawler registry and register available crawlers
default_registry = CrawlerRegistry()

# Register JD crawler with default configuration
jd_default_config = {
    'base_url': 'https://www.jd.com',
    'search_url': 'https://search.jd.com/Search',
    'api_url': 'https://api.m.jd.com',
    'search_keywords': [
        '内存条', 'DDR4内存', 'DDR5内存', '台式机内存', '笔记本内存'
    ],
    'max_pages': 10,
    'products_per_page': 60,
    'min_delay': 2.0,
    'max_delay': 5.0,
    'timeout': 30.0,
    'rate_limit': {
        'requests_per_second': 0.5,  # Conservative rate limiting for JD
        'requests_per_minute': 30,
        'requests_per_hour': 1800,
        'burst_size': 3,
        'backoff_factor': 2.0,
        'max_delay': 300.0
    },
    'retry': {
        'max_attempts': 3,
        'backoff_factor': 2.0,
        'initial_delay': 1.0,
        'max_delay': 60.0,
        'retry_on_status': [429, 500, 502, 503, 504]
    }
}

default_registry.register('jd', JDCrawler, jd_default_config)

# Register ZOL crawler with default configuration
zol_default_config = {
    'base_url': 'https://www.zol.com.cn',
    'search_url': 'https://search.zol.com.cn/s/list.php',
    'product_base_url': 'https://detail.zol.com.cn',
    'search_keywords': [
        '内存条', 'DDR4内存', 'DDR5内存', '台式机内存', '笔记本内存'
    ],
    'memory_category': 'memory',
    'max_pages': 10,
    'products_per_page': 20,
    'min_delay': 2.0,
    'max_delay': 5.0,
    'timeout': 30.0,
    'rate_limit': {
        'requests_per_second': 0.3,  # More conservative for ZOL
        'requests_per_minute': 18,
        'requests_per_hour': 1080,
        'burst_size': 2,
        'backoff_factor': 2.5,
        'max_delay': 300.0
    },
    'retry': {
        'max_attempts': 3,
        'backoff_factor': 2.0,
        'initial_delay': 1.5,
        'max_delay': 60.0,
        'retry_on_status': [429, 500, 502, 503, 504]
    }
}

default_registry.register('zol', ZOLCrawler, zol_default_config)

# Register Playwright crawlers if available
if PLAYWRIGHT_AVAILABLE:
    # Playwright JD crawler configuration
    playwright_jd_config = {
        'headless': True,
        'browser_type': 'chromium',
        'viewport_width': 1920,
        'viewport_height': 1080,
        'page_timeout': 30000,
        'base_url': 'https://www.jd.com',
        'search_url': 'https://search.jd.com/Search',
        'search_keywords': ['内存条', 'DDR4内存', 'DDR5内存'],
        'max_pages': 2,  # Conservative for testing
        'min_delay': 3.0,
        'max_delay': 8.0,
        'scroll_delay': 1.0
    }
    
    default_registry.register('jd_playwright', PlaywrightJDCrawler, playwright_jd_config)
    
    # Playwright ZOL crawler configuration
    playwright_zol_config = {
        'headless': True,
        'browser_type': 'chromium',
        'viewport_width': 1920,
        'viewport_height': 1080,
        'page_timeout': 30000,
        'base_url': 'https://www.zol.com.cn',
        'search_url': 'https://search.zol.com.cn/s/all.php',
        'memory_category_url': 'https://detail.zol.com.cn/memory/',
        'search_keywords': ['内存条', 'DDR4内存', 'DDR5内存'],
        'max_pages': 3,  # Conservative for testing
        'min_delay': 2.0,
        'max_delay': 5.0,
        'scroll_delay': 1.0
    }
    
    default_registry.register('zol_playwright', PlaywrightZOLCrawler, playwright_zol_config)

__all__ = [
    'BaseCrawler',
    'CrawlerRegistry', 
    'RawProduct',
    'PricePoint',
    'CrawlResult',
    'HTTPClient',
    'RateLimitConfig',
    'RetryConfig',
    'UserAgentRotator',
    'JDCrawler',
    'ZOLCrawler',
    'PlaywrightJDCrawler',
    'PlaywrightZOLCrawler',
    'default_registry',
    'PLAYWRIGHT_AVAILABLE'
]
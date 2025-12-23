#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Playwright-based ZOL (中关村在线) crawler implementation for memory products.
Uses real browser automation to bypass anti-crawler mechanisms.
"""

import re
import json
import time
import random
from typing import List, Dict, Any, Optional
from datetime import datetime
from decimal import Decimal

from playwright.sync_api import sync_playwright, Browser, BrowserContext, Page

# Import stealth mode if available
try:
    from playwright_stealth import stealth_sync
    STEALTH_AVAILABLE = True
except ImportError:
    STEALTH_AVAILABLE = False
    stealth_sync = None

from memory_price_monitor.crawlers.base import BaseCrawler, RawProduct, PricePoint
from memory_price_monitor.utils.errors import CrawlerError
from memory_price_monitor.utils.logging import get_business_logger


logger = get_business_logger('crawler_zol')


class PlaywrightZOLCrawler(BaseCrawler):
    """Playwright-based ZOL (中关村在线) crawler for memory products."""
    
    def __init__(self, source_name: str = 'zol_playwright', config: Optional[Dict[str, Any]] = None):
        """
        Initialize Playwright ZOL crawler.
        
        Args:
            source_name: Source name (default: 'zol_playwright')
            config: Crawler configuration
        """
        super().__init__(source_name, config)
        
        # Playwright-specific configuration
        self.headless = self.config.get('headless', True)
        self.browser_type = self.config.get('browser_type', 'chromium')
        self.viewport_width = self.config.get('viewport_width', 1920)
        self.viewport_height = self.config.get('viewport_height', 1080)
        self.page_timeout = self.config.get('page_timeout', 30000)  # 30 seconds
        
        # ZOL-specific configuration
        self.base_url = self.config.get('base_url', 'https://www.zol.com.cn')
        self.search_url = self.config.get('search_url', 'https://search.zol.com.cn/s/all.php')
        self.memory_category_url = self.config.get('memory_category_url', 'https://detail.zol.com.cn/memory/')
        
        # Search parameters for memory products
        self.search_keywords = self.config.get('search_keywords', [
            '内存条', 'DDR4内存', 'DDR5内存'
        ])
        
        # Pagination settings
        self.max_pages = self.config.get('max_pages', 3)  # Reduced for testing
        self.products_per_page = self.config.get('products_per_page', 20)
        
        # Anti-crawler settings
        self.min_delay = self.config.get('min_delay', 2.0)
        self.max_delay = self.config.get('max_delay', 5.0)
        self.scroll_delay = self.config.get('scroll_delay', 1.0)
        
        # Browser instances
        self.playwright = None
        self.browser = None
        self.context = None
        
        # User agents for rotation
        self.user_agents = [
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]
    
    def _init_browser(self):
        """Initialize Playwright browser with stealth mode."""
        try:
            if not self.playwright:
                self.playwright = sync_playwright().start()
            
            if not self.browser:
                # Launch browser with anti-detection arguments
                browser_args = [
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor',
                    '--disable-extensions',
                    '--no-first-run',
                    '--disable-default-apps'
                ]
                
                if self.browser_type == 'chromium':
                    self.browser = self.playwright.chromium.launch(
                        headless=self.headless,
                        args=browser_args
                    )
                elif self.browser_type == 'firefox':
                    self.browser = self.playwright.firefox.launch(
                        headless=self.headless
                    )
                else:
                    self.browser = self.playwright.webkit.launch(
                        headless=self.headless
                    )
            
            if not self.context:
                # Create browser context with realistic settings
                user_agent = random.choice(self.user_agents)
                
                self.context = self.browser.new_context(
                    user_agent=user_agent,
                    viewport={'width': self.viewport_width, 'height': self.viewport_height},
                    locale='zh-CN',
                    timezone_id='Asia/Shanghai',
                    permissions=['geolocation'],
                    geolocation={'latitude': 39.9042, 'longitude': 116.4074},  # Beijing
                    extra_http_headers={
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'DNT': '1',
                        'Sec-Fetch-Dest': 'document',
                        'Sec-Fetch-Mode': 'navigate',
                        'Sec-Fetch-Site': 'none',
                        'Sec-Fetch-User': '?1',
                        'Upgrade-Insecure-Requests': '1'
                    }
                )
                
                # Set default timeout
                self.context.set_default_timeout(self.page_timeout)
            
            self.logger.info("Playwright browser initialized successfully")
            
        except Exception as e:
            raise CrawlerError(
                f"Failed to initialize Playwright browser",
                {"error": str(e), "browser_type": self.browser_type}
            )
    
    def _create_stealth_page(self) -> Page:
        """Create a new page with stealth mode enabled."""
        if not self.context:
            self._init_browser()
        
        page = self.context.new_page()
        
        # Apply stealth mode if available
        if STEALTH_AVAILABLE and stealth_sync:
            stealth_sync(page)
        else:
            self.logger.warning("Stealth mode not available, using basic anti-detection")
        
        # Add additional anti-detection scripts
        page.add_init_script("""
            // Override navigator.webdriver
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            
            // Override navigator.plugins
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });
            
            // Override navigator.languages
            Object.defineProperty(navigator, 'languages', {
                get: () => ['zh-CN', 'zh', 'en']
            });
        """)
        
        return page
    
    def fetch_products(self) -> List[RawProduct]:
        """
        Fetch product list from ZOL search results using Playwright.
        
        Returns:
            List of raw product data
            
        Raises:
            CrawlerError: If fetching fails
        """
        all_products = []
        
        try:
            self._init_browser()
            
            # First try category page approach
            self.logger.info("Trying memory category page approach")
            category_products = self._fetch_from_category_page()
            all_products.extend(category_products)
            
            # Then try search for each keyword
            for keyword in self.search_keywords:
                self.logger.info(f"Searching for keyword: {keyword}")
                
                # Search for products with this keyword
                products = self._search_products(keyword)
                all_products.extend(products)
                
                # Random delay between keyword searches
                delay = random.uniform(self.min_delay, self.max_delay)
                time.sleep(delay)
            
            # Remove duplicates based on product_id
            unique_products = {}
            for product in all_products:
                if product.product_id not in unique_products:
                    unique_products[product.product_id] = product
            
            result = list(unique_products.values())
            self.logger.info(f"Found {len(result)} unique products from {len(all_products)} total")
            
            return result
            
        except Exception as e:
            raise CrawlerError(
                f"Failed to fetch products from ZOL using Playwright",
                {"error": str(e), "keywords": self.search_keywords}
            )
    
    def _fetch_from_category_page(self) -> List[RawProduct]:
        """
        Fetch products from ZOL memory category page.
        
        Returns:
            List of raw products from category page
        """
        products = []
        page = None
        
        try:
            page = self._create_stealth_page()
            
            # Use the main memory category page
            category_url = "https://detail.zol.com.cn/memory/"
            
            self.logger.debug(f"Navigating to memory category page: {category_url}")
            
            # Navigate to memory category page
            page.goto(category_url, wait_until='networkidle', timeout=self.page_timeout)
            
            # Wait for products to load - look for the main product container
            try:
                page.wait_for_selector('#J_PicMode', timeout=15000)
                self.logger.debug("Found main product container #J_PicMode")
            except Exception:
                # Try alternative selectors
                try:
                    page.wait_for_selector('li[data-follow-id]', timeout=10000)
                    self.logger.debug("Found product items with data-follow-id")
                except Exception:
                    self.logger.warning("No products found on category page")
                    return products
            
            # Simulate human behavior
            self._simulate_human_behavior(page)
            
            # Extract products from category page
            products = self._extract_products_from_page(page, 'category')
            
            self.logger.info(f"Found {len(products)} products from category page")
            
        except Exception as e:
            self.logger.warning(f"Failed to fetch from category page: {str(e)}")
        finally:
            if page:
                page.close()
        
        return products
    
    def _search_products(self, keyword: str) -> List[RawProduct]:
        """
        Search for products with a specific keyword using Playwright.
        
        Args:
            keyword: Search keyword
            
        Returns:
            List of raw products for this keyword
        """
        products = []
        page = None
        
        try:
            page = self._create_stealth_page()
            
            for page_num in range(1, self.max_pages + 1):
                self.logger.debug(f"Fetching page {page_num} for keyword: {keyword}")
                
                # Build search URL
                search_params = {
                    'keyword': keyword,
                    'page': page_num
                }
                
                # Navigate to search page
                search_url = f"{self.search_url}?" + "&".join([f"{k}={v}" for k, v in search_params.items()])
                
                self.logger.debug(f"Navigating to: {search_url}")
                
                try:
                    # Navigate with wait for network idle
                    page.goto(search_url, wait_until='networkidle', timeout=self.page_timeout)
                    
                    # Check if we're redirected to homepage
                    current_url = page.url
                    if current_url == self.base_url or current_url == f"{self.base_url}/":
                        self.logger.warning(f"Redirected to homepage for keyword: {keyword}")
                        break
                    
                    # Wait for products to load
                    try:
                        page.wait_for_selector('.list-item, .product-item, .search-item', timeout=10000)
                    except Exception:
                        self.logger.warning(f"No products found on page {page_num}")
                        break
                    
                    # Simulate human behavior
                    self._simulate_human_behavior(page)
                    
                    # Extract products from current page
                    page_products = self._extract_products_from_page(page, keyword)
                    
                    if not page_products:
                        self.logger.info(f"No more products found on page {page_num}, stopping pagination")
                        break
                    
                    products.extend(page_products)
                    self.logger.debug(f"Found {len(page_products)} products on page {page_num}")
                    
                    # Random delay between pages
                    delay = random.uniform(self.min_delay, self.max_delay)
                    time.sleep(delay)
                    
                except Exception as e:
                    self.logger.warning(f"Failed to load page {page_num} for keyword {keyword}: {str(e)}")
                    break
            
        except Exception as e:
            self.logger.error(f"Search failed for keyword {keyword}: {str(e)}")
        finally:
            if page:
                page.close()
        
        return products
    
    def _simulate_human_behavior(self, page: Page):
        """
        Simulate human browsing behavior.
        
        Args:
            page: Playwright page object
        """
        try:
            # Random mouse movement
            page.mouse.move(
                random.randint(100, 800),
                random.randint(100, 600)
            )
            
            # Random scroll
            scroll_amount = random.randint(200, 800)
            page.evaluate(f'window.scrollBy(0, {scroll_amount})')
            
            # Wait a bit
            time.sleep(random.uniform(0.5, self.scroll_delay))
            
            # Scroll back up a bit
            page.evaluate(f'window.scrollBy(0, -{scroll_amount // 2})')
            
            # Another small delay
            time.sleep(random.uniform(0.2, 0.8))
            
        except Exception as e:
            self.logger.warning(f"Error simulating human behavior: {str(e)}")
    
    def _extract_products_from_page(self, page: Page, keyword: str) -> List[RawProduct]:
        """
        Extract products from current page.
        
        Args:
            page: Playwright page object
            keyword: Search keyword
            
        Returns:
            List of raw products
        """
        products = []
        
        try:
            # First try to find the main product container
            main_container = page.locator('#J_PicMode').first
            if main_container.count() > 0:
                # ZOL uses <li> elements with data-follow-id for products within the main container
                product_items = main_container.locator('li[data-follow-id]').all()
                self.logger.debug(f"Found {len(product_items)} product items in main container")
            else:
                # Fallback: look for product items anywhere on the page
                product_items = page.locator('li[data-follow-id]').all()
                self.logger.debug(f"Found {len(product_items)} product items using fallback selector")
            
            if not product_items:
                # Try alternative selectors for different page layouts
                alternative_selectors = [
                    'ul.clearfix li',
                    '.pic-mode-box li',
                    '.product-list li',
                    'li[class*="product"]'
                ]
                
                for selector in alternative_selectors:
                    product_items = page.locator(selector).all()
                    if product_items:
                        self.logger.debug(f"Found {len(product_items)} items using selector: {selector}")
                        break
            
            for item in product_items:
                try:
                    product = self._extract_product_from_element(item, keyword)
                    if product:
                        products.append(product)
                except Exception as e:
                    self.logger.warning(f"Failed to extract product: {str(e)}")
                    continue
            
        except Exception as e:
            self.logger.error(f"Failed to extract products from page: {str(e)}")
        
        return products
    
    def _extract_product_from_element(self, item, keyword: str) -> Optional[RawProduct]:
        """
        Extract product data from a single product element.
        
        Args:
            item: Playwright locator for product item
            keyword: Search keyword
            
        Returns:
            Raw product data or None if extraction fails
        """
        try:
            # Extract data-follow-id as product ID
            product_id = item.get_attribute('data-follow-id')
            if not product_id:
                return None
            
            # Remove 'p' prefix if present (e.g., 'p1403967' -> '1403967')
            if product_id.startswith('p'):
                product_id = product_id[1:]
            
            # Extract product URL from the main product link
            # ZOL uses href="/memory/index{id}.shtml" format
            link_element = item.locator('h3 a').first
            if link_element.count() == 0:
                # Try alternative link selector
                link_element = item.locator('a[href*="/memory/index"]').first
                if link_element.count() == 0:
                    return None
            
            product_url = link_element.get_attribute('href')
            if not product_url:
                return None
            
            # Ensure URL is absolute
            if not product_url.startswith('http'):
                product_url = f"{self.base_url}{product_url}" if product_url.startswith('/') else f"{self.base_url}/{product_url}"
            
            # Extract title from h3 > a element
            title = link_element.get_attribute('title') or link_element.inner_text()
            if not title or len(title.strip()) < 5:
                return None
            
            title = title.strip()
            
            # Extract price from .price-type span
            price = 0.0
            price_element = item.locator('.price-type').first
            if price_element.count() > 0:
                price_text = price_element.inner_text()
                try:
                    # Remove any non-numeric characters except decimal point
                    price_clean = re.sub(r'[^\d.]', '', price_text)
                    if price_clean:
                        price = float(price_clean)
                except (ValueError, TypeError):
                    price = 0.0
            
            # Extract brand from title
            brand = self._extract_brand_from_title(title)
            
            # Extract image URL
            img_url = ''
            img_element = item.locator('img').first
            if img_element.count() > 0:
                # ZOL uses both 'src' and '.src' attributes
                img_src = img_element.get_attribute('src') or img_element.get_attribute('.src')
                if img_src:
                    if not img_src.startswith('http'):
                        img_src = f"https:{img_src}" if img_src.startswith('//') else f"{self.base_url}{img_src}"
                    img_url = img_src
            
            # Extract specifications from title span (if available)
            specs = {}
            spec_element = item.locator('h3 a span').first
            if spec_element.count() > 0:
                spec_text = spec_element.inner_text()
                if spec_text:
                    specs['description'] = spec_text.strip()
            
            # Extract rating information
            rating = 0.0
            rating_element = item.locator('.score').first
            if rating_element.count() > 0:
                rating_text = rating_element.inner_text()
                try:
                    rating = float(rating_text)
                except (ValueError, TypeError):
                    rating = 0.0
            
            # Extract review count
            review_count = 0
            review_element = item.locator('.comment-num').first
            if review_element.count() > 0:
                review_text = review_element.inner_text()
                # Extract number from text like "5701人点评"
                review_match = re.search(r'(\d+)', review_text)
                if review_match:
                    try:
                        review_count = int(review_match.group(1))
                    except (ValueError, TypeError):
                        review_count = 0
            
            raw_data = {
                'name': title,
                'current_price': price,
                'market_price': price,
                'brand': brand,
                'image_url': img_url,
                'search_keyword': keyword,
                'specs': specs,
                'zol_id': product_id,
                'rating': rating,
                'review_count': review_count
            }
            
            return RawProduct(
                source=self.source_name,
                product_id=product_id,
                raw_data=raw_data,
                url=product_url,
                timestamp=datetime.now()
            )
            
        except Exception as e:
            self.logger.warning(f"Failed to extract product from element: {str(e)}")
            return None
    
    def _extract_brand_from_title(self, title: str) -> str:
        """
        Extract brand name from product title.
        
        Args:
            title: Product title
            
        Returns:
            Brand name or empty string
        """
        # Common memory brands
        brands = [
            'CORSAIR', '海盗船',
            'KINGSTON', '金士顿',
            'G.SKILL', '芝奇',
            'CRUCIAL', '英睿达',
            'SAMSUNG', '三星',
            'SK HYNIX', 'SK海力士',
            'ADATA', '威刚',
            'TEAMGROUP', '十铨',
            'MUSHKIN',
            'PATRIOT', '博帝'
        ]
        
        title_upper = title.upper()
        for brand in brands:
            if brand.upper() in title_upper:
                return brand
        
        # Try to extract first word as brand
        words = title.split()
        if words:
            return words[0]
        
        return ''
    
    def parse_product(self, raw_data: Any) -> RawProduct:
        """
        Parse individual product data from raw response.
        
        Args:
            raw_data: Raw product data (should be RawProduct)
            
        Returns:
            Parsed raw product
            
        Raises:
            CrawlerError: If parsing fails
        """
        if isinstance(raw_data, RawProduct):
            return raw_data
        else:
            raise CrawlerError(
                "Invalid raw data type for Playwright ZOL crawler",
                {"expected": "RawProduct", "received": type(raw_data).__name__}
            )
    
    def get_price_history(self, product_id: str) -> List[PricePoint]:
        """
        Get price history for a specific product using Playwright.
        
        Args:
            product_id: Product identifier
            
        Returns:
            List of historical price points
        """
        price_points = []
        page = None
        
        try:
            page = self._create_stealth_page()
            
            # Build product URL
            product_url = f"https://detail.zol.com.cn/{product_id}.html"
            
            # Navigate to product page
            page.goto(product_url, wait_until='networkidle', timeout=self.page_timeout)
            
            # Look for price history data
            # This would need to be implemented based on ZOL's specific price history format
            self.logger.info(f"Price history extraction not yet implemented for ZOL product {product_id}")
            
        except Exception as e:
            self.logger.warning(f"Failed to get price history for product {product_id}: {str(e)}")
        finally:
            if page:
                page.close()
        
        return price_points
    
    def cleanup(self) -> None:
        """Clean up Playwright resources."""
        try:
            if self.context:
                self.context.close()
                self.context = None
            
            if self.browser:
                self.browser.close()
                self.browser = None
            
            if self.playwright:
                self.playwright.stop()
                self.playwright = None
            
            self.logger.info("Playwright resources cleaned up")
            
        except Exception as e:
            self.logger.warning(f"Error during cleanup: {str(e)}")
    
    def validate_config(self) -> bool:
        """
        Validate Playwright ZOL crawler configuration.
        
        Returns:
            True if configuration is valid
            
        Raises:
            CrawlerError: If configuration is invalid
        """
        errors = []
        
        # Validate browser type
        valid_browsers = ['chromium', 'firefox', 'webkit']
        if self.browser_type not in valid_browsers:
            errors.append(f"browser_type must be one of {valid_browsers}")
        
        # Validate viewport dimensions
        if self.viewport_width <= 0:
            errors.append("viewport_width must be positive")
        
        if self.viewport_height <= 0:
            errors.append("viewport_height must be positive")
        
        # Validate timeout
        if self.page_timeout <= 0:
            errors.append("page_timeout must be positive")
        
        # Validate search keywords
        if not self.search_keywords or not isinstance(self.search_keywords, list):
            errors.append("search_keywords must be a non-empty list")
        
        # Validate pagination settings
        if self.max_pages <= 0:
            errors.append("max_pages must be positive")
        
        # Validate delay settings
        if self.min_delay < 0:
            errors.append("min_delay must be non-negative")
        
        if self.max_delay < self.min_delay:
            errors.append("max_delay must be >= min_delay")
        
        if errors:
            raise CrawlerError(
                "Playwright ZOL crawler configuration is invalid",
                {"errors": errors}
            )
        
        return True
    
    def __del__(self):
        """Destructor to ensure cleanup."""
        self.cleanup()
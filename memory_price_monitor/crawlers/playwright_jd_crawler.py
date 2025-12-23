#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Playwright-based JD (京东) crawler implementation for memory products.
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


logger = get_business_logger('crawler_jd')


class PlaywrightJDCrawler(BaseCrawler):
    """Playwright-based JD (京东) crawler for memory products."""
    
    def __init__(self, source_name: str = 'jd_playwright', config: Optional[Dict[str, Any]] = None):
        """
        Initialize Playwright JD crawler.
        
        Args:
            source_name: Source name (default: 'jd_playwright')
            config: Crawler configuration
        """
        super().__init__(source_name, config)
        
        # Playwright-specific configuration
        self.headless = self.config.get('headless', True)
        self.browser_type = self.config.get('browser_type', 'chromium')
        self.viewport_width = self.config.get('viewport_width', 1920)
        self.viewport_height = self.config.get('viewport_height', 1080)
        self.page_timeout = self.config.get('page_timeout', 30000)  # 30 seconds
        
        # JD-specific configuration
        self.base_url = self.config.get('base_url', 'https://www.jd.com')
        self.search_url = self.config.get('search_url', 'https://search.jd.com/Search')
        self.mobile_search_url = self.config.get('mobile_search_url', 'https://so.m.jd.com/ware/search.action')
        self.category_url = self.config.get('category_url', 'https://list.jd.com/list.html')
        
        # Search parameters for memory products
        self.search_keywords = self.config.get('search_keywords', [
            '内存条', 'DDR4内存', 'DDR5内存'
        ])
        
        # Pagination settings
        self.max_pages = self.config.get('max_pages', 2)  # Reduced for testing
        self.products_per_page = self.config.get('products_per_page', 20)
        
        # Anti-crawler settings
        self.min_delay = self.config.get('min_delay', 3.0)
        self.max_delay = self.config.get('max_delay', 8.0)
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
            
            // Override screen properties
            Object.defineProperty(screen, 'colorDepth', {
                get: () => 24
            });
        """)
        
        return page
    
    def fetch_products(self) -> List[RawProduct]:
        """
        Fetch product list from JD search results using Playwright.
        
        Returns:
            List of raw product data
            
        Raises:
            CrawlerError: If fetching fails
        """
        all_products = []
        
        try:
            self._init_browser()
            
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
                f"Failed to fetch products from JD using Playwright",
                {"error": str(e), "keywords": self.search_keywords}
            )
    
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
            
            # Strategy 1: Try category page first (no login required)
            self.logger.info(f"Trying category page approach for keyword: {keyword}")
            category_products = self._try_category_page(page, keyword)
            if category_products:
                products.extend(category_products)
                self.logger.info(f"Found {len(category_products)} products from category page")
                return products
            
            # Strategy 2: Try mobile search (usually less restrictive)
            self.logger.info(f"Trying mobile search for keyword: {keyword}")
            mobile_products = self._try_mobile_search(page, keyword)
            if mobile_products:
                products.extend(mobile_products)
                self.logger.info(f"Found {len(mobile_products)} products from mobile search")
                return products
            
            # Strategy 3: Try direct product listing page
            self.logger.info(f"Trying direct listing page for keyword: {keyword}")
            listing_products = self._try_listing_page(page, keyword)
            if listing_products:
                products.extend(listing_products)
                self.logger.info(f"Found {len(listing_products)} products from listing page")
                return products
            
            self.logger.warning(f"All strategies failed for keyword: {keyword}")
            
        except Exception as e:
            self.logger.error(f"Search failed for keyword {keyword}: {str(e)}")
        finally:
            if page:
                page.close()
        
        return products
    
    def _try_category_page(self, page: Page, keyword: str) -> List[RawProduct]:
        """
        Try to fetch products from JD category page (no login required).
        
        Args:
            page: Playwright page object
            keyword: Search keyword
            
        Returns:
            List of raw products
        """
        products = []
        
        try:
            # JD memory category page
            # cat=670,671,672 is the memory category
            category_url = f"{self.category_url}?cat=670,671,672&sort=sort_totalsales15_desc&trans=1&JL=6_0_0"
            
            self.logger.debug(f"Navigating to category page: {category_url}")
            page.goto(category_url, wait_until='networkidle', timeout=self.page_timeout)
            
            # Check if we need login
            current_url = page.url
            if 'passport.jd.com' in current_url or 'login' in current_url.lower():
                self.logger.warning("Category page requires login")
                return products
            
            # Wait for products to load
            try:
                page.wait_for_selector('.gl-item', timeout=10000)
            except Exception:
                self.logger.warning("No products found on category page")
                return products
            
            # Simulate human behavior
            self._simulate_human_behavior(page)
            
            # Extract products
            products = self._extract_products_from_page(page, keyword)
            
        except Exception as e:
            self.logger.warning(f"Category page approach failed: {str(e)}")
        
        return products
    
    def _try_mobile_search(self, page: Page, keyword: str) -> List[RawProduct]:
        """
        Try to fetch products from JD mobile search (usually less restrictive).
        
        Args:
            page: Playwright page object
            keyword: Search keyword
            
        Returns:
            List of raw products
        """
        products = []
        
        try:
            # Use mobile search URL
            mobile_url = f"{self.mobile_search_url}?keyword={keyword}"
            
            self.logger.debug(f"Navigating to mobile search: {mobile_url}")
            
            # Set mobile user agent
            page.set_extra_http_headers({
                'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1'
            })
            
            page.goto(mobile_url, wait_until='networkidle', timeout=self.page_timeout)
            
            # Check if we need login
            current_url = page.url
            if 'passport.jd.com' in current_url or 'login' in current_url.lower():
                self.logger.warning("Mobile search requires login")
                return products
            
            # Wait for products to load (mobile selectors)
            try:
                page.wait_for_selector('.goods_item, .ware-item, .product-item', timeout=10000)
            except Exception:
                self.logger.warning("No products found on mobile search")
                return products
            
            # Extract products from mobile page
            products = self._extract_mobile_products(page, keyword)
            
        except Exception as e:
            self.logger.warning(f"Mobile search approach failed: {str(e)}")
        
        return products
    
    def _try_listing_page(self, page: Page, keyword: str) -> List[RawProduct]:
        """
        Try to fetch products from JD listing page.
        
        Args:
            page: Playwright page object
            keyword: Search keyword
            
        Returns:
            List of raw products
        """
        products = []
        
        try:
            # Try different listing URLs
            listing_urls = [
                "https://list.jd.com/list.html?cat=670,671,672",  # Memory category
                "https://channel.jd.com/670-671-672.html",  # Channel page
            ]
            
            for listing_url in listing_urls:
                self.logger.debug(f"Trying listing URL: {listing_url}")
                
                try:
                    page.goto(listing_url, wait_until='networkidle', timeout=self.page_timeout)
                    
                    # Check if we need login
                    current_url = page.url
                    if 'passport.jd.com' in current_url or 'login' in current_url.lower():
                        self.logger.warning(f"Listing page requires login: {listing_url}")
                        continue
                    
                    # Wait for products
                    try:
                        page.wait_for_selector('.gl-item', timeout=10000)
                    except Exception:
                        continue
                    
                    # Simulate human behavior
                    self._simulate_human_behavior(page)
                    
                    # Extract products
                    products = self._extract_products_from_page(page, keyword)
                    
                    if products:
                        self.logger.info(f"Found {len(products)} products from listing: {listing_url}")
                        break
                        
                except Exception as e:
                    self.logger.warning(f"Failed to load listing URL {listing_url}: {str(e)}")
                    continue
            
        except Exception as e:
            self.logger.warning(f"Listing page approach failed: {str(e)}")
        
        return products
    
    def _extract_mobile_products(self, page: Page, keyword: str) -> List[RawProduct]:
        """
        Extract products from mobile page.
        
        Args:
            page: Playwright page object
            keyword: Search keyword
            
        Returns:
            List of raw products
        """
        products = []
        
        try:
            # Try different mobile selectors
            mobile_selectors = ['.goods_item', '.ware-item', '.product-item', '.item']
            
            product_items = []
            for selector in mobile_selectors:
                items = page.locator(selector).all()
                if items:
                    product_items = items
                    self.logger.debug(f"Found {len(items)} items using mobile selector: {selector}")
                    break
            
            if not product_items:
                return products
            
            for item in product_items:
                try:
                    product = self._extract_mobile_product_from_element(item, keyword)
                    if product:
                        products.append(product)
                except Exception as e:
                    self.logger.warning(f"Failed to extract mobile product: {str(e)}")
                    continue
            
        except Exception as e:
            self.logger.error(f"Failed to extract mobile products: {str(e)}")
        
        return products
    
    def _extract_mobile_product_from_element(self, item, keyword: str) -> Optional[RawProduct]:
        """
        Extract product data from a mobile product element.
        
        Args:
            item: Playwright locator for product item
            keyword: Search keyword
            
        Returns:
            Raw product data or None if extraction fails
        """
        try:
            # Extract product ID from data attributes or links
            product_id = None
            
            # Try data-sku
            product_id = item.get_attribute('data-sku') or item.get_attribute('data-id')
            
            # Try to find from link
            if not product_id:
                link_element = item.locator('a[href*="item.jd.com"], a[href*="item.m.jd.com"]').first
                if link_element.count() > 0:
                    href = link_element.get_attribute('href')
                    if href:
                        import re
                        id_match = re.search(r'/(\d+)\.html', href)
                        if id_match:
                            product_id = id_match.group(1)
            
            if not product_id:
                return None
            
            # Extract title
            title_selectors = ['.goods_name', '.ware-name', '.title', 'h3', 'h4']
            title = ''
            
            for selector in title_selectors:
                title_element = item.locator(selector).first
                if title_element.count() > 0:
                    title = title_element.inner_text()
                    if title and len(title.strip()) > 5:
                        break
            
            if not title:
                return None
            
            # Extract price
            price_selectors = ['.goods_price', '.price', '.p-price', '[class*="price"]']
            price = 0.0
            
            for selector in price_selectors:
                price_element = item.locator(selector).first
                if price_element.count() > 0:
                    price_text = price_element.inner_text()
                    import re
                    price_match = re.search(r'[\d,]+\.?\d*', price_text.replace('¥', '').replace(',', ''))
                    if price_match:
                        try:
                            price = float(price_match.group())
                            break
                        except ValueError:
                            continue
            
            # Build product URL
            product_url = f"https://item.jd.com/{product_id}.html"
            
            # Extract brand from title
            brand = self._extract_brand_from_title(title)
            
            raw_data = {
                'title': title.strip(),
                'price': price,
                'original_price': price,
                'brand': brand,
                'search_keyword': keyword
            }
            
            return RawProduct(
                source=self.source_name,
                product_id=product_id,
                raw_data=raw_data,
                url=product_url,
                timestamp=datetime.now()
            )
            
        except Exception as e:
            self.logger.warning(f"Failed to extract mobile product from element: {str(e)}")
            return None
    
    def _handle_verification_page(self, page: Page) -> bool:
        """
        Handle JD verification/risk handler page.
        
        Args:
            page: Playwright page object
            
        Returns:
            True if verification was handled successfully
        """
        try:
            self.logger.info("Attempting to handle verification page")
            
            # Wait a bit for the page to fully load
            time.sleep(3)
            
            # Look for common verification elements
            verification_selectors = [
                'button[type="submit"]',
                '.verify-button',
                '#verify-btn',
                '.btn-verify'
            ]
            
            for selector in verification_selectors:
                try:
                    if page.locator(selector).is_visible():
                        self.logger.info(f"Found verification button: {selector}")
                        page.click(selector)
                        time.sleep(2)
                        return True
                except Exception:
                    continue
            
            # If no button found, try to wait and see if it auto-resolves
            time.sleep(5)
            
            # Check if we're still on verification page
            current_url = page.url
            if 'risk_handler' not in current_url and 'verification' not in current_url.lower():
                self.logger.info("Verification page resolved automatically")
                return True
            
            self.logger.warning("Could not handle verification page")
            return False
            
        except Exception as e:
            self.logger.error(f"Error handling verification page: {str(e)}")
            return False
    
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
            # Get all product items
            product_items = page.locator('.gl-item').all()
            
            self.logger.debug(f"Found {len(product_items)} product items on page")
            
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
            # Extract product ID
            product_id = item.get_attribute('data-sku')
            if not product_id:
                return None
            
            # Extract title
            title_element = item.locator('.p-name a')
            title = title_element.inner_text() if title_element.count() > 0 else ''
            
            # Extract price
            price_element = item.locator('.p-price i')
            price_text = price_element.inner_text() if price_element.count() > 0 else '0'
            
            try:
                price = float(re.sub(r'[^\d.]', '', price_text))
            except (ValueError, TypeError):
                price = 0.0
            
            # Extract image URL
            img_element = item.locator('.p-img img')
            img_src = img_element.get_attribute('src') if img_element.count() > 0 else ''
            if img_src and not img_src.startswith('http'):
                img_src = f"https:{img_src}" if img_src.startswith('//') else f"https://www.jd.com{img_src}"
            
            # Extract shop name
            shop_element = item.locator('.p-shop')
            shop_name = shop_element.inner_text() if shop_element.count() > 0 else ''
            
            # Build product URL
            product_url = f"https://item.jd.com/{product_id}.html"
            
            # Extract brand from title
            brand = self._extract_brand_from_title(title)
            
            raw_data = {
                'title': title.strip(),
                'price': price,
                'original_price': price,
                'brand': brand,
                'image_url': img_src,
                'shop_name': shop_name.strip(),
                'search_keyword': keyword
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
                "Invalid raw data type for Playwright JD crawler",
                {"expected": "RawProduct", "received": type(raw_data).__name__}
            )
    
    def get_price_history(self, product_id: str) -> List[PricePoint]:
        """
        Get price history for a specific product.
        
        Note: JD doesn't typically provide public price history API,
        so this returns empty list.
        
        Args:
            product_id: Product identifier
            
        Returns:
            Empty list (JD doesn't provide price history)
        """
        self.logger.info(f"Price history not available from JD for product {product_id}")
        return []
    
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
        Validate Playwright JD crawler configuration.
        
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
                "Playwright JD crawler configuration is invalid",
                {"errors": errors}
            )
        
        return True
    
    def __del__(self):
        """Destructor to ensure cleanup."""
        self.cleanup()
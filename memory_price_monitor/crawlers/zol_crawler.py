"""
ZOL (中关村在线) crawler implementation for memory products.
"""

import re
import json
import time
import random
from typing import List, Dict, Any, Optional
from datetime import datetime
from decimal import Decimal
from urllib.parse import urljoin, urlparse, parse_qs

from memory_price_monitor.crawlers.base import BaseCrawler, RawProduct, PricePoint
from memory_price_monitor.utils.errors import CrawlerError
from memory_price_monitor.utils.logging import get_business_logger


logger = get_business_logger('crawler_zol')


class ZOLCrawler(BaseCrawler):
    """ZOL (中关村在线) crawler for memory products."""
    
    def __init__(self, source_name: str = 'zol', config: Optional[Dict[str, Any]] = None):
        """
        Initialize ZOL crawler.
        
        Args:
            source_name: Source name (default: 'zol')
            config: Crawler configuration
        """
        super().__init__(source_name, config)
        
        # ZOL-specific configuration
        self.base_url = self.config.get('base_url', 'https://www.zol.com.cn')
        # 更新为正确的搜索URL
        self.search_url = self.config.get('search_url', 'https://search.zol.com.cn/s/all.php')
        self.product_search_url = self.config.get('product_search_url', 'https://search.zol.com.cn/s/product.php')
        self.product_base_url = self.config.get('product_base_url', 'https://detail.zol.com.cn')
        
        # Search parameters for memory products
        self.search_keywords = self.config.get('search_keywords', [
            '内存条', 'DDR4内存', 'DDR5内存', '台式机内存', '笔记本内存'
        ])
        
        # Category settings for memory products
        self.memory_category = self.config.get('memory_category', 'memory')
        
        # Pagination settings
        self.max_pages = self.config.get('max_pages', 10)
        self.products_per_page = self.config.get('products_per_page', 20)
        
        # Anti-crawler settings
        self.min_delay = self.config.get('min_delay', 2.0)
        self.max_delay = self.config.get('max_delay', 5.0)
        
        # ZOL-specific headers
        self.zol_headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        }
    
    def fetch_products(self) -> List[RawProduct]:
        """
        Fetch product list from ZOL search results.
        
        Returns:
            List of raw product data
            
        Raises:
            CrawlerError: If fetching fails
        """
        all_products = []
        
        try:
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
                f"Failed to fetch products from ZOL",
                {"error": str(e), "keywords": self.search_keywords}
            )
    
    def _search_products(self, keyword: str) -> List[RawProduct]:
        """
        Search for products with a specific keyword.
        
        Args:
            keyword: Search keyword
            
        Returns:
            List of raw products for this keyword
        """
        products = []
        
        for page in range(1, self.max_pages + 1):
            try:
                self.logger.debug(f"Fetching page {page} for keyword: {keyword}")
                
                # 尝试使用产品搜索URL
                search_params = {
                    'keyword': keyword,  # 使用keyword而不是kword
                    'page': page,
                    'subcate': 'memory',  # 内存分类
                    'order': 'price_asc'  # 按价格排序
                }
                
                # 首先尝试产品搜索
                response = self.http_client.get(
                    self.product_search_url,
                    params=search_params,
                    headers=self.zol_headers
                )
                
                # 如果产品搜索失败，尝试综合搜索
                if response.status_code != 200 or len(response.text) < 1000:
                    self.logger.info(f"Product search failed, trying general search for: {keyword}")
                    
                    general_params = {
                        'keyword': keyword,
                        'page': page
                    }
                    
                    response = self.http_client.get(
                        self.search_url,
                        params=general_params,
                        headers=self.zol_headers
                    )
                
                if response.status_code != 200:
                    self.logger.warning(f"Search request failed with status {response.status_code}")
                    break
                
                # 检查是否被重定向到首页
                if 'search.zol.com.cn/s/' == response.url.rstrip('/'):
                    self.logger.warning(f"Redirected to homepage, trying alternative approach")
                    # 尝试直接访问内存分类页面
                    products.extend(self._try_memory_category_page(keyword))
                    break
                
                # Parse products from search results
                page_products = self._parse_search_results(response.text, keyword)
                
                if not page_products:
                    self.logger.info(f"No more products found on page {page}, stopping pagination")
                    break
                
                products.extend(page_products)
                self.logger.debug(f"Found {len(page_products)} products on page {page}")
                
                # Random delay between pages
                delay = random.uniform(self.min_delay, self.max_delay)
                time.sleep(delay)
                
            except Exception as e:
                self.logger.warning(f"Failed to fetch page {page} for keyword {keyword}: {str(e)}")
                break
        
        return products
    
    def _try_memory_category_page(self, keyword: str) -> List[RawProduct]:
        """
        尝试直接访问内存分类页面作为备用方案
        
        Args:
            keyword: Search keyword
            
        Returns:
            List of raw products
        """
        products = []
        
        try:
            # ZOL内存分类页面
            category_url = "https://detail.zol.com.cn/memory/"
            
            self.logger.info(f"Trying memory category page as fallback for keyword: {keyword}")
            
            response = self.http_client.get(
                category_url,
                headers=self.zol_headers
            )
            
            if response.status_code == 200:
                products = self._parse_search_results(response.text, keyword)
                self.logger.info(f"Category page fallback found {len(products)} products")
            
        except Exception as e:
            self.logger.warning(f"Memory category page fallback failed: {str(e)}")
        
        return products
    
    def _parse_search_results(self, html_content: str, keyword: str) -> List[RawProduct]:
        """
        Parse products from ZOL search results HTML.
        
        Args:
            html_content: HTML content from search page
            keyword: Search keyword used
            
        Returns:
            List of raw products
        """
        products = []
        
        try:
            # ZOL search results are in HTML format
            # Look for product items with specific CSS classes
            product_pattern = r'<li[^>]*class="[^"]*list-item[^"]*"[^>]*>(.*?)</li>'
            product_matches = re.findall(product_pattern, html_content, re.DOTALL)
            
            for match in product_matches:
                try:
                    product = self._extract_product_from_html(match, keyword)
                    if product:
                        products.append(product)
                except Exception as e:
                    self.logger.warning(f"Failed to extract product from HTML: {str(e)}")
            
            # Alternative pattern for different ZOL layouts
            if not products:
                alt_pattern = r'<div[^>]*class="[^"]*product-item[^"]*"[^>]*>(.*?)</div>'
                alt_matches = re.findall(alt_pattern, html_content, re.DOTALL)
                
                for match in alt_matches:
                    try:
                        product = self._extract_product_from_html(match, keyword)
                        if product:
                            products.append(product)
                    except Exception as e:
                        self.logger.warning(f"Failed to extract product from alternative HTML: {str(e)}")
            
        except Exception as e:
            self.logger.warning(f"Failed to parse search results: {str(e)}")
        
        return products
    
    def _extract_product_from_html(self, html_item: str, keyword: str) -> Optional[RawProduct]:
        """
        Extract product data from HTML item.
        
        Args:
            html_item: HTML content for single product
            keyword: Search keyword
            
        Returns:
            Raw product data or None if extraction fails
        """
        try:
            # Extract product URL and ID
            url_match = re.search(r'href="([^"]*detail\.zol\.com\.cn[^"]*)"', html_item)
            if not url_match:
                return None
            
            product_url = url_match.group(1)
            if not product_url.startswith('http'):
                product_url = urljoin(self.base_url, product_url)
            
            # Extract product ID from URL
            id_match = re.search(r'/(\d+)\.html', product_url)
            if not id_match:
                # Try alternative ID extraction
                id_match = re.search(r'product_id[=:](\d+)', html_item)
            
            if not id_match:
                return None
            
            product_id = id_match.group(1)
            
            # Extract title
            title_patterns = [
                r'<h3[^>]*>(.*?)</h3>',
                r'<a[^>]*title="([^"]*)"[^>]*>',
                r'alt="([^"]*)"'
            ]
            
            title = ''
            for pattern in title_patterns:
                title_match = re.search(pattern, html_item, re.DOTALL)
                if title_match:
                    title = title_match.group(1)
                    title = re.sub(r'<[^>]+>', '', title).strip()  # Remove HTML tags
                    break
            
            if not title:
                return None
            
            # Extract price
            price_patterns = [
                r'¥([0-9,.]+)',
                r'price["\']:\s*["\']?([0-9,.]+)',
                r'<span[^>]*class="[^"]*price[^"]*"[^>]*>¥?([0-9,.]+)</span>'
            ]
            
            current_price = 0.0
            for pattern in price_patterns:
                price_match = re.search(pattern, html_item)
                if price_match:
                    current_price = float(price_match.group(1).replace(',', ''))
                    break
            
            # Extract brand from title
            brand = self._extract_brand_from_title(title)
            
            # Extract specifications if available
            specs = self._extract_specs_from_html(html_item)
            
            raw_data = {
                'name': title,
                'current_price': current_price,
                'market_price': current_price,  # Will be updated if we can get market price
                'brand': brand,
                'search_keyword': keyword,
                'specs': specs
            }
            
            return RawProduct(
                source=self.source_name,
                product_id=product_id,
                raw_data=raw_data,
                url=product_url,
                timestamp=datetime.now()
            )
            
        except Exception as e:
            self.logger.warning(f"Failed to extract product from HTML item: {str(e)}")
            return None
    
    def _extract_specs_from_html(self, html_item: str) -> Dict[str, str]:
        """
        Extract specifications from HTML item.
        
        Args:
            html_item: HTML content
            
        Returns:
            Dictionary of specifications
        """
        specs = {}
        
        try:
            # Look for specification patterns
            spec_patterns = [
                r'容量[：:]?\s*([^<\s]+)',
                r'频率[：:]?\s*([^<\s]+)',
                r'类型[：:]?\s*([^<\s]+)',
                r'品牌[：:]?\s*([^<\s]+)'
            ]
            
            spec_names = ['capacity', 'frequency', 'type', 'brand']
            
            for i, pattern in enumerate(spec_patterns):
                match = re.search(pattern, html_item)
                if match:
                    specs[spec_names[i]] = match.group(1).strip()
            
        except Exception as e:
            self.logger.warning(f"Failed to extract specs: {str(e)}")
        
        return specs
    
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
            Parsed raw product with enhanced data
            
        Raises:
            CrawlerError: If parsing fails
        """
        if isinstance(raw_data, RawProduct):
            # Already parsed, but we can enhance with additional data
            try:
                enhanced_data = self._enhance_product_data(raw_data)
                return enhanced_data
            except Exception as e:
                self.logger.warning(f"Failed to enhance product data: {str(e)}")
                return raw_data
        else:
            raise CrawlerError(
                "Invalid raw data type for ZOL crawler",
                {"expected": "RawProduct", "received": type(raw_data).__name__}
            )
    
    def _enhance_product_data(self, raw_product: RawProduct) -> RawProduct:
        """
        Enhance product data by fetching additional details from product page.
        
        Args:
            raw_product: Basic product data
            
        Returns:
            Enhanced product data
        """
        try:
            # Fetch product detail page
            response = self.http_client.get(
                raw_product.url,
                headers=self.zol_headers
            )
            
            if response.status_code != 200:
                self.logger.warning(f"Failed to fetch product details for {raw_product.product_id}")
                return raw_product
            
            # Parse additional details from product page
            enhanced_data = raw_product.raw_data.copy()
            
            # Extract detailed specifications
            detailed_specs = self._extract_detailed_specifications(response.text)
            enhanced_data['specs'].update(detailed_specs)
            
            # Extract market price if available
            market_price = self._extract_market_price(response.text)
            if market_price:
                enhanced_data['market_price'] = market_price
            
            # Extract price history if available
            price_history = self._extract_price_history_from_page(response.text)
            if price_history:
                enhanced_data['price_history'] = price_history
            
            # Create enhanced raw product
            return RawProduct(
                source=raw_product.source,
                product_id=raw_product.product_id,
                raw_data=enhanced_data,
                url=raw_product.url,
                timestamp=raw_product.timestamp
            )
            
        except Exception as e:
            self.logger.warning(f"Failed to enhance product {raw_product.product_id}: {str(e)}")
            return raw_product
    
    def _extract_detailed_specifications(self, html_content: str) -> Dict[str, str]:
        """
        Extract detailed product specifications from product page.
        
        Args:
            html_content: Product page HTML
            
        Returns:
            Dictionary of detailed specifications
        """
        specs = {}
        
        try:
            # Look for specification table
            table_patterns = [
                r'<table[^>]*class="[^"]*param[^"]*"[^>]*>(.*?)</table>',
                r'<div[^>]*class="[^"]*spec[^"]*"[^>]*>(.*?)</div>'
            ]
            
            for table_pattern in table_patterns:
                table_match = re.search(table_pattern, html_content, re.DOTALL)
                if table_match:
                    # Parse specification rows
                    row_pattern = r'<tr[^>]*>(.*?)</tr>'
                    rows = re.findall(row_pattern, table_match.group(1), re.DOTALL)
                    
                    for row in rows:
                        # Extract key-value pairs
                        cell_pattern = r'<td[^>]*>(.*?)</td>'
                        cells = re.findall(cell_pattern, row, re.DOTALL)
                        
                        if len(cells) >= 2:
                            key = re.sub(r'<[^>]+>', '', cells[0]).strip()
                            value = re.sub(r'<[^>]+>', '', cells[1]).strip()
                            if key and value:
                                specs[key] = value
                    
                    if specs:
                        break
            
            # Look for JSON specification data
            json_pattern = r'var\s+productSpec\s*=\s*({.*?});'
            json_match = re.search(json_pattern, html_content, re.DOTALL)
            
            if json_match:
                try:
                    spec_data = json.loads(json_match.group(1))
                    specs.update(spec_data)
                except json.JSONDecodeError:
                    pass
            
        except Exception as e:
            self.logger.warning(f"Failed to extract detailed specifications: {str(e)}")
        
        return specs
    
    def _extract_market_price(self, html_content: str) -> Optional[float]:
        """
        Extract market price from product page.
        
        Args:
            html_content: Product page HTML
            
        Returns:
            Market price or None if not found
        """
        try:
            # Look for market price patterns
            patterns = [
                r'市场价[：:]?\s*¥?([0-9,.]+)',
                r'参考价[：:]?\s*¥?([0-9,.]+)',
                r'<del[^>]*>¥?([0-9,.]+)</del>',
                r'market[Pp]rice["\']:\s*["\']?([0-9,.]+)'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, html_content)
                if match:
                    return float(match.group(1).replace(',', ''))
            
        except Exception as e:
            self.logger.warning(f"Failed to extract market price: {str(e)}")
        
        return None
    
    def _extract_price_history_from_page(self, html_content: str) -> Optional[List[Dict[str, Any]]]:
        """
        Extract price history from product page if available.
        
        Args:
            html_content: Product page HTML
            
        Returns:
            List of price history points or None if not available
        """
        try:
            # Look for price history data in JavaScript
            history_patterns = [
                r'priceHistory\s*[:=]\s*(\[.*?\])',
                r'var\s+priceData\s*=\s*(\[.*?\]);'
            ]
            
            for pattern in history_patterns:
                match = re.search(pattern, html_content, re.DOTALL)
                if match:
                    try:
                        history_data = json.loads(match.group(1))
                        return history_data
                    except json.JSONDecodeError:
                        continue
            
        except Exception as e:
            self.logger.warning(f"Failed to extract price history: {str(e)}")
        
        return None
    
    def get_price_history(self, product_id: str) -> List[PricePoint]:
        """
        Get price history for a specific product.
        
        Args:
            product_id: Product identifier
            
        Returns:
            List of historical price points
        """
        try:
            # Build product URL
            product_url = f"{self.product_base_url}/{product_id}.html"
            
            # Fetch product page
            response = self.http_client.get(
                product_url,
                headers=self.zol_headers
            )
            
            if response.status_code != 200:
                self.logger.warning(f"Failed to fetch product page for {product_id}")
                return []
            
            # Extract price history from page
            history_data = self._extract_price_history_from_page(response.text)
            
            if not history_data:
                self.logger.info(f"No price history available for product {product_id}")
                return []
            
            # Convert to PricePoint objects
            price_points = []
            for item in history_data:
                try:
                    if isinstance(item, dict):
                        price = Decimal(str(item.get('price', 0)))
                        timestamp_str = item.get('date', item.get('timestamp', ''))
                        
                        # Parse timestamp
                        if timestamp_str:
                            timestamp = datetime.fromisoformat(timestamp_str)
                        else:
                            timestamp = datetime.now()
                        
                        price_point = PricePoint(
                            price=price,
                            timestamp=timestamp,
                            metadata=item
                        )
                        price_points.append(price_point)
                        
                except Exception as e:
                    self.logger.warning(f"Failed to parse price history item: {str(e)}")
            
            self.logger.info(f"Found {len(price_points)} price history points for product {product_id}")
            return price_points
            
        except Exception as e:
            self.logger.warning(f"Failed to get price history for product {product_id}: {str(e)}")
            return []
    
    def validate_config(self) -> bool:
        """
        Validate ZOL crawler configuration.
        
        Returns:
            True if configuration is valid
            
        Raises:
            CrawlerError: If configuration is invalid
        """
        errors = []
        
        # Validate URLs
        if not self.base_url.startswith(('http://', 'https://')):
            errors.append("base_url must be a valid URL")
        
        if not self.search_url.startswith(('http://', 'https://')):
            errors.append("search_url must be a valid URL")
        
        if not self.product_base_url.startswith(('http://', 'https://')):
            errors.append("product_base_url must be a valid URL")
        
        # Validate search keywords
        if not self.search_keywords or not isinstance(self.search_keywords, list):
            errors.append("search_keywords must be a non-empty list")
        
        # Validate pagination settings
        if self.max_pages <= 0:
            errors.append("max_pages must be positive")
        
        if self.products_per_page <= 0:
            errors.append("products_per_page must be positive")
        
        # Validate delay settings
        if self.min_delay < 0:
            errors.append("min_delay must be non-negative")
        
        if self.max_delay < self.min_delay:
            errors.append("max_delay must be >= min_delay")
        
        if errors:
            raise CrawlerError(
                "ZOL crawler configuration is invalid",
                {"errors": errors}
            )
        
        return True
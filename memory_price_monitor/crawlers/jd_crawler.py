"""
JD (京东) crawler implementation for memory products.
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


logger = get_business_logger('crawler_jd')


class JDCrawler(BaseCrawler):
    """JD (京东) crawler for memory products."""
    
    def __init__(self, source_name: str = 'jd', config: Optional[Dict[str, Any]] = None):
        """
        Initialize JD crawler.
        
        Args:
            source_name: Source name (default: 'jd')
            config: Crawler configuration
        """
        super().__init__(source_name, config)
        
        # JD-specific configuration
        self.base_url = self.config.get('base_url', 'https://www.jd.com')
        # 使用移动端API，通常反爬虫较少
        self.search_url = self.config.get('search_url', 'https://api.m.jd.com/api')
        self.web_search_url = self.config.get('web_search_url', 'https://search.jd.com/Search')
        self.api_url = self.config.get('api_url', 'https://api.m.jd.com')
        
        # Search parameters for memory products
        self.search_keywords = self.config.get('search_keywords', [
            '内存条', 'DDR4内存', 'DDR5内存', '台式机内存', '笔记本内存'
        ])
        
        # Pagination settings
        self.max_pages = self.config.get('max_pages', 3)  # 减少页数避免触发反爬虫
        self.products_per_page = self.config.get('products_per_page', 20)
        
        # Anti-crawler settings - 增加延迟
        self.min_delay = self.config.get('min_delay', 3.0)
        self.max_delay = self.config.get('max_delay', 8.0)
        
        # JD-specific headers - 模拟真实浏览器
        self.jd_headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"'
        }
    
    def fetch_products(self) -> List[RawProduct]:
        """
        Fetch product list from JD search results.
        
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
                f"Failed to fetch products from JD",
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
                
                # 尝试使用PC端搜索，但使用更好的参数
                search_params = {
                    'keyword': keyword,
                    'page': page,
                    'sort': 'sort_totalsales15_desc',  # Sort by sales
                    'trans': '1',
                    'JL': '6_0_0',  # Memory category filter
                    'ev': '1',  # 添加事件参数
                    'stock': '1'  # 只显示有库存的
                }
                
                # 使用PC端搜索URL
                response = self.http_client.get(
                    self.web_search_url,
                    params=search_params,
                    headers=self.jd_headers
                )
                
                if response.status_code != 200:
                    self.logger.warning(f"Search request failed with status {response.status_code}")
                    break
                
                # 检查是否被重定向到风险检测页面
                if 'risk_handler' in response.url or 'verification' in response.url.lower():
                    self.logger.warning(f"Detected anti-crawler redirect: {response.url}")
                    # 尝试备用方案：直接访问分类页面
                    products.extend(self._try_category_page(keyword))
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
    
    def _try_category_page(self, keyword: str) -> List[RawProduct]:
        """
        尝试直接访问内存分类页面作为备用方案
        
        Args:
            keyword: Search keyword
            
        Returns:
            List of raw products
        """
        products = []
        
        try:
            # 京东内存分类页面
            category_url = "https://list.jd.com/list.html"
            category_params = {
                'cat': '670,671,672',  # 内存分类ID
                'sort': 'sort_totalsales15_desc',
                'trans': '1',
                'JL': '6_0_0'
            }
            
            self.logger.info(f"Trying category page as fallback for keyword: {keyword}")
            
            response = self.http_client.get(
                category_url,
                params=category_params,
                headers=self.jd_headers
            )
            
            if response.status_code == 200 and 'risk_handler' not in response.url:
                products = self._parse_search_results(response.text, keyword)
                self.logger.info(f"Category page fallback found {len(products)} products")
            
        except Exception as e:
            self.logger.warning(f"Category page fallback failed: {str(e)}")
        
        return products
    
    def _parse_search_results(self, html_content: str, keyword: str) -> List[RawProduct]:
        """
        Parse products from JD search results HTML.
        
        Args:
            html_content: HTML content from search page
            keyword: Search keyword used
            
        Returns:
            List of raw products
        """
        products = []
        
        try:
            # JD search results are often loaded via JavaScript
            # Look for JSON data in script tags
            json_pattern = r'window\.searchData\s*=\s*({.*?});'
            json_match = re.search(json_pattern, html_content, re.DOTALL)
            
            if json_match:
                # Parse JSON data
                search_data = json.loads(json_match.group(1))
                product_list = search_data.get('resultList', [])
                
                for item in product_list:
                    try:
                        product = self._extract_product_from_json(item, keyword)
                        if product:
                            products.append(product)
                    except Exception as e:
                        self.logger.warning(f"Failed to extract product from JSON: {str(e)}")
            
            else:
                # Fallback: parse HTML directly
                products = self._parse_html_search_results(html_content, keyword)
            
        except Exception as e:
            self.logger.warning(f"Failed to parse search results: {str(e)}")
        
        return products
    
    def _extract_product_from_json(self, item: Dict[str, Any], keyword: str) -> Optional[RawProduct]:
        """
        Extract product data from JSON item.
        
        Args:
            item: JSON product item
            keyword: Search keyword
            
        Returns:
            Raw product data or None if extraction fails
        """
        try:
            # Extract basic product information
            product_id = str(item.get('skuId', ''))
            if not product_id:
                return None
            
            title = item.get('skuName', '')
            price = item.get('price', 0)
            
            # Build product URL
            product_url = f"https://item.jd.com/{product_id}.html"
            
            # Extract additional data
            brand = self._extract_brand_from_title(title)
            image_url = item.get('imageUrl', '')
            
            raw_data = {
                'title': title,
                'price': float(price) if price else 0.0,
                'original_price': float(item.get('priceWap', price)) if item.get('priceWap') else float(price),
                'brand': brand,
                'image_url': image_url,
                'shop_name': item.get('shopName', ''),
                'comment_count': item.get('commentCount', 0),
                'good_rate': item.get('goodRate', 0),
                'search_keyword': keyword,
                'category': item.get('categoryName', ''),
                'tags': item.get('tags', [])
            }
            
            return RawProduct(
                source=self.source_name,
                product_id=product_id,
                raw_data=raw_data,
                url=product_url,
                timestamp=datetime.now()
            )
            
        except Exception as e:
            self.logger.warning(f"Failed to extract product from JSON item: {str(e)}")
            return None
    
    def _parse_html_search_results(self, html_content: str, keyword: str) -> List[RawProduct]:
        """
        Parse products from HTML search results (fallback method).
        
        Args:
            html_content: HTML content
            keyword: Search keyword
            
        Returns:
            List of raw products
        """
        products = []
        
        try:
            # Use regex to find product items in HTML
            # JD uses specific CSS classes for product items
            product_pattern = r'<li[^>]*class="[^"]*gl-item[^"]*"[^>]*>(.*?)</li>'
            product_matches = re.findall(product_pattern, html_content, re.DOTALL)
            
            for match in product_matches:
                try:
                    product = self._extract_product_from_html(match, keyword)
                    if product:
                        products.append(product)
                except Exception as e:
                    self.logger.warning(f"Failed to extract product from HTML: {str(e)}")
            
        except Exception as e:
            self.logger.warning(f"Failed to parse HTML search results: {str(e)}")
        
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
            # Extract product ID from data-sku attribute
            sku_match = re.search(r'data-sku="(\d+)"', html_item)
            if not sku_match:
                return None
            
            product_id = sku_match.group(1)
            
            # Extract title
            title_match = re.search(r'<em[^>]*>(.*?)</em>', html_item, re.DOTALL)
            title = title_match.group(1) if title_match else ''
            title = re.sub(r'<[^>]+>', '', title).strip()  # Remove HTML tags
            
            # Extract price
            price_match = re.search(r'<i[^>]*>¥?([0-9,.]+)</i>', html_item)
            price = float(price_match.group(1).replace(',', '')) if price_match else 0.0
            
            # Build product URL
            product_url = f"https://item.jd.com/{product_id}.html"
            
            # Extract brand from title
            brand = self._extract_brand_from_title(title)
            
            raw_data = {
                'title': title,
                'price': price,
                'original_price': price,  # Will be updated if we can get original price
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
            self.logger.warning(f"Failed to extract product from HTML item: {str(e)}")
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
                "Invalid raw data type for JD crawler",
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
                headers=self.jd_headers
            )
            
            if response.status_code != 200:
                self.logger.warning(f"Failed to fetch product details for {raw_product.product_id}")
                return raw_product
            
            # Parse additional details from product page
            enhanced_data = raw_product.raw_data.copy()
            
            # Extract specifications
            specs = self._extract_specifications(response.text)
            enhanced_data.update(specs)
            
            # Extract original price if available
            original_price = self._extract_original_price(response.text)
            if original_price:
                enhanced_data['original_price'] = original_price
            
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
    
    def _extract_specifications(self, html_content: str) -> Dict[str, Any]:
        """
        Extract product specifications from product page.
        
        Args:
            html_content: Product page HTML
            
        Returns:
            Dictionary of specifications
        """
        specs = {}
        
        try:
            # Look for specifications in various formats
            # JD often has specs in JSON format
            spec_pattern = r'window\.pageConfig\.product\s*=\s*({.*?});'
            spec_match = re.search(spec_pattern, html_content, re.DOTALL)
            
            if spec_match:
                try:
                    product_data = json.loads(spec_match.group(1))
                    specs.update(product_data)
                except json.JSONDecodeError:
                    pass
            
            # Look for specification table
            table_pattern = r'<table[^>]*class="[^"]*Ptable[^"]*"[^>]*>(.*?)</table>'
            table_match = re.search(table_pattern, html_content, re.DOTALL)
            
            if table_match:
                # Parse specification table
                row_pattern = r'<tr[^>]*>(.*?)</tr>'
                rows = re.findall(row_pattern, table_match.group(1), re.DOTALL)
                
                for row in rows:
                    cell_pattern = r'<td[^>]*>(.*?)</td>'
                    cells = re.findall(cell_pattern, row, re.DOTALL)
                    
                    if len(cells) >= 2:
                        key = re.sub(r'<[^>]+>', '', cells[0]).strip()
                        value = re.sub(r'<[^>]+>', '', cells[1]).strip()
                        if key and value:
                            specs[key] = value
            
        except Exception as e:
            self.logger.warning(f"Failed to extract specifications: {str(e)}")
        
        return specs
    
    def _extract_original_price(self, html_content: str) -> Optional[float]:
        """
        Extract original price from product page.
        
        Args:
            html_content: Product page HTML
            
        Returns:
            Original price or None if not found
        """
        try:
            # Look for original price patterns
            patterns = [
                r'<del[^>]*>¥?([0-9,.]+)</del>',
                r'原价[：:]?\s*¥?([0-9,.]+)',
                r'market[Pp]rice["\']:\s*["\']?([0-9,.]+)',
            ]
            
            for pattern in patterns:
                match = re.search(pattern, html_content)
                if match:
                    return float(match.group(1).replace(',', ''))
            
        except Exception as e:
            self.logger.warning(f"Failed to extract original price: {str(e)}")
        
        return None
    
    def get_price_history(self, product_id: str) -> List[PricePoint]:
        """
        Get price history for a specific product.
        
        Note: JD doesn't typically provide public price history API,
        so this returns empty list. Price history would need to be
        built from our own historical data collection.
        
        Args:
            product_id: Product identifier
            
        Returns:
            Empty list (JD doesn't provide price history)
        """
        self.logger.info(f"Price history not available from JD for product {product_id}")
        return []
    
    def validate_config(self) -> bool:
        """
        Validate JD crawler configuration.
        
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
                "JD crawler configuration is invalid",
                {"errors": errors}
            )
        
        return True
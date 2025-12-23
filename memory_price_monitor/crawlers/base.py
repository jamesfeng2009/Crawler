"""
Abstract base classes and interfaces for crawlers.
"""

import time
from abc import ABC, abstractmethod
from typing import List, Any, Optional, Dict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from memory_price_monitor.utils.logging import get_business_logger
from memory_price_monitor.utils.errors import CrawlerError
from memory_price_monitor.crawlers.http_client import HTTPClient, RateLimitConfig, RetryConfig


logger = get_business_logger('crawler_general')


@dataclass
class RawProduct:
    """Raw product data from crawler before standardization."""
    source: str
    product_id: str
    raw_data: Dict[str, Any]
    url: str
    timestamp: datetime


@dataclass
class PricePoint:
    """Single price point with timestamp."""
    price: Decimal
    timestamp: datetime
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class BackupStrategy:
    """Backup strategy configuration."""
    name: str
    enabled: bool = True
    config: Optional[Dict[str, Any]] = None


@dataclass
class CrawlResult:
    """Result of a crawling operation."""
    source: str
    success: bool
    products_found: int
    products_extracted: int
    errors: List[str]
    started_at: datetime
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[int] = None


class BaseCrawler(ABC):
    """Abstract base class for all crawlers."""
    
    def __init__(self, source_name: str, config: Optional[Dict[str, Any]] = None):
        """
        Initialize crawler with source name and configuration.
        
        Args:
            source_name: Name of the data source (e.g., 'jd', 'zol')
            config: Optional crawler-specific configuration
        """
        self.source_name = source_name
        self.config = config or {}
        self.logger = get_business_logger('crawler_general')
        
        # Backup strategy configuration
        self.backup_strategies = self._initialize_backup_strategies()
        self.failure_count = 0
        self.max_failures_before_backup = self.config.get('max_failures_before_backup', 3)
        self.active_backup_strategies = []
        
        # Initialize HTTP client with configuration
        self.http_client = self._create_http_client()
    
    def _initialize_backup_strategies(self) -> List[BackupStrategy]:
        """Initialize backup strategies from configuration."""
        strategies = []
        backup_config = self.config.get('backup_strategies', [])
        
        if isinstance(backup_config, list):
            for strategy_name in backup_config:
                if isinstance(strategy_name, str):
                    strategies.append(BackupStrategy(name=strategy_name))
                elif isinstance(strategy_name, dict):
                    name = strategy_name.get('name', '')
                    enabled = strategy_name.get('enabled', True)
                    config = strategy_name.get('config', {})
                    if name:
                        strategies.append(BackupStrategy(name=name, enabled=enabled, config=config))
        
        return strategies
    
    def _create_http_client(self) -> HTTPClient:
        """Create HTTP client with crawler-specific configuration."""
        # Extract rate limiting config
        rate_limit_config = RateLimitConfig()
        if 'rate_limit' in self.config:
            rate_config = self.config['rate_limit']
            rate_limit_config.requests_per_second = rate_config.get('requests_per_second', 1.0)
            rate_limit_config.requests_per_minute = rate_config.get('requests_per_minute', 60)
            rate_limit_config.requests_per_hour = rate_config.get('requests_per_hour', 3600)
            rate_limit_config.burst_size = rate_config.get('burst_size', 5)
            rate_limit_config.backoff_factor = rate_config.get('backoff_factor', 2.0)
            rate_limit_config.max_delay = rate_config.get('max_delay', 300.0)
        
        # Extract retry config
        retry_config = RetryConfig()
        if 'retry' in self.config:
            retry_cfg = self.config['retry']
            retry_config.max_attempts = retry_cfg.get('max_attempts', 3)
            retry_config.backoff_factor = retry_cfg.get('backoff_factor', 2.0)
            retry_config.initial_delay = retry_cfg.get('initial_delay', 1.0)
            retry_config.max_delay = retry_cfg.get('max_delay', 60.0)
            retry_config.retry_on_status = retry_cfg.get('retry_on_status', [429, 500, 502, 503, 504])
        
        # Get timeout
        timeout = self.config.get('timeout', 30.0)
        
        return HTTPClient(
            rate_limit_config=rate_limit_config,
            retry_config=retry_config,
            timeout=timeout
        )
    
    @abstractmethod
    def fetch_products(self) -> List[RawProduct]:
        """
        Fetch product list from the source.
        
        Returns:
            List of raw product data
            
        Raises:
            CrawlerError: If fetching fails
        """
        pass
    
    @abstractmethod
    def parse_product(self, raw_data: Any) -> RawProduct:
        """
        Parse individual product data from raw response.
        
        Args:
            raw_data: Raw product data from the source
            
        Returns:
            Parsed raw product
            
        Raises:
            CrawlerError: If parsing fails
        """
        pass
    
    @abstractmethod
    def get_price_history(self, product_id: str) -> List[PricePoint]:
        """
        Get price history for a specific product (if available).
        
        Args:
            product_id: Product identifier
            
        Returns:
            List of historical price points
            
        Raises:
            CrawlerError: If fetching price history fails
        """
        pass
    
    def execute(self) -> CrawlResult:
        """
        Execute the complete crawling workflow.
        
        This is a template method that orchestrates the crawling process.
        
        Returns:
            Crawling result with statistics and status
        """
        started_at = datetime.now()
        result = CrawlResult(
            source=self.source_name,
            success=False,
            products_found=0,
            products_extracted=0,
            errors=[],
            started_at=started_at
        )
        
        try:
            self.logger.info(f"Starting crawl for {self.source_name}")
            
            # Fetch products with backup strategy support
            raw_products = self._fetch_products_with_backup()
            result.products_found = len(raw_products)
            
            # Process each product
            extracted_products = []
            for raw_product in raw_products:
                try:
                    parsed_product = self.parse_product(raw_product)
                    extracted_products.append(parsed_product)
                except Exception as e:
                    error_msg = f"Failed to parse product {raw_product.product_id}: {str(e)}"
                    result.errors.append(error_msg)
                    self.logger.warning(f"Product parsing failed for {raw_product.product_id}: {str(e)}")
            
            result.products_extracted = len(extracted_products)
            result.success = True
            
            self.logger.info(f"Crawl completed successfully for {self.source_name}: "
                           f"found={result.products_found}, extracted={result.products_extracted}, "
                           f"errors={len(result.errors)}")
            
        except Exception as e:
            error_msg = f"Crawl failed: {str(e)}"
            result.errors.append(error_msg)
            self.logger.error(f"Crawl failed for {self.source_name}: {str(e)}")
            
        finally:
            result.completed_at = datetime.now()
            result.duration_seconds = int(
                (result.completed_at - result.started_at).total_seconds()
            )
        
        return result
    
    def _fetch_products_with_backup(self) -> List[RawProduct]:
        """
        Fetch products with backup strategy support.
        
        Returns:
            List of raw product data
            
        Raises:
            CrawlerError: If fetching fails even with backup strategies
        """
        last_exception = None
        
        for attempt in range(self.max_failures_before_backup + len(self.backup_strategies) + 1):
            try:
                # Reset failure count on successful attempt
                if attempt > 0:
                    self.failure_count = 0
                
                return self.fetch_products()
                
            except Exception as e:
                last_exception = e
                self.failure_count += 1
                
                self.logger.warning(f"Fetch attempt {attempt + 1} failed: {str(e)}")
                
                # Check if we should activate backup strategies
                if (self.failure_count >= self.max_failures_before_backup and 
                    attempt < self.max_failures_before_backup + len(self.backup_strategies)):
                    
                    # Activate next backup strategy
                    strategy_index = attempt - self.max_failures_before_backup
                    if strategy_index < len(self.backup_strategies):
                        strategy = self.backup_strategies[strategy_index]
                        if strategy.enabled:
                            self.logger.info(f"Activating backup strategy: {strategy.name}")
                            self._activate_backup_strategy(strategy)
                        else:
                            self.logger.warning(f"Backup strategy {strategy.name} is disabled")
                
                # Add delay before retry
                if attempt < self.max_failures_before_backup + len(self.backup_strategies):
                    delay = min(2.0 * (attempt + 1), 30.0)  # Exponential backoff, max 30s
                    time.sleep(delay)
        
        # All attempts failed
        raise CrawlerError(
            f"Failed to fetch products after {self.max_failures_before_backup + len(self.backup_strategies) + 1} attempts",
            {
                "source": self.source_name,
                "failure_count": self.failure_count,
                "backup_strategies_tried": [s.name for s in self.active_backup_strategies],
                "last_error": str(last_exception) if last_exception else "Unknown error"
            }
        )
    
    def _activate_backup_strategy(self, strategy: BackupStrategy) -> bool:
        """
        Activate a backup strategy.
        
        Args:
            strategy: Backup strategy to activate
            
        Returns:
            True if strategy was activated successfully
        """
        try:
            self.logger.info(f"Activating backup strategy: {strategy.name}")
            
            if strategy.name == 'user_agent_rotation':
                return self._activate_user_agent_rotation(strategy)
            elif strategy.name == 'proxy_rotation':
                return self._activate_proxy_rotation(strategy)
            elif strategy.name == 'delay_increase':
                return self._activate_delay_increase(strategy)
            elif strategy.name == 'alternative_endpoint':
                return self._activate_alternative_endpoint(strategy)
            else:
                self.logger.warning(f"Unknown backup strategy: {strategy.name}")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to activate backup strategy {strategy.name}: {str(e)}")
            return False
    
    def _activate_user_agent_rotation(self, strategy: BackupStrategy) -> bool:
        """Activate user agent rotation strategy."""
        try:
            # Force user agent rotation in HTTP client
            if hasattr(self.http_client, 'user_agent_rotator'):
                # Get a new user agent
                new_user_agent = self.http_client.user_agent_rotator.get_random_user_agent()
                self.logger.info(f"Switched to new user agent: {new_user_agent[:50]}...")
                
                # Mark strategy as active
                if strategy not in self.active_backup_strategies:
                    self.active_backup_strategies.append(strategy)
                
                return True
        except Exception as e:
            self.logger.error(f"Failed to activate user agent rotation: {str(e)}")
        
        return False
    
    def _activate_proxy_rotation(self, strategy: BackupStrategy) -> bool:
        """Activate proxy rotation strategy."""
        try:
            # This would require proxy configuration
            # For now, we'll just log that it would be activated
            self.logger.info("Proxy rotation strategy activated (implementation needed)")
            
            # Mark strategy as active
            if strategy not in self.active_backup_strategies:
                self.active_backup_strategies.append(strategy)
            
            return True
        except Exception as e:
            self.logger.error(f"Failed to activate proxy rotation: {str(e)}")
        
        return False
    
    def _activate_delay_increase(self, strategy: BackupStrategy) -> bool:
        """Activate delay increase strategy."""
        try:
            # Increase delays in configuration
            current_min = self.config.get('min_delay', 1.0)
            current_max = self.config.get('max_delay', 3.0)
            
            # Double the delays
            self.config['min_delay'] = current_min * 2
            self.config['max_delay'] = current_max * 2
            
            self.logger.info(f"Increased delays: min={self.config['min_delay']}, max={self.config['max_delay']}")
            
            # Mark strategy as active
            if strategy not in self.active_backup_strategies:
                self.active_backup_strategies.append(strategy)
            
            return True
        except Exception as e:
            self.logger.error(f"Failed to activate delay increase: {str(e)}")
        
        return False
    
    def _activate_alternative_endpoint(self, strategy: BackupStrategy) -> bool:
        """Activate alternative endpoint strategy."""
        try:
            # This would switch to alternative URLs/endpoints
            # Implementation depends on specific crawler
            self.logger.info("Alternative endpoint strategy activated (implementation needed)")
            
            # Mark strategy as active
            if strategy not in self.active_backup_strategies:
                self.active_backup_strategies.append(strategy)
            
            return True
        except Exception as e:
            self.logger.error(f"Failed to activate alternative endpoint: {str(e)}")
        
        return False
    
    def is_blocked(self, response: Optional[Any] = None, exception: Optional[Exception] = None) -> bool:
        """
        Detect if the crawler is being blocked.
        
        Args:
            response: HTTP response object (if available)
            exception: Exception that occurred (if any)
            
        Returns:
            True if blocking is detected
        """
        # Check HTTP response indicators
        if response:
            if hasattr(response, 'status_code'):
                # Common blocking status codes
                if response.status_code in [403, 429, 503]:
                    return True
                
                # Check response content for blocking indicators
                if hasattr(response, 'text'):
                    blocking_indicators = [
                        'access denied', 'blocked', 'captcha', 'verification',
                        '访问被拒绝', '验证码', '人机验证', '请稍后再试'
                    ]
                    response_text = response.text.lower()
                    for indicator in blocking_indicators:
                        if indicator in response_text:
                            return True
        
        # Check exception types
        if exception:
            exception_str = str(exception).lower()
            blocking_exceptions = [
                'connection refused', 'timeout', 'too many requests',
                'rate limit', 'blocked'
            ]
            for blocking_exception in blocking_exceptions:
                if blocking_exception in exception_str:
                    return True
        
        return False
    
    def get_active_backup_strategies(self) -> List[str]:
        """Get list of currently active backup strategy names."""
        return [strategy.name for strategy in self.active_backup_strategies]
    
    def validate_config(self) -> bool:
        """
        Validate crawler configuration.
        
        Returns:
            True if configuration is valid
            
        Raises:
            CrawlerError: If configuration is invalid
        """
        # Base implementation - can be overridden by subclasses
        return True
    
    def cleanup(self) -> None:
        """Clean up resources used by the crawler."""
        if hasattr(self, 'http_client') and self.http_client:
            self.http_client.close()
    
    def __del__(self):
        """Destructor to ensure cleanup."""
        self.cleanup()


class CrawlerRegistry:
    """Registry for managing crawler instances."""
    
    def __init__(self):
        """Initialize empty crawler registry."""
        self._crawlers: Dict[str, type] = {}
        self._instances: Dict[str, BaseCrawler] = {}
        self._crawler_configs: Dict[str, Dict[str, Any]] = {}
        self.logger = get_business_logger('crawler_general')
    
    def register(self, name: str, crawler_class: type, default_config: Optional[Dict[str, Any]] = None) -> None:
        """
        Register a crawler class.
        
        Args:
            name: Crawler name (e.g., 'jd', 'zol')
            crawler_class: Crawler class that extends BaseCrawler
            default_config: Default configuration for the crawler
            
        Raises:
            CrawlerError: If crawler class is invalid
        """
        if not issubclass(crawler_class, BaseCrawler):
            raise CrawlerError(
                f"Crawler class must extend BaseCrawler",
                {"crawler_name": name, "crawler_class": str(crawler_class)}
            )
        
        self._crawlers[name] = crawler_class
        self._crawler_configs[name] = default_config or {}
        self.logger.info(f"Crawler registered: {name}, has_config={bool(default_config)}")
    
    def get_crawler(self, name: str, config: Optional[Dict[str, Any]] = None) -> BaseCrawler:
        """
        Get a crawler instance.
        
        Args:
            name: Crawler name
            config: Optional crawler configuration (merged with default config)
            
        Returns:
            Crawler instance
            
        Raises:
            CrawlerError: If crawler is not registered
        """
        if name not in self._crawlers:
            raise CrawlerError(
                f"Crawler '{name}' is not registered",
                {"available_crawlers": list(self._crawlers.keys())}
            )
        
        # Merge default config with provided config
        final_config = self._crawler_configs[name].copy()
        if config:
            final_config.update(config)
        
        # Create new instance if not cached or config changed
        cache_key = f"{name}_{hash(str(sorted(final_config.items())))}"
        if cache_key not in self._instances:
            crawler_class = self._crawlers[name]
            try:
                instance = crawler_class(name, final_config)
                # Validate configuration
                instance.validate_config()
                self._instances[cache_key] = instance
                self.logger.debug(f"Crawler instance created: {name}, cache_key={cache_key}")
            except Exception as e:
                raise CrawlerError(
                    f"Failed to create crawler instance '{name}'",
                    {"error": str(e), "config": final_config}
                )
        
        return self._instances[cache_key]
    
    def list_crawlers(self) -> List[str]:
        """
        List all registered crawler names.
        
        Returns:
            List of crawler names
        """
        return list(self._crawlers.keys())
    
    def get_crawler_info(self, name: str) -> Dict[str, Any]:
        """
        Get information about a registered crawler.
        
        Args:
            name: Crawler name
            
        Returns:
            Dictionary with crawler information
            
        Raises:
            CrawlerError: If crawler is not registered
        """
        if name not in self._crawlers:
            raise CrawlerError(
                f"Crawler '{name}' is not registered",
                {"available_crawlers": list(self._crawlers.keys())}
            )
        
        crawler_class = self._crawlers[name]
        return {
            "name": name,
            "class": crawler_class.__name__,
            "module": crawler_class.__module__,
            "default_config": self._crawler_configs[name],
            "doc": crawler_class.__doc__ or "No documentation available"
        }
    
    def update_default_config(self, name: str, config: Dict[str, Any]) -> None:
        """
        Update default configuration for a crawler.
        
        Args:
            name: Crawler name
            config: New default configuration
            
        Raises:
            CrawlerError: If crawler is not registered
        """
        if name not in self._crawlers:
            raise CrawlerError(
                f"Crawler '{name}' is not registered",
                {"available_crawlers": list(self._crawlers.keys())}
            )
        
        self._crawler_configs[name] = config
        # Clear cached instances to force recreation with new config
        keys_to_remove = [k for k in self._instances.keys() if k.startswith(f"{name}_")]
        for key in keys_to_remove:
            del self._instances[key]
        
        self.logger.info(f"Crawler default config updated: {name}")
    
    def unregister(self, name: str) -> None:
        """
        Unregister a crawler.
        
        Args:
            name: Crawler name to unregister
        """
        if name in self._crawlers:
            del self._crawlers[name]
            if name in self._crawler_configs:
                del self._crawler_configs[name]
            # Remove cached instances
            keys_to_remove = [k for k in self._instances.keys() if k.startswith(f"{name}_")]
            for key in keys_to_remove:
                del self._instances[key]
            self.logger.info(f"Crawler unregistered: {name}")
    
    def clear_cache(self, name: Optional[str] = None) -> None:
        """
        Clear cached crawler instances.
        
        Args:
            name: Optional crawler name to clear cache for. If None, clears all.
        """
        if name:
            keys_to_remove = [k for k in self._instances.keys() if k.startswith(f"{name}_")]
            for key in keys_to_remove:
                del self._instances[key]
            self.logger.debug(f"Crawler cache cleared: {name}")
        else:
            self._instances.clear()
            self.logger.debug("All crawler cache cleared")
    
    def get_active_instances(self) -> Dict[str, List[str]]:
        """
        Get information about active crawler instances.
        
        Returns:
            Dictionary mapping crawler names to list of cache keys
        """
        active_instances = {}
        for cache_key in self._instances.keys():
            crawler_name = cache_key.split('_')[0]
            if crawler_name not in active_instances:
                active_instances[crawler_name] = []
            active_instances[crawler_name].append(cache_key)
        
        return active_instances
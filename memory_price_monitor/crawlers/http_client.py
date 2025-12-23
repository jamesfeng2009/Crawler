"""
HTTP client with retry logic, rate limiting, and anti-crawler countermeasures.
"""

import time
import random
import asyncio
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from urllib.parse import urlparse
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from memory_price_monitor.utils.logging import get_business_logger
from memory_price_monitor.utils.errors import CrawlerError, retry_on_error


logger = get_business_logger('crawler_general')


@dataclass
class RateLimitConfig:
    """Rate limiting configuration."""
    requests_per_second: float = 1.0
    requests_per_minute: int = 60
    requests_per_hour: int = 3600
    burst_size: int = 5
    backoff_factor: float = 2.0
    max_delay: float = 300.0  # 5 minutes


@dataclass
class RetryConfig:
    """Retry configuration."""
    max_attempts: int = 3
    backoff_factor: float = 2.0
    initial_delay: float = 1.0
    max_delay: float = 60.0
    retry_on_status: List[int] = field(default_factory=lambda: [429, 500, 502, 503, 504])


@dataclass
class RequestStats:
    """Request statistics for rate limiting."""
    last_request_time: datetime = field(default_factory=datetime.now)
    requests_this_second: int = 0
    requests_this_minute: int = 0
    requests_this_hour: int = 0
    second_window_start: datetime = field(default_factory=datetime.now)
    minute_window_start: datetime = field(default_factory=datetime.now)
    hour_window_start: datetime = field(default_factory=datetime.now)


class UserAgentRotator:
    """Rotates user agents to avoid detection."""
    
    def __init__(self):
        """Initialize with common user agents."""
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
        ]
        self.current_index = 0
    
    def get_random_user_agent(self) -> str:
        """Get a random user agent."""
        return random.choice(self.user_agents)
    
    def get_next_user_agent(self) -> str:
        """Get the next user agent in rotation."""
        user_agent = self.user_agents[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.user_agents)
        return user_agent


class RateLimiter:
    """Rate limiter with sliding window algorithm."""
    
    def __init__(self, config: RateLimitConfig):
        """Initialize rate limiter with configuration."""
        self.config = config
        self.stats_by_domain: Dict[str, RequestStats] = {}
        self.logger = get_business_logger('crawler_general')
    
    def wait_if_needed(self, url: str) -> None:
        """Wait if rate limit would be exceeded."""
        domain = self._get_domain(url)
        stats = self._get_stats(domain)
        
        now = datetime.now()
        self._update_windows(stats, now)
        
        # Check if we need to wait
        delay = self._calculate_delay(stats, now)
        if delay > 0:
            self.logger.info("Rate limiting delay", 
                           domain=domain, 
                           delay_seconds=delay)
            time.sleep(delay)
            
            # Update stats after waiting
            now = datetime.now()
            self._update_windows(stats, now)
        
        # Record the request
        self._record_request(stats, now)
    
    def _get_domain(self, url: str) -> str:
        """Extract domain from URL."""
        try:
            return urlparse(url).netloc
        except Exception:
            return "unknown"
    
    def _get_stats(self, domain: str) -> RequestStats:
        """Get or create stats for domain."""
        if domain not in self.stats_by_domain:
            self.stats_by_domain[domain] = RequestStats()
        return self.stats_by_domain[domain]
    
    def _update_windows(self, stats: RequestStats, now: datetime) -> None:
        """Update sliding windows."""
        # Reset second window if needed
        if (now - stats.second_window_start).total_seconds() >= 1.0:
            stats.requests_this_second = 0
            stats.second_window_start = now
        
        # Reset minute window if needed
        if (now - stats.minute_window_start).total_seconds() >= 60.0:
            stats.requests_this_minute = 0
            stats.minute_window_start = now
        
        # Reset hour window if needed
        if (now - stats.hour_window_start).total_seconds() >= 3600.0:
            stats.requests_this_hour = 0
            stats.hour_window_start = now
    
    def _calculate_delay(self, stats: RequestStats, now: datetime) -> float:
        """Calculate required delay to respect rate limits."""
        delays = []
        
        # Check per-second limit
        if stats.requests_this_second >= self.config.requests_per_second:
            time_since_window_start = (now - stats.second_window_start).total_seconds()
            delay = 1.0 - time_since_window_start
            if delay > 0:
                delays.append(delay)
        
        # Check per-minute limit
        if stats.requests_this_minute >= self.config.requests_per_minute:
            time_since_window_start = (now - stats.minute_window_start).total_seconds()
            delay = 60.0 - time_since_window_start
            if delay > 0:
                delays.append(delay)
        
        # Check per-hour limit
        if stats.requests_this_hour >= self.config.requests_per_hour:
            time_since_window_start = (now - stats.hour_window_start).total_seconds()
            delay = 3600.0 - time_since_window_start
            if delay > 0:
                delays.append(delay)
        
        # Return the maximum delay needed
        max_delay = max(delays) if delays else 0.0
        return min(max_delay, self.config.max_delay)
    
    def _record_request(self, stats: RequestStats, now: datetime) -> None:
        """Record a request in the stats."""
        stats.last_request_time = now
        stats.requests_this_second += 1
        stats.requests_this_minute += 1
        stats.requests_this_hour += 1


class HTTPClient:
    """HTTP client with retry logic, rate limiting, and anti-crawler countermeasures."""
    
    def __init__(self, 
                 rate_limit_config: Optional[RateLimitConfig] = None,
                 retry_config: Optional[RetryConfig] = None,
                 timeout: float = 30.0):
        """
        Initialize HTTP client.
        
        Args:
            rate_limit_config: Rate limiting configuration
            retry_config: Retry configuration
            timeout: Request timeout in seconds
        """
        self.rate_limit_config = rate_limit_config or RateLimitConfig()
        self.retry_config = retry_config or RetryConfig()
        self.timeout = timeout
        
        self.rate_limiter = RateLimiter(self.rate_limit_config)
        self.user_agent_rotator = UserAgentRotator()
        self.session = self._create_session()
        self.logger = get_business_logger('crawler_general')
    
    def _create_session(self) -> requests.Session:
        """Create requests session with retry configuration."""
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=self.retry_config.max_attempts,
            backoff_factor=self.retry_config.backoff_factor,
            status_forcelist=self.retry_config.retry_on_status,
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        
        # Mount adapter with retry strategy
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def get(self, url: str, 
            headers: Optional[Dict[str, str]] = None,
            params: Optional[Dict[str, Any]] = None,
            **kwargs) -> requests.Response:
        """
        Perform GET request with rate limiting and anti-crawler measures.
        
        Args:
            url: URL to request
            headers: Additional headers
            params: Query parameters
            **kwargs: Additional arguments for requests
            
        Returns:
            Response object
            
        Raises:
            CrawlerError: If request fails after all retries
        """
        return self._request("GET", url, headers=headers, params=params, **kwargs)
    
    def post(self, url: str,
             data: Optional[Union[Dict[str, Any], str]] = None,
             json: Optional[Dict[str, Any]] = None,
             headers: Optional[Dict[str, str]] = None,
             **kwargs) -> requests.Response:
        """
        Perform POST request with rate limiting and anti-crawler measures.
        
        Args:
            url: URL to request
            data: Form data
            json: JSON data
            headers: Additional headers
            **kwargs: Additional arguments for requests
            
        Returns:
            Response object
            
        Raises:
            CrawlerError: If request fails after all retries
        """
        return self._request("POST", url, data=data, json=json, headers=headers, **kwargs)
    
    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        """
        Perform HTTP request with all countermeasures.
        
        Args:
            method: HTTP method
            url: URL to request
            **kwargs: Additional arguments for requests
            
        Returns:
            Response object
            
        Raises:
            CrawlerError: If request fails after all retries
        """
        # Apply rate limiting
        self.rate_limiter.wait_if_needed(url)
        
        # Prepare headers with anti-crawler measures
        headers = kwargs.get('headers', {})
        headers = self._prepare_headers(headers)
        kwargs['headers'] = headers
        
        # Add random delay to avoid detection
        self._add_random_delay()
        
        # Set timeout
        kwargs.setdefault('timeout', self.timeout)
        
        # Perform request with retry logic
        last_exception = None
        for attempt in range(self.retry_config.max_attempts):
            try:
                self.logger.debug(f"Making HTTP request: {method} {url} (attempt {attempt + 1})")
                
                response = self.session.request(method, url, **kwargs)
                
                # Check for blocking indicators
                if self._is_blocked_response(response):
                    self.logger.warning(f"Blocking detected for {url}: status={response.status_code}")
                    
                    # If this is a blocking response and we have retries left, continue to retry
                    if attempt < self.retry_config.max_attempts - 1:
                        # Apply anti-blocking measures
                        self._apply_anti_blocking_measures()
                        
                        # Wait before retry
                        retry_after = self._get_retry_after(response)
                        if retry_after:
                            self.logger.warning(f"Rate limited, waiting {retry_after}s for {url}")
                            time.sleep(retry_after)
                        else:
                            delay = self._calculate_retry_delay(attempt)
                            time.sleep(delay)
                        continue
                
                # Raise for HTTP errors (will be caught and retried if configured)
                response.raise_for_status()
                
                self.logger.debug(f"HTTP request successful: {method} {url} (status={response.status_code}, size={len(response.content)})")
                
                return response
                
            except requests.exceptions.RequestException as e:
                last_exception = e
                
                if attempt < self.retry_config.max_attempts - 1:
                    delay = self._calculate_retry_delay(attempt)
                    self.logger.warning(f"HTTP request failed, retrying: {method} {url} (attempt {attempt + 1}, error: {str(e)}, retry_delay: {delay}s)")
                    time.sleep(delay)
                else:
                    self.logger.error(f"HTTP request failed after all retries: {method} {url} (attempts: {self.retry_config.max_attempts}, error: {str(e)})")
        
        # All retries exhausted
        raise CrawlerError(
            f"HTTP request failed after {self.retry_config.max_attempts} attempts",
            {
                "method": method,
                "url": url,
                "last_error": str(last_exception) if last_exception else "Unknown error"
            }
        )
    
    def _is_blocked_response(self, response: requests.Response) -> bool:
        """
        Check if response indicates blocking.
        
        Args:
            response: HTTP response
            
        Returns:
            True if response indicates blocking
        """
        # Check status codes that indicate blocking
        if response.status_code in [403, 429, 503]:
            return True
        
        # Check response content for blocking indicators
        if response.text:
            blocking_indicators = [
                'access denied', 'blocked', 'captcha', 'verification',
                '访问被拒绝', '验证码', '人机验证', '请稍后再试',
                'too many requests', 'rate limit'
            ]
            response_text = response.text.lower()
            for indicator in blocking_indicators:
                if indicator in response_text:
                    return True
        
        return False
    
    def _apply_anti_blocking_measures(self) -> None:
        """Apply anti-blocking measures like rotating user agent."""
        # Rotate user agent
        new_user_agent = self.user_agent_rotator.get_random_user_agent()
        self.logger.debug(f"Rotating user agent to: {new_user_agent[:50]}...")
        
        # Add extra random delay
        extra_delay = random.uniform(1.0, 3.0)
        time.sleep(extra_delay)
    
    def _prepare_headers(self, headers: Optional[Dict[str, str]]) -> Dict[str, str]:
        """Prepare headers with anti-crawler measures."""
        # Start with default headers
        default_headers = {
            'User-Agent': self.user_agent_rotator.get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        }
        
        # Merge with provided headers (provided headers take precedence)
        if headers:
            final_headers = {**default_headers, **headers}
        else:
            final_headers = default_headers
        
        return final_headers
    
    def _add_random_delay(self) -> None:
        """Add random delay to avoid detection patterns."""
        # Random delay between 0.1 and 2.0 seconds
        delay = random.uniform(0.1, 2.0)
        time.sleep(delay)
    
    def _get_retry_after(self, response: requests.Response) -> Optional[float]:
        """Extract retry-after header value."""
        retry_after = response.headers.get('Retry-After')
        if retry_after:
            try:
                return float(retry_after)
            except ValueError:
                # Could be a date format, but we'll just use default delay
                pass
        return None
    
    def _calculate_retry_delay(self, attempt: int) -> float:
        """Calculate delay for retry attempt."""
        delay = self.retry_config.initial_delay * (self.retry_config.backoff_factor ** attempt)
        # Add jitter to avoid thundering herd
        jitter = random.uniform(0.1, 0.5)
        delay = delay + jitter
        return min(delay, self.retry_config.max_delay)
    
    def close(self) -> None:
        """Close the HTTP session."""
        if self.session:
            self.session.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
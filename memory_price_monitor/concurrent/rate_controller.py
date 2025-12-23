"""
Rate controller for concurrent crawler system.
Provides global request frequency control and rate limiting functionality.
"""

import time
import threading
from typing import Dict, Optional, Any
from datetime import datetime, timedelta
from collections import deque

from memory_price_monitor.utils.logging import get_logger
from .thread_safe import ThreadSafeCounter


logger = get_logger(__name__)


class RateController:
    """
    Global rate controller for managing request frequency across all workers.
    
    Provides thread-safe rate limiting with support for:
    - Requests per second limiting
    - Per-worker permit tracking
    - Adaptive rate adjustment
    - Backoff strategies for rate limit errors
    """
    
    def __init__(self, requests_per_second: float = 2.0, max_concurrent_requests: int = 10):
        """
        Initialize rate controller.
        
        Args:
            requests_per_second: Maximum requests per second globally
            max_concurrent_requests: Maximum concurrent requests allowed
        """
        self.requests_per_second = requests_per_second
        self.max_concurrent_requests = max_concurrent_requests
        
        # Thread safety
        self._lock = threading.RLock()
        
        # Request tracking
        self._request_times = deque()  # Track request timestamps
        self._active_requests = ThreadSafeCounter()  # Current active requests
        self._total_requests = ThreadSafeCounter()  # Total requests made
        
        # Per-worker tracking
        self._worker_permits: Dict[str, bool] = {}  # Worker permit status
        self._worker_request_times: Dict[str, deque] = {}  # Per-worker request history
        self._worker_backoff_until: Dict[str, datetime] = {}  # Worker backoff end times
        
        # Rate limiting state
        self._last_request_time = 0.0
        self._min_interval = 1.0 / requests_per_second if requests_per_second > 0 else 0.0
        
        # Adaptive rate control
        self._adaptive_enabled = True
        self._rate_limit_errors = ThreadSafeCounter()
        self._consecutive_errors = 0
        self._original_rate = requests_per_second
        
        self.logger = get_logger(__name__)
        self.logger.info(f"Rate controller initialized: {requests_per_second} req/s, max concurrent: {max_concurrent_requests}")
    
    def acquire_permit(self, worker_id: str) -> bool:
        """
        Acquire a permit to make a request.
        
        Args:
            worker_id: Unique identifier for the requesting worker
            
        Returns:
            True if permit was acquired, False if rate limited
        """
        with self._lock:
            current_time = time.time()
            
            # Check if worker is in backoff period
            if worker_id in self._worker_backoff_until:
                backoff_until = self._worker_backoff_until[worker_id]
                if datetime.now() < backoff_until:
                    self.logger.debug(f"Worker {worker_id} still in backoff until {backoff_until}")
                    return False
                else:
                    # Backoff period ended
                    del self._worker_backoff_until[worker_id]
                    self.logger.debug(f"Worker {worker_id} backoff period ended")
            
            # Check concurrent request limit
            if self._active_requests.get_value() >= self.max_concurrent_requests:
                self.logger.debug(f"Concurrent request limit reached: {self._active_requests.get_value()}/{self.max_concurrent_requests}")
                return False
            
            # Check global rate limit
            if not self._check_global_rate_limit(current_time):
                self.logger.debug(f"Global rate limit exceeded: {self.requests_per_second} req/s")
                return False
            
            # Acquire permit
            self._active_requests.increment()
            self._total_requests.increment()
            self._worker_permits[worker_id] = True
            
            # Track request time
            self._request_times.append(current_time)
            self._last_request_time = current_time
            
            # Track per-worker request time
            if worker_id not in self._worker_request_times:
                self._worker_request_times[worker_id] = deque()
            self._worker_request_times[worker_id].append(current_time)
            
            # Clean old request times (keep only last second)
            self._cleanup_old_request_times(current_time)
            
            self.logger.debug(f"Permit acquired for worker {worker_id}, active: {self._active_requests.get_value()}")
            return True
    
    def release_permit(self, worker_id: str) -> None:
        """
        Release a permit after request completion.
        
        Args:
            worker_id: Unique identifier for the worker releasing the permit
        """
        with self._lock:
            if worker_id in self._worker_permits and self._worker_permits[worker_id]:
                self._active_requests.decrement()
                self._worker_permits[worker_id] = False
                self.logger.debug(f"Permit released for worker {worker_id}, active: {self._active_requests.get_value()}")
            else:
                self.logger.warning(f"Attempted to release permit for worker {worker_id} without active permit")
    
    def handle_rate_limit_error(self, worker_id: str) -> int:
        """
        Handle a rate limit error (429 response) for a worker.
        
        Args:
            worker_id: Worker that received the rate limit error
            
        Returns:
            Backoff delay in seconds
        """
        with self._lock:
            self._rate_limit_errors.increment()
            self._consecutive_errors += 1
            
            # Calculate exponential backoff delay
            base_delay = 1.0  # Base delay of 1 second
            max_delay = 60.0  # Maximum delay of 60 seconds
            
            # Exponential backoff: 1, 2, 4, 8, 16, 32, 60 seconds
            delay = min(base_delay * (2 ** (self._consecutive_errors - 1)), max_delay)
            
            # Set worker backoff period
            backoff_until = datetime.now() + timedelta(seconds=delay)
            self._worker_backoff_until[worker_id] = backoff_until
            
            # Release the worker's permit if they have one
            self.release_permit(worker_id)
            
            # Adaptive rate adjustment
            if self._adaptive_enabled:
                self._adjust_rate_for_errors()
            
            self.logger.warning(f"Rate limit error for worker {worker_id}, backoff for {delay}s until {backoff_until}")
            return int(delay)
    
    def adjust_rate_limit(self, new_rate: float) -> None:
        """
        Adjust the global rate limit.
        
        Args:
            new_rate: New requests per second limit
        """
        with self._lock:
            old_rate = self.requests_per_second
            self.requests_per_second = max(0.1, min(new_rate, 10.0))  # Clamp between 0.1 and 10.0
            self._min_interval = 1.0 / self.requests_per_second if self.requests_per_second > 0 else 0.0
            
            self.logger.info(f"Rate limit adjusted from {old_rate} to {self.requests_per_second} req/s")
    
    def get_current_rate(self) -> float:
        """
        Get current actual request rate.
        
        Returns:
            Current requests per second over the last second
        """
        with self._lock:
            current_time = time.time()
            self._cleanup_old_request_times(current_time)
            
            # Count requests in the last second
            recent_requests = len([t for t in self._request_times if current_time - t <= 1.0])
            return float(recent_requests)
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get rate controller statistics.
        
        Returns:
            Dictionary with rate controller statistics
        """
        with self._lock:
            current_time = time.time()
            self._cleanup_old_request_times(current_time)
            
            # Calculate current rate
            current_rate = self.get_current_rate()
            
            # Worker statistics
            active_workers = len([w for w, active in self._worker_permits.items() if active])
            workers_in_backoff = len(self._worker_backoff_until)
            
            return {
                "requests_per_second_limit": self.requests_per_second,
                "max_concurrent_requests": self.max_concurrent_requests,
                "current_rate": current_rate,
                "active_requests": self._active_requests.get_value(),
                "total_requests": self._total_requests.get_value(),
                "rate_limit_errors": self._rate_limit_errors.get_value(),
                "consecutive_errors": self._consecutive_errors,
                "active_workers": active_workers,
                "workers_in_backoff": workers_in_backoff,
                "adaptive_enabled": self._adaptive_enabled,
                "original_rate": self._original_rate,
                "utilization_percentage": (current_rate / self.requests_per_second * 100.0) if self.requests_per_second > 0 else 0.0
            }
    
    def reset_statistics(self) -> None:
        """Reset all statistics counters."""
        with self._lock:
            self._total_requests.reset()
            self._rate_limit_errors.reset()
            self._consecutive_errors = 0
            self._request_times.clear()
            self._worker_request_times.clear()
            self._worker_backoff_until.clear()
            
            self.logger.info("Rate controller statistics reset")
    
    def enable_adaptive_rate(self, enabled: bool = True) -> None:
        """
        Enable or disable adaptive rate adjustment.
        
        Args:
            enabled: Whether to enable adaptive rate adjustment
        """
        with self._lock:
            self._adaptive_enabled = enabled
            if not enabled:
                # Reset to original rate
                self.requests_per_second = self._original_rate
                self._min_interval = 1.0 / self.requests_per_second if self.requests_per_second > 0 else 0.0
                self._consecutive_errors = 0
            
            self.logger.info(f"Adaptive rate control {'enabled' if enabled else 'disabled'}")
    
    def _check_global_rate_limit(self, current_time: float) -> bool:
        """
        Check if global rate limit allows a new request.
        
        Args:
            current_time: Current timestamp
            
        Returns:
            True if request is allowed
        """
        # If no rate limit, allow all requests
        if self.requests_per_second <= 0:
            return True
        
        # For the first request, always allow it
        if self._last_request_time == 0.0:
            return True
        
        # Count requests in the last second (not including the current one)
        recent_requests = len([t for t in self._request_times if current_time - t <= 1.0])
        
        # Allow request if under the rate limit
        return recent_requests < self.requests_per_second
    
    def _cleanup_old_request_times(self, current_time: float) -> None:
        """
        Remove request times older than 1 second.
        
        Args:
            current_time: Current timestamp
        """
        # Clean global request times
        while self._request_times and current_time - self._request_times[0] > 1.0:
            self._request_times.popleft()
        
        # Clean per-worker request times
        for worker_id in list(self._worker_request_times.keys()):
            worker_times = self._worker_request_times[worker_id]
            while worker_times and current_time - worker_times[0] > 1.0:
                worker_times.popleft()
            
            # Remove empty deques
            if not worker_times:
                del self._worker_request_times[worker_id]
    
    def _adjust_rate_for_errors(self) -> None:
        """Adjust rate limit based on consecutive errors."""
        if self._consecutive_errors >= 3:
            # Reduce rate by 50% after 3 consecutive errors
            new_rate = self.requests_per_second * 0.5
            new_rate = max(0.1, new_rate)  # Don't go below 0.1 req/s
            
            if new_rate != self.requests_per_second:
                self.logger.warning(f"Reducing rate due to errors: {self.requests_per_second} -> {new_rate} req/s")
                self.requests_per_second = new_rate
                self._min_interval = 1.0 / self.requests_per_second
        elif self._consecutive_errors == 0 and self.requests_per_second < self._original_rate:
            # Gradually increase rate back to original when no errors
            new_rate = min(self.requests_per_second * 1.1, self._original_rate)
            
            if new_rate != self.requests_per_second:
                self.logger.info(f"Increasing rate after success: {self.requests_per_second} -> {new_rate} req/s")
                self.requests_per_second = new_rate
                self._min_interval = 1.0 / self.requests_per_second
    
    def wait_for_permit(self, worker_id: str, timeout: Optional[float] = None) -> bool:
        """
        Wait for a permit to become available.
        
        Args:
            worker_id: Worker requesting the permit
            timeout: Maximum time to wait in seconds (None for no timeout)
            
        Returns:
            True if permit was acquired, False if timeout
        """
        start_time = time.time()
        
        while True:
            if self.acquire_permit(worker_id):
                return True
            
            # Check timeout
            if timeout is not None and (time.time() - start_time) >= timeout:
                return False
            
            # Wait a short time before retrying
            time.sleep(0.1)
    
    def get_worker_status(self, worker_id: str) -> Dict[str, Any]:
        """
        Get status for a specific worker.
        
        Args:
            worker_id: Worker to get status for
            
        Returns:
            Dictionary with worker status
        """
        with self._lock:
            has_permit = self._worker_permits.get(worker_id, False)
            in_backoff = worker_id in self._worker_backoff_until
            backoff_until = self._worker_backoff_until.get(worker_id)
            
            # Calculate worker request rate
            worker_rate = 0.0
            if worker_id in self._worker_request_times:
                current_time = time.time()
                recent_requests = len([t for t in self._worker_request_times[worker_id] 
                                     if current_time - t <= 1.0])
                worker_rate = float(recent_requests)
            
            return {
                "worker_id": worker_id,
                "has_permit": has_permit,
                "in_backoff": in_backoff,
                "backoff_until": backoff_until.isoformat() if backoff_until else None,
                "current_rate": worker_rate,
                "can_request": not in_backoff and not has_permit
            }
    
    def __str__(self) -> str:
        """String representation of rate controller."""
        stats = self.get_statistics()
        return f"RateController(rate={stats['requests_per_second_limit']}, active={stats['active_requests']}, current_rate={stats['current_rate']:.1f})"
    
    def __repr__(self) -> str:
        """Detailed string representation."""
        return f"RateController(requests_per_second={self.requests_per_second}, max_concurrent={self.max_concurrent_requests})"
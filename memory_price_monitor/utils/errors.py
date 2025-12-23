"""
Custom exception classes and error handling utilities.
"""

from typing import Optional, Dict, Any
import traceback


class MemoryPriceMonitorError(Exception):
    """Base exception for all memory price monitor errors."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}


class CrawlerError(MemoryPriceMonitorError):
    """Exception raised during crawling operations."""
    pass


class DataStandardizationError(MemoryPriceMonitorError):
    """Exception raised during data standardization."""
    pass


class DatabaseError(MemoryPriceMonitorError):
    """Exception raised during database operations."""
    pass


class NotificationError(MemoryPriceMonitorError):
    """Exception raised during notification operations."""
    pass


class ConfigurationError(MemoryPriceMonitorError):
    """Exception raised for configuration-related issues."""
    pass


class ValidationError(MemoryPriceMonitorError):
    """Exception raised for data validation failures."""
    pass


class SchedulerError(MemoryPriceMonitorError):
    """Exception raised during scheduler operations."""
    pass


class StateManagementError(MemoryPriceMonitorError):
    """Exception raised during state management operations."""
    pass


def handle_error(
    error: Exception,
    logger,
    context: Optional[Dict[str, Any]] = None,
    reraise: bool = True
) -> None:
    """
    Handle and log errors with context information.
    
    Args:
        error: The exception that occurred
        logger: Logger instance to use for logging
        context: Additional context information
        reraise: Whether to reraise the exception after logging
    """
    error_context = {
        "error_type": type(error).__name__,
        "error_message": str(error),
        "traceback": traceback.format_exc(),
        **(context or {})
    }
    
    if isinstance(error, MemoryPriceMonitorError):
        error_context.update(error.details)
    
    logger.error("Error occurred", **error_context)
    
    if reraise:
        raise error


def retry_on_error(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: tuple = (Exception,)
):
    """
    Decorator for retrying functions on specific exceptions.
    
    Args:
        max_attempts: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff_factor: Factor to multiply delay by after each attempt
        exceptions: Tuple of exception types to retry on
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            current_delay = delay
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        import time
                        time.sleep(current_delay)
                        current_delay *= backoff_factor
                    else:
                        raise last_exception
            
            # This should never be reached, but just in case
            if last_exception:
                raise last_exception
                
        return wrapper
    return decorator
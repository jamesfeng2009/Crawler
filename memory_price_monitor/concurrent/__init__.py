"""
Concurrent crawler framework for parallel data extraction.

This module provides a complete concurrent crawling system with:
- Task scheduling and load balancing
- Thread pool management
- Rate limiting and backoff strategies
- Thread-safe data storage
- Real-time monitoring and reporting

Main Components:
- ConcurrentCrawlerController: Main system controller
- TaskScheduler: Task queue and assignment management
- ThreadPoolManager: Worker thread lifecycle management
- RateController: Global request rate limiting
- ThreadSafeRepository: Concurrent data storage
- ConcurrentCrawlerMonitor: Real-time system monitoring
"""

from .models import (
    ConcurrentConfig,
    CrawlTask,
    WorkerStatus,
    CrawlResult,
    TaskResult,
    ConcurrentResultCollector,
    WorkerState,
    TaskStatus
)

from .thread_safe import (
    ThreadSafeCounter,
    ThreadSafeQueue,
    ThreadSafeSet
)

from .controller import ConcurrentCrawlerController
from .scheduler import TaskScheduler
from .rate_controller import RateController
from .thread_pool import ThreadPoolManager, WorkerThread
from .monitoring import ConcurrentCrawlerMonitor, MonitoringConfig, MonitoringSnapshot

__all__ = [
    # Core models
    'ConcurrentConfig',
    'CrawlTask', 
    'WorkerStatus',
    'CrawlResult',
    'TaskResult',
    'ConcurrentResultCollector',
    'WorkerState',
    'TaskStatus',
    
    # Thread-safe utilities
    'ThreadSafeCounter',
    'ThreadSafeQueue', 
    'ThreadSafeSet',
    
    # Main components
    'ConcurrentCrawlerController',
    'TaskScheduler',
    'RateController',
    'ThreadPoolManager',
    'WorkerThread',
    
    # Monitoring
    'ConcurrentCrawlerMonitor',
    'MonitoringConfig',
    'MonitoringSnapshot'
]
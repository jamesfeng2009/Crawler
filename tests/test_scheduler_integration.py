"""
Integration test for the task scheduler to verify basic functionality.
"""

import pytest
import time
from unittest.mock import Mock

from memory_price_monitor.services.scheduler import (
    TaskScheduler, ResourceLimits, CrawlTask, TaskPriority
)
from memory_price_monitor.crawlers import CrawlerRegistry, BaseCrawler
from memory_price_monitor.data.repository import PriceRepository


class SimpleCrawler(BaseCrawler):
    """Simple test crawler."""
    
    def fetch_products(self):
        return [Mock() for _ in range(2)]
    
    def parse_product(self, raw_data):
        return Mock()
    
    def get_price_history(self, product_id: str):
        return []


def test_scheduler_basic_functionality():
    """Test basic scheduler functionality."""
    # Create registry and repository
    registry = CrawlerRegistry()
    repository = Mock(spec=PriceRepository)
    
    # Register a simple crawler
    registry.register('test_crawler', SimpleCrawler, {})
    
    # Create scheduler with minimal resource limits
    resource_limits = ResourceLimits(
        max_concurrent_tasks=1,
        max_memory_usage_percent=95.0,
        max_cpu_usage_percent=95.0,
        min_disk_space_gb=0.1
    )
    
    scheduler = TaskScheduler(registry, repository, resource_limits)
    
    try:
        # Start scheduler
        scheduler.start()
        assert scheduler.get_scheduler_status()['running'] is True
        
        # Execute a crawl task
        task_ids = scheduler.execute_crawl_task(['test_crawler'])
        assert len(task_ids) == 1
        assert task_ids[0].startswith('crawl_test_crawler_')
        
        # Wait briefly for task processing
        time.sleep(0.5)
        
        # Check status
        status = scheduler.get_scheduler_status()
        assert 'resource_status' in status
        assert 'active_tasks' in status
        
        # Check health
        health = scheduler.get_health_status()
        assert 'healthy' in health
        assert 'timestamp' in health
        
    finally:
        scheduler.stop(wait=False)


def test_scheduler_task_queue():
    """Test task queue functionality."""
    resource_limits = ResourceLimits(max_concurrent_tasks=2)
    registry = CrawlerRegistry()
    repository = Mock(spec=PriceRepository)
    
    registry.register('queue_test', SimpleCrawler, {})
    
    scheduler = TaskScheduler(registry, repository, resource_limits)
    
    try:
        scheduler.start()
        
        # Add multiple tasks
        task1 = CrawlTask('task1', 'queue_test', TaskPriority.HIGH)
        task2 = CrawlTask('task2', 'queue_test', TaskPriority.NORMAL)
        
        scheduler.task_queue.add_task(task1)
        scheduler.task_queue.add_task(task2)
        
        # Check queue size
        assert scheduler.task_queue.get_queue_size() == 2
        
        # Get resource status
        status = scheduler.task_queue.get_resource_status()
        assert 'running_tasks' in status
        assert 'queued_tasks' in status
        
    finally:
        scheduler.stop(wait=False)


if __name__ == '__main__':
    test_scheduler_basic_functionality()
    test_scheduler_task_queue()
    print("All integration tests passed!")
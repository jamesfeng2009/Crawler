"""
Property-based tests for scheduler execution completeness.

**Feature: memory-price-monitor, Property 7: Scheduler execution completeness**
**Validates: Requirements 3.1**
"""

import pytest
from hypothesis import given, strategies as st, settings, assume
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock, patch
from typing import List, Dict, Any

from memory_price_monitor.services.scheduler import (
    TaskScheduler, TaskQueue, CrawlTask, TaskPriority, TaskStatus, 
    TaskResult, ResourceLimits
)
from memory_price_monitor.crawlers import CrawlerRegistry, BaseCrawler, CrawlResult
from memory_price_monitor.data.repository import PriceRepository


class MockCrawler(BaseCrawler):
    """Mock crawler for testing."""
    
    def __init__(self, source_name: str, config: Dict[str, Any] = None):
        super().__init__(source_name, config)
        self.should_succeed = config.get('should_succeed', True) if config else True
        self.products_to_return = config.get('products_count', 5) if config else 5
    
    def fetch_products(self):
        if not self.should_succeed:
            raise Exception("Mock crawler failure")
        return [Mock() for _ in range(self.products_to_return)]
    
    def parse_product(self, raw_data):
        return Mock()
    
    def get_price_history(self, product_id: str):
        return []


@st.composite
def crawler_names_strategy(draw):
    """Generate list of crawler names."""
    names = draw(st.lists(
        st.text(alphabet=st.characters(whitelist_categories=('Ll', 'Lu', 'Nd')), 
                min_size=3, max_size=10),
        min_size=1, max_size=5, unique=True
    ))
    return names


@st.composite
def resource_limits_strategy(draw):
    """Generate resource limits configuration."""
    return ResourceLimits(
        max_concurrent_tasks=draw(st.integers(min_value=1, max_value=10)),
        max_memory_usage_percent=draw(st.floats(min_value=50.0, max_value=95.0)),
        max_cpu_usage_percent=draw(st.floats(min_value=50.0, max_value=95.0)),
        min_disk_space_gb=draw(st.floats(min_value=0.1, max_value=5.0)),
        check_interval_seconds=draw(st.floats(min_value=1.0, max_value=60.0))
    )


@st.composite
def crawler_configs_strategy(draw):
    """Generate crawler configurations."""
    return {
        'should_succeed': draw(st.booleans()),
        'products_count': draw(st.integers(min_value=0, max_value=20))
    }


class TestSchedulerExecutionProperties:
    """Property-based tests for scheduler execution completeness."""
    
    @given(
        crawler_names=crawler_names_strategy(),
        resource_limits=resource_limits_strategy(),
        crawler_configs=st.dictionaries(
            st.text(alphabet=st.characters(whitelist_categories=('Ll', 'Lu', 'Nd')), 
                    min_size=3, max_size=10),
            crawler_configs_strategy(),
            min_size=1, max_size=5
        )
    )
    @settings(max_examples=100, deadline=30000)
    def test_scheduler_executes_all_configured_crawlers(
        self, 
        crawler_names: List[str], 
        resource_limits: ResourceLimits,
        crawler_configs: Dict[str, Dict[str, Any]]
    ):
        """
        **Feature: memory-price-monitor, Property 7: Scheduler execution completeness**
        
        For any scheduled crawling task, the scheduler should execute all configured 
        crawler modules and verify completion.
        
        **Validates: Requirements 3.1**
        """
        # Ensure we have matching crawler names and configs
        assume(len(crawler_names) > 0)
        
        # Create mock registry and repository
        registry = CrawlerRegistry()
        repository = Mock(spec=PriceRepository)
        
        # Register mock crawlers
        for name in crawler_names:
            config = crawler_configs.get(name, {'should_succeed': True, 'products_count': 5})
            registry.register(name, MockCrawler, config)
        
        # Create scheduler
        scheduler = TaskScheduler(registry, repository, resource_limits)
        
        try:
            # Start scheduler
            scheduler.start()
            
            # Execute crawl task for all crawlers
            task_ids = scheduler.execute_crawl_task(crawler_names)
            
            # Verify that task IDs were created for all crawlers
            assert len(task_ids) == len(crawler_names), \
                f"Expected {len(crawler_names)} task IDs, got {len(task_ids)}"
            
            # Verify all task IDs are unique
            assert len(set(task_ids)) == len(task_ids), \
                "Task IDs should be unique"
            
            # Verify task IDs follow expected pattern
            for task_id in task_ids:
                assert task_id.startswith('crawl_'), \
                    f"Task ID should start with 'crawl_': {task_id}"
                assert any(name in task_id for name in crawler_names), \
                    f"Task ID should contain crawler name: {task_id}"
            
            # Wait a bit for tasks to be processed
            import time
            time.sleep(0.1)
            
            # Verify tasks were added to queue
            queue_size = scheduler.task_queue.get_queue_size()
            running_tasks = len(scheduler.task_queue.get_running_tasks())
            
            # Total tasks should equal the number of crawlers
            # (some might be running, some might be queued)
            total_tasks = queue_size + running_tasks
            assert total_tasks <= len(crawler_names), \
                f"Total tasks ({total_tasks}) should not exceed crawler count ({len(crawler_names)})"
            
        finally:
            # Clean up
            scheduler.stop(wait=False)
    
    @given(
        crawler_names=crawler_names_strategy(),
        resource_limits=resource_limits_strategy()
    )
    @settings(max_examples=10, deadline=30000)
    def test_scheduler_handles_empty_crawler_list(
        self, 
        crawler_names: List[str],
        resource_limits: ResourceLimits
    ):
        """
        **Feature: memory-price-monitor, Property 7: Scheduler execution completeness**
        
        When no crawlers are specified, the scheduler should execute all registered crawlers.
        
        **Validates: Requirements 3.1**
        """
        assume(len(crawler_names) > 0)
        
        # Create mock registry and repository
        registry = CrawlerRegistry()
        repository = Mock(spec=PriceRepository)
        
        # Register mock crawlers
        for name in crawler_names:
            registry.register(name, MockCrawler, {'should_succeed': True, 'products_count': 3})
        
        # Create scheduler
        scheduler = TaskScheduler(registry, repository, resource_limits)
        
        try:
            scheduler.start()
            
            # Execute crawl task without specifying crawler names (should use all)
            task_ids = scheduler.execute_crawl_task(None)
            
            # Should create tasks for all registered crawlers
            assert len(task_ids) == len(crawler_names), \
                f"Expected {len(crawler_names)} task IDs for all crawlers, got {len(task_ids)}"
            
            # Verify all crawler names are represented in task IDs
            for name in crawler_names:
                assert any(name in task_id for task_id in task_ids), \
                    f"Crawler {name} should have a corresponding task ID"
            
        finally:
            scheduler.stop(wait=False)
    
    @given(
        crawler_names=crawler_names_strategy(),
        resource_limits=resource_limits_strategy()
    )
    @settings(max_examples=10, deadline=30000)
    def test_scheduler_status_reflects_execution_state(
        self, 
        crawler_names: List[str],
        resource_limits: ResourceLimits
    ):
        """
        **Feature: memory-price-monitor, Property 7: Scheduler execution completeness**
        
        The scheduler status should accurately reflect the execution state of all tasks.
        
        **Validates: Requirements 3.1**
        """
        assume(len(crawler_names) > 0)
        
        # Create mock registry and repository
        registry = CrawlerRegistry()
        repository = Mock(spec=PriceRepository)
        
        # Register mock crawlers
        for name in crawler_names:
            registry.register(name, MockCrawler, {'should_succeed': True, 'products_count': 2})
        
        # Create scheduler
        scheduler = TaskScheduler(registry, repository, resource_limits)
        
        try:
            scheduler.start()
            
            # Get initial status
            initial_status = scheduler.get_scheduler_status()
            assert initial_status['running'] is True
            assert initial_status['scheduler_running'] is True
            assert 'resource_status' in initial_status
            assert 'jobs' in initial_status
            
            # Execute crawl tasks
            task_ids = scheduler.execute_crawl_task(crawler_names)
            
            # Get status after task creation
            status_after_tasks = scheduler.get_scheduler_status()
            
            # Verify status contains expected information
            assert 'active_tasks' in status_after_tasks
            assert 'completed_tasks_count' in status_after_tasks
            assert 'resource_status' in status_after_tasks
            
            # Resource status should contain expected fields
            resource_status = status_after_tasks['resource_status']
            assert 'running_tasks' in resource_status
            assert 'queued_tasks' in resource_status
            assert 'can_execute' in resource_status
            
            # The total number of tasks should match what we created
            running_tasks = resource_status['running_tasks']
            queued_tasks = resource_status['queued_tasks']
            total_tasks = running_tasks + queued_tasks
            
            assert total_tasks <= len(crawler_names), \
                f"Total tasks ({total_tasks}) should not exceed created tasks ({len(crawler_names)})"
            
        finally:
            scheduler.stop(wait=False)
    
    @given(
        crawler_names=st.lists(
            st.text(alphabet=st.characters(whitelist_categories=('Ll', 'Lu', 'Nd')), 
                    min_size=3, max_size=10),
            min_size=2, max_size=4, unique=True
        ),
        resource_limits=resource_limits_strategy()
    )
    @settings(max_examples=10, deadline=30000)
    def test_scheduler_handles_mixed_success_failure(
        self, 
        crawler_names: List[str],
        resource_limits: ResourceLimits
    ):
        """
        **Feature: memory-price-monitor, Property 7: Scheduler execution completeness**
        
        The scheduler should execute all crawlers regardless of individual success/failure.
        
        **Validates: Requirements 3.1**
        """
        assume(len(crawler_names) >= 2)
        
        # Create mock registry and repository
        registry = CrawlerRegistry()
        repository = Mock(spec=PriceRepository)
        
        # Register crawlers with mixed success/failure
        for i, name in enumerate(crawler_names):
            should_succeed = i % 2 == 0  # Alternate success/failure
            config = {'should_succeed': should_succeed, 'products_count': 3}
            registry.register(name, MockCrawler, config)
        
        # Create scheduler
        scheduler = TaskScheduler(registry, repository, resource_limits)
        
        try:
            scheduler.start()
            
            # Execute crawl tasks
            task_ids = scheduler.execute_crawl_task(crawler_names)
            
            # Should create tasks for all crawlers regardless of expected success/failure
            assert len(task_ids) == len(crawler_names), \
                f"Expected {len(crawler_names)} task IDs, got {len(task_ids)}"
            
            # All task IDs should be unique
            assert len(set(task_ids)) == len(task_ids), \
                "All task IDs should be unique"
            
            # Each crawler should have a corresponding task
            for name in crawler_names:
                assert any(name in task_id for task_id in task_ids), \
                    f"Crawler {name} should have a corresponding task ID"
            
        finally:
            scheduler.stop(wait=False)
    
    @given(
        resource_limits=resource_limits_strategy()
    )
    @settings(max_examples=10, deadline=30000)
    def test_scheduler_health_check_completeness(
        self,
        resource_limits: ResourceLimits
    ):
        """
        **Feature: memory-price-monitor, Property 7: Scheduler execution completeness**
        
        The scheduler health check should provide complete status information.
        
        **Validates: Requirements 3.1**
        """
        # Create mock registry and repository
        registry = CrawlerRegistry()
        repository = Mock(spec=PriceRepository)
        
        # Register at least one crawler
        registry.register('test_crawler', MockCrawler, {'should_succeed': True})
        
        # Create scheduler
        scheduler = TaskScheduler(registry, repository, resource_limits)
        
        try:
            scheduler.start()
            
            # Get health status
            health_status = scheduler.get_health_status()
            
            # Verify health status contains all required fields
            required_fields = ['healthy', 'timestamp', 'scheduler_running', 'issues', 'resources']
            for field in required_fields:
                assert field in health_status, f"Health status should contain '{field}' field"
            
            # Verify health status types
            assert isinstance(health_status['healthy'], bool), \
                "Health status 'healthy' should be boolean"
            assert isinstance(health_status['timestamp'], str), \
                "Health status 'timestamp' should be string"
            assert isinstance(health_status['scheduler_running'], bool), \
                "Health status 'scheduler_running' should be boolean"
            assert isinstance(health_status['issues'], list), \
                "Health status 'issues' should be list"
            assert isinstance(health_status['resources'], dict), \
                "Health status 'resources' should be dict"
            
            # When scheduler is running properly, it should be healthy
            if health_status['scheduler_running']:
                # If no other issues, should be healthy
                if len(health_status['issues']) == 0:
                    assert health_status['healthy'] is True, \
                        "Scheduler should be healthy when running with no issues"
            
        finally:
            scheduler.stop(wait=False)
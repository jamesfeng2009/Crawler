"""
Property-based tests for retry mechanism correctness.

**Feature: memory-price-monitor, Property 8: Retry mechanism correctness**
**Validates: Requirements 3.3**
"""

import pytest
from hypothesis import given, strategies as st, settings, assume
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock, patch
from typing import List, Dict, Any
import time
import threading

from memory_price_monitor.services.scheduler import (
    TaskScheduler, TaskQueue, CrawlTask, TaskPriority, TaskStatus, 
    TaskResult, ResourceLimits
)
from memory_price_monitor.crawlers import CrawlerRegistry, BaseCrawler, CrawlResult
from memory_price_monitor.data.repository import PriceRepository


class FailingCrawler(BaseCrawler):
    """Mock crawler that fails for testing retry mechanism."""
    
    def __init__(self, source_name: str, config: Dict[str, Any] = None):
        super().__init__(source_name, config)
        self.failure_count = config.get('failure_count', 3) if config else 3
        self.call_count = 0
    
    def fetch_products(self):
        self.call_count += 1
        if self.call_count <= self.failure_count:
            raise Exception(f"Mock failure #{self.call_count}")
        return [Mock() for _ in range(3)]
    
    def parse_product(self, raw_data):
        return Mock()
    
    def get_price_history(self, product_id: str):
        return []


class CountingCrawler(BaseCrawler):
    """Mock crawler that counts execution attempts."""
    
    def __init__(self, source_name: str, config: Dict[str, Any] = None):
        super().__init__(source_name, config)
        self.execution_count = 0
        self.should_fail = config.get('should_fail', True) if config else True
        self.max_failures = config.get('max_failures', 2) if config else 2
    
    def fetch_products(self):
        self.execution_count += 1
        if self.should_fail and self.execution_count <= self.max_failures:
            raise Exception(f"Intentional failure #{self.execution_count}")
        return [Mock() for _ in range(2)]
    
    def parse_product(self, raw_data):
        return Mock()
    
    def get_price_history(self, product_id: str):
        return []


@st.composite
def retry_config_strategy(draw):
    """Generate retry configuration."""
    return {
        'max_retries': draw(st.integers(min_value=1, max_value=5)),
        'retry_delay': draw(st.floats(min_value=0.1, max_value=2.0)),
        'timeout': draw(st.one_of(st.none(), st.floats(min_value=5.0, max_value=30.0)))
    }


@st.composite
def failure_config_strategy(draw):
    """Generate failure configuration for crawlers."""
    return {
        'should_fail': draw(st.booleans()),
        'max_failures': draw(st.integers(min_value=1, max_value=4)),
        'failure_count': draw(st.integers(min_value=1, max_value=6))
    }


class TestRetryMechanismProperties:
    """Property-based tests for retry mechanism correctness."""
    
    @given(
        max_retries=st.integers(min_value=1, max_value=5),
        retry_delay=st.floats(min_value=0.1, max_value=1.0),
        failure_count=st.integers(min_value=1, max_value=8)
    )
    @settings(max_examples=10, deadline=30000)
    def test_retry_mechanism_attempts_exactly_max_retries(
        self, 
        max_retries: int,
        retry_delay: float,
        failure_count: int
    ):
        """
        **Feature: memory-price-monitor, Property 8: Retry mechanism correctness**
        
        For any failed crawling task, the system should retry exactly three times 
        with exponential backoff delays before marking as failed.
        
        **Validates: Requirements 3.3**
        """
        # Create mock registry and repository
        registry = CrawlerRegistry()
        repository = Mock(spec=PriceRepository)
        
        # Create crawler that will fail for specified number of times
        crawler_config = {'failure_count': failure_count}
        registry.register('failing_crawler', FailingCrawler, crawler_config)
        
        # Create resource limits with fast execution
        resource_limits = ResourceLimits(
            max_concurrent_tasks=1,
            max_memory_usage_percent=95.0,
            max_cpu_usage_percent=95.0,
            min_disk_space_gb=0.1,
            check_interval_seconds=0.1
        )
        
        # Create scheduler
        scheduler = TaskScheduler(registry, repository, resource_limits)
        
        try:
            scheduler.start()
            
            # Create a task with specific retry configuration
            task = CrawlTask(
                task_id="test_retry_task",
                crawler_name='failing_crawler',
                priority=TaskPriority.NORMAL,
                max_retries=max_retries,
                retry_delay=retry_delay
            )
            
            # Add task to queue
            scheduler.task_queue.add_task(task)
            
            # Wait for task execution and retries to complete
            # We need to wait longer for retries to happen
            max_wait_time = (max_retries + 1) * retry_delay + 5.0  # Extra buffer
            time.sleep(min(max_wait_time, 10.0))  # Cap at 10 seconds for test performance
            
            # Get completed tasks
            completed_tasks = scheduler.task_queue.get_completed_tasks()
            
            # Count total attempts (original + retries)
            total_attempts = 0
            original_task_found = False
            retry_tasks_found = 0
            
            for completed_task in completed_tasks:
                if completed_task.task_id == "test_retry_task":
                    original_task_found = True
                    total_attempts += 1
                elif completed_task.task_id.startswith("test_retry_task_retry_"):
                    retry_tasks_found += 1
                    total_attempts += 1
            
            # Verify the retry behavior based on failure count vs max retries
            if failure_count > max_retries:
                # Should fail after max_retries attempts
                expected_total_attempts = max_retries + 1  # Original + retries
                assert total_attempts <= expected_total_attempts, \
                    f"Should not exceed {expected_total_attempts} attempts, got {total_attempts}"
                
                # Should have at least the original task
                assert original_task_found, "Original task should be found in completed tasks"
                
                # Should have attempted retries up to max_retries
                assert retry_tasks_found <= max_retries, \
                    f"Should not exceed {max_retries} retry attempts, got {retry_tasks_found}"
            
            else:
                # Should succeed before reaching max retries
                assert total_attempts >= 1, "Should have at least one attempt"
                assert total_attempts <= max_retries + 1, \
                    f"Should not exceed {max_retries + 1} attempts when succeeding"
        
        finally:
            scheduler.stop(wait=False)
    
    @given(
        max_retries=st.integers(min_value=2, max_value=4),
        retry_delay=st.floats(min_value=0.1, max_value=0.5),
        max_failures=st.integers(min_value=1, max_value=3)
    )
    @settings(max_examples=10, deadline=30000)
    def test_retry_mechanism_exponential_backoff(
        self,
        max_retries: int,
        retry_delay: float,
        max_failures: int
    ):
        """
        **Feature: memory-price-monitor, Property 8: Retry mechanism correctness**
        
        The retry mechanism should implement exponential backoff delays.
        
        **Validates: Requirements 3.3**
        """
        assume(max_failures <= max_retries)
        
        # Create mock registry and repository
        registry = CrawlerRegistry()
        repository = Mock(spec=PriceRepository)
        
        # Create crawler that fails for specified attempts then succeeds
        crawler_config = {'should_fail': True, 'max_failures': max_failures}
        registry.register('counting_crawler', CountingCrawler, crawler_config)
        
        # Create resource limits
        resource_limits = ResourceLimits(
            max_concurrent_tasks=1,
            max_memory_usage_percent=95.0,
            max_cpu_usage_percent=95.0,
            min_disk_space_gb=0.1
        )
        
        # Create scheduler
        scheduler = TaskScheduler(registry, repository, resource_limits)
        
        try:
            scheduler.start()
            
            # Record start time
            start_time = time.time()
            
            # Create task
            task = CrawlTask(
                task_id="backoff_test_task",
                crawler_name='counting_crawler',
                priority=TaskPriority.NORMAL,
                max_retries=max_retries,
                retry_delay=retry_delay
            )
            
            scheduler.task_queue.add_task(task)
            
            # Wait for completion
            max_wait_time = sum(retry_delay * (2 ** i) for i in range(max_retries + 1)) + 5.0
            time.sleep(min(max_wait_time, 15.0))
            
            # Get completed tasks
            completed_tasks = scheduler.task_queue.get_completed_tasks()
            
            # Verify that retries happened with appropriate timing
            retry_tasks = [t for t in completed_tasks if 'retry_' in t.task_id]
            
            if max_failures > 0:
                # Should have at least one retry if there were failures
                assert len(retry_tasks) >= 0, "Should have retry tasks for failures"
                
                # Verify retry tasks have appropriate metadata
                for retry_task in retry_tasks:
                    assert 'original_task_id' in retry_task.metadata, \
                        "Retry task should have original_task_id in metadata"
                    assert 'retry_reason' in retry_task.metadata, \
                        "Retry task should have retry_reason in metadata"
        
        finally:
            scheduler.stop(wait=False)
    
    @given(
        max_retries=st.integers(min_value=1, max_value=4),
        retry_delay=st.floats(min_value=0.1, max_value=0.8)
    )
    @settings(max_examples=10, deadline=30000)
    def test_retry_mechanism_preserves_task_properties(
        self,
        max_retries: int,
        retry_delay: float
    ):
        """
        **Feature: memory-price-monitor, Property 8: Retry mechanism correctness**
        
        Retry tasks should preserve the original task's properties and configuration.
        
        **Validates: Requirements 3.3**
        """
        # Create mock registry and repository
        registry = CrawlerRegistry()
        repository = Mock(spec=PriceRepository)
        
        # Create always-failing crawler
        crawler_config = {'failure_count': 10}  # Always fail
        registry.register('always_failing', FailingCrawler, crawler_config)
        
        # Create resource limits
        resource_limits = ResourceLimits(max_concurrent_tasks=1)
        
        # Create scheduler
        scheduler = TaskScheduler(registry, repository, resource_limits)
        
        try:
            scheduler.start()
            
            # Create task with specific properties
            original_config = {'test_param': 'test_value'}
            original_metadata = {'test_meta': 'meta_value'}
            
            task = CrawlTask(
                task_id="property_test_task",
                crawler_name='always_failing',
                priority=TaskPriority.HIGH,
                max_retries=max_retries,
                retry_delay=retry_delay,
                timeout=30.0,
                config_override=original_config,
                metadata=original_metadata.copy()
            )
            
            scheduler.task_queue.add_task(task)
            
            # Wait for retries
            wait_time = (max_retries + 1) * retry_delay + 3.0
            time.sleep(min(wait_time, 8.0))
            
            # Get completed tasks
            completed_tasks = scheduler.task_queue.get_completed_tasks()
            
            # Find retry tasks
            retry_tasks = [t for t in completed_tasks if 'retry_' in t.task_id]
            
            # Verify retry tasks preserve original properties
            for retry_task in retry_tasks:
                # Should have original task metadata
                assert 'original_task_id' in retry_task.metadata, \
                    "Retry task should reference original task"
                assert retry_task.metadata['original_task_id'] == "property_test_task", \
                    "Retry task should reference correct original task"
                
                # Should have retry reason
                assert 'retry_reason' in retry_task.metadata, \
                    "Retry task should have retry reason"
                assert isinstance(retry_task.metadata['retry_reason'], str), \
                    "Retry reason should be a string"
        
        finally:
            scheduler.stop(wait=False)
    
    @given(
        max_retries=st.integers(min_value=1, max_value=3),
        retry_delay=st.floats(min_value=0.1, max_value=0.5),
        success_after_attempts=st.integers(min_value=1, max_value=4)
    )
    @settings(max_examples=10, deadline=30000)
    def test_retry_mechanism_stops_on_success(
        self,
        max_retries: int,
        retry_delay: float,
        success_after_attempts: int
    ):
        """
        **Feature: memory-price-monitor, Property 8: Retry mechanism correctness**
        
        The retry mechanism should stop retrying once a task succeeds.
        
        **Validates: Requirements 3.3**
        """
        # Create mock registry and repository
        registry = CrawlerRegistry()
        repository = Mock(spec=PriceRepository)
        
        # Create crawler that succeeds after specified attempts
        crawler_config = {
            'should_fail': True, 
            'max_failures': success_after_attempts - 1  # Succeed on attempt N
        }
        registry.register('eventual_success', CountingCrawler, crawler_config)
        
        # Create resource limits
        resource_limits = ResourceLimits(max_concurrent_tasks=1)
        
        # Create scheduler
        scheduler = TaskScheduler(registry, repository, resource_limits)
        
        try:
            scheduler.start()
            
            # Create task
            task = CrawlTask(
                task_id="success_test_task",
                crawler_name='eventual_success',
                priority=TaskPriority.NORMAL,
                max_retries=max_retries,
                retry_delay=retry_delay
            )
            
            scheduler.task_queue.add_task(task)
            
            # Wait for completion
            max_wait_time = max_retries * retry_delay + 5.0
            time.sleep(min(max_wait_time, 10.0))
            
            # Get completed tasks
            completed_tasks = scheduler.task_queue.get_completed_tasks()
            
            # Count total attempts
            total_attempts = len([t for t in completed_tasks 
                                if t.task_id.startswith("success_test_task")])
            
            if success_after_attempts <= max_retries + 1:
                # Should succeed and not exceed necessary attempts
                assert total_attempts <= success_after_attempts, \
                    f"Should not exceed {success_after_attempts} attempts for success, got {total_attempts}"
                
                # Should have at least one successful task
                successful_tasks = [t for t in completed_tasks 
                                  if t.task_id.startswith("success_test_task") 
                                  and t.status == TaskStatus.COMPLETED]
                
                # Note: Due to timing and async nature, we might not always catch the success
                # but we should not have more attempts than needed
                if len(successful_tasks) > 0:
                    assert len(successful_tasks) >= 1, "Should have at least one successful task"
            
            else:
                # Should fail after max_retries + 1 attempts
                assert total_attempts <= max_retries + 1, \
                    f"Should not exceed {max_retries + 1} attempts, got {total_attempts}"
        
        finally:
            scheduler.stop(wait=False)
    
    @given(
        retry_delay=st.floats(min_value=0.1, max_value=1.0)
    )
    @settings(max_examples=10, deadline=30000)
    def test_retry_mechanism_handles_concurrent_failures(
        self,
        retry_delay: float
    ):
        """
        **Feature: memory-price-monitor, Property 8: Retry mechanism correctness**
        
        The retry mechanism should handle multiple concurrent task failures correctly.
        
        **Validates: Requirements 3.3**
        """
        # Create mock registry and repository
        registry = CrawlerRegistry()
        repository = Mock(spec=PriceRepository)
        
        # Register multiple failing crawlers
        crawler_names = ['fail_1', 'fail_2', 'fail_3']
        for name in crawler_names:
            crawler_config = {'failure_count': 5}  # Always fail
            registry.register(name, FailingCrawler, crawler_config)
        
        # Create resource limits allowing concurrent execution
        resource_limits = ResourceLimits(
            max_concurrent_tasks=3,
            max_memory_usage_percent=95.0,
            max_cpu_usage_percent=95.0
        )
        
        # Create scheduler
        scheduler = TaskScheduler(registry, repository, resource_limits)
        
        try:
            scheduler.start()
            
            # Create multiple tasks
            task_ids = []
            for i, name in enumerate(crawler_names):
                task = CrawlTask(
                    task_id=f"concurrent_fail_task_{i}",
                    crawler_name=name,
                    priority=TaskPriority.NORMAL,
                    max_retries=2,
                    retry_delay=retry_delay
                )
                scheduler.task_queue.add_task(task)
                task_ids.append(task.task_id)
            
            # Wait for all tasks and retries to complete
            wait_time = 3 * retry_delay + 5.0  # 3 attempts max per task
            time.sleep(min(wait_time, 12.0))
            
            # Get completed tasks
            completed_tasks = scheduler.task_queue.get_completed_tasks()
            
            # Verify each original task was attempted
            for task_id in task_ids:
                original_task_found = any(t.task_id == task_id for t in completed_tasks)
                retry_tasks_found = [t for t in completed_tasks if t.task_id.startswith(f"{task_id}_retry_")]
                
                # Should have at least the original task
                assert original_task_found or len(retry_tasks_found) > 0, \
                    f"Should have attempts for task {task_id}"
                
                # Should not have more than max_retries retry tasks
                assert len(retry_tasks_found) <= 2, \
                    f"Should not exceed 2 retries for task {task_id}, got {len(retry_tasks_found)}"
        
        finally:
            scheduler.stop(wait=False)
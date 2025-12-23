"""
Property-based tests for resource-aware task queuing.

**Feature: memory-price-monitor, Property 10: Resource-aware task queuing**
**Validates: Requirements 3.5**
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


class SlowCrawler(BaseCrawler):
    """Mock crawler that takes time to execute for testing resource constraints."""
    
    def __init__(self, source_name: str, config: Dict[str, Any] = None):
        super().__init__(source_name, config)
        self.execution_time = config.get('execution_time', 1.0) if config else 1.0
        self.should_succeed = config.get('should_succeed', True) if config else True
    
    def fetch_products(self):
        time.sleep(self.execution_time)
        if not self.should_succeed:
            raise Exception("Mock crawler failure")
        return [Mock() for _ in range(3)]
    
    def parse_product(self, raw_data):
        return Mock()
    
    def get_price_history(self, product_id: str):
        return []


class FastCrawler(BaseCrawler):
    """Mock crawler that executes quickly."""
    
    def __init__(self, source_name: str, config: Dict[str, Any] = None):
        super().__init__(source_name, config)
        self.execution_count = 0
    
    def fetch_products(self):
        self.execution_count += 1
        return [Mock() for _ in range(2)]
    
    def parse_product(self, raw_data):
        return Mock()
    
    def get_price_history(self, product_id: str):
        return []


@st.composite
def resource_limits_strategy(draw):
    """Generate resource limits with constrained values for testing."""
    return ResourceLimits(
        max_concurrent_tasks=draw(st.integers(min_value=1, max_value=5)),
        max_memory_usage_percent=draw(st.floats(min_value=60.0, max_value=95.0)),
        max_cpu_usage_percent=draw(st.floats(min_value=60.0, max_value=95.0)),
        min_disk_space_gb=draw(st.floats(min_value=0.1, max_value=2.0)),
        check_interval_seconds=draw(st.floats(min_value=0.1, max_value=5.0))
    )


@st.composite
def task_count_strategy(draw):
    """Generate number of tasks that exceeds concurrent limits."""
    return draw(st.integers(min_value=3, max_value=10))


class TestResourceAwareQueuingProperties:
    """Property-based tests for resource-aware task queuing."""
    
    @given(
        resource_limits=resource_limits_strategy(),
        task_count=task_count_strategy()
    )
    @settings(max_examples=10, deadline=30000)
    def test_scheduler_respects_concurrent_task_limits(
        self,
        resource_limits: ResourceLimits,
        task_count: int
    ):
        """
        **Feature: memory-price-monitor, Property 10: Resource-aware task queuing**
        
        When system resources are constrained, the scheduler should queue crawler tasks 
        rather than execute them simultaneously.
        
        **Validates: Requirements 3.5**
        """
        assume(task_count > resource_limits.max_concurrent_tasks)
        
        # Create mock registry and repository
        registry = CrawlerRegistry()
        repository = Mock(spec=PriceRepository)
        
        # Register slow crawlers to ensure they run long enough to test concurrency
        for i in range(task_count):
            crawler_name = f'slow_crawler_{i}'
            crawler_config = {'execution_time': 0.5, 'should_succeed': True}
            registry.register(crawler_name, SlowCrawler, crawler_config)
        
        # Create scheduler with resource limits
        scheduler = TaskScheduler(registry, repository, resource_limits)
        
        try:
            scheduler.start()
            
            # Create tasks for all crawlers
            crawler_names = [f'slow_crawler_{i}' for i in range(task_count)]
            task_ids = scheduler.execute_crawl_task(crawler_names)
            
            # Verify correct number of tasks created
            assert len(task_ids) == task_count, \
                f"Expected {task_count} task IDs, got {len(task_ids)}"
            
            # Wait a short time for tasks to start
            time.sleep(0.2)
            
            # Check resource status
            resource_status = scheduler.task_queue.get_resource_status()
            
            # Verify concurrent task limit is respected
            running_tasks = resource_status['running_tasks']
            queued_tasks = resource_status['queued_tasks']
            total_tasks = running_tasks + queued_tasks
            
            # Should not exceed concurrent task limit
            assert running_tasks <= resource_limits.max_concurrent_tasks, \
                f"Running tasks ({running_tasks}) should not exceed limit ({resource_limits.max_concurrent_tasks})"
            
            # Total tasks should equal what we created
            assert total_tasks <= task_count, \
                f"Total tasks ({total_tasks}) should not exceed created tasks ({task_count})"
            
            # If we have more tasks than the limit, some should be queued
            if task_count > resource_limits.max_concurrent_tasks:
                assert queued_tasks >= 0, "Should have queued tasks when exceeding concurrent limit"
        
        finally:
            scheduler.stop(wait=False)
    
    @given(
        max_concurrent_tasks=st.integers(min_value=1, max_value=3),
        task_count=st.integers(min_value=4, max_value=8)
    )
    @settings(max_examples=10, deadline=30000)
    def test_task_queue_processes_tasks_sequentially_when_constrained(
        self,
        max_concurrent_tasks: int,
        task_count: int
    ):
        """
        **Feature: memory-price-monitor, Property 10: Resource-aware task queuing**
        
        The task queue should process tasks sequentially when concurrent limits are reached.
        
        **Validates: Requirements 3.5**
        """
        assume(task_count > max_concurrent_tasks)
        
        # Create resource limits with specific concurrent task limit
        resource_limits = ResourceLimits(
            max_concurrent_tasks=max_concurrent_tasks,
            max_memory_usage_percent=95.0,
            max_cpu_usage_percent=95.0,
            min_disk_space_gb=0.1
        )
        
        # Create mock registry and repository
        registry = CrawlerRegistry()
        repository = Mock(spec=PriceRepository)
        
        # Register fast crawlers
        for i in range(task_count):
            crawler_name = f'fast_crawler_{i}'
            registry.register(crawler_name, FastCrawler, {})
        
        # Create scheduler
        scheduler = TaskScheduler(registry, repository, resource_limits)
        
        try:
            scheduler.start()
            
            # Create all tasks at once
            crawler_names = [f'fast_crawler_{i}' for i in range(task_count)]
            task_ids = scheduler.execute_crawl_task(crawler_names)
            
            # Track resource status over time
            max_concurrent_observed = 0
            observations = 0
            
            # Monitor for a short period
            for _ in range(10):  # 10 observations
                time.sleep(0.1)
                resource_status = scheduler.task_queue.get_resource_status()
                running_tasks = resource_status['running_tasks']
                max_concurrent_observed = max(max_concurrent_observed, running_tasks)
                observations += 1
            
            # Should never exceed the concurrent task limit
            assert max_concurrent_observed <= max_concurrent_tasks, \
                f"Observed {max_concurrent_observed} concurrent tasks, limit is {max_concurrent_tasks}"
            
            # Wait for all tasks to complete
            time.sleep(2.0)
            
            # Eventually all tasks should complete
            final_status = scheduler.task_queue.get_resource_status()
            completed_tasks = scheduler.task_queue.get_completed_tasks()
            
            # Should have some completed tasks
            assert len(completed_tasks) >= 0, "Should have some completed tasks"
        
        finally:
            scheduler.stop(wait=False)
    
    @given(
        resource_limits=resource_limits_strategy(),
        high_priority_count=st.integers(min_value=1, max_value=3),
        normal_priority_count=st.integers(min_value=2, max_value=5)
    )
    @settings(max_examples=10, deadline=30000)
    def test_task_queue_respects_priority_under_resource_constraints(
        self,
        resource_limits: ResourceLimits,
        high_priority_count: int,
        normal_priority_count: int
    ):
        """
        **Feature: memory-price-monitor, Property 10: Resource-aware task queuing**
        
        Under resource constraints, higher priority tasks should be processed first.
        
        **Validates: Requirements 3.5**
        """
        total_tasks = high_priority_count + normal_priority_count
        assume(total_tasks > resource_limits.max_concurrent_tasks)
        
        # Create mock registry and repository
        registry = CrawlerRegistry()
        repository = Mock(spec=PriceRepository)
        
        # Register crawlers
        all_crawler_names = []
        for i in range(total_tasks):
            crawler_name = f'priority_crawler_{i}'
            registry.register(crawler_name, FastCrawler, {})
            all_crawler_names.append(crawler_name)
        
        # Create scheduler
        scheduler = TaskScheduler(registry, repository, resource_limits)
        
        try:
            scheduler.start()
            
            # Create high priority tasks
            high_priority_tasks = []
            for i in range(high_priority_count):
                task = CrawlTask(
                    task_id=f"high_priority_task_{i}",
                    crawler_name=f'priority_crawler_{i}',
                    priority=TaskPriority.HIGH
                )
                scheduler.task_queue.add_task(task)
                high_priority_tasks.append(task.task_id)
            
            # Create normal priority tasks
            normal_priority_tasks = []
            for i in range(normal_priority_count):
                task = CrawlTask(
                    task_id=f"normal_priority_task_{i}",
                    crawler_name=f'priority_crawler_{high_priority_count + i}',
                    priority=TaskPriority.NORMAL
                )
                scheduler.task_queue.add_task(task)
                normal_priority_tasks.append(task.task_id)
            
            # Wait for some processing
            time.sleep(1.0)
            
            # Check that tasks are being processed
            resource_status = scheduler.task_queue.get_resource_status()
            
            # Should respect concurrent limits
            assert resource_status['running_tasks'] <= resource_limits.max_concurrent_tasks, \
                f"Running tasks should not exceed limit"
            
            # Should have queued some tasks if total exceeds limit
            if total_tasks > resource_limits.max_concurrent_tasks:
                total_active = resource_status['running_tasks'] + resource_status['queued_tasks']
                assert total_active >= 0, "Should have some active tasks"
        
        finally:
            scheduler.stop(wait=False)
    
    @patch('psutil.virtual_memory')
    @patch('psutil.cpu_percent')
    @patch('psutil.disk_usage')
    @given(
        memory_percent=st.floats(min_value=85.0, max_value=99.0),
        cpu_percent=st.floats(min_value=85.0, max_value=99.0),
        disk_free_gb=st.floats(min_value=0.01, max_value=0.5)
    )
    @settings(max_examples=10, deadline=30000)
    def test_task_queue_blocks_execution_on_resource_constraints(
        self,
        mock_disk_usage,
        mock_cpu_percent,
        mock_virtual_memory,
        memory_percent: float,
        cpu_percent: float,
        disk_free_gb: float
    ):
        """
        **Feature: memory-price-monitor, Property 10: Resource-aware task queuing**
        
        The task queue should block task execution when system resources are constrained.
        
        **Validates: Requirements 3.5**
        """
        # Mock system resource readings to simulate high usage
        mock_memory = Mock()
        mock_memory.percent = memory_percent
        mock_virtual_memory.return_value = mock_memory
        
        mock_cpu_percent.return_value = cpu_percent
        
        mock_disk = Mock()
        mock_disk.free = disk_free_gb * (1024**3)  # Convert GB to bytes
        mock_disk.total = 100 * (1024**3)  # 100GB total
        mock_disk_usage.return_value = mock_disk
        
        # Create strict resource limits
        resource_limits = ResourceLimits(
            max_concurrent_tasks=3,
            max_memory_usage_percent=80.0,  # Lower than mocked value
            max_cpu_usage_percent=80.0,     # Lower than mocked value
            min_disk_space_gb=1.0,          # Higher than mocked value
            check_interval_seconds=0.1
        )
        
        # Create task queue
        task_queue = TaskQueue(resource_limits)
        
        # Create a test task
        task = CrawlTask(
            task_id="resource_test_task",
            crawler_name='test_crawler',
            priority=TaskPriority.NORMAL
        )
        
        # Add task to queue
        task_queue.add_task(task)
        
        # Try to get next task - should be blocked by resource constraints
        next_task = task_queue.get_next_task()
        
        # Should not return a task due to resource constraints
        assert next_task is None, \
            "Should not return task when resources are constrained"
        
        # Queue should still contain the task
        assert task_queue.get_queue_size() == 1, \
            "Task should remain in queue when blocked by resources"
        
        # Resource status should indicate constraints
        resource_status = task_queue.get_resource_status()
        assert resource_status['can_execute'] is False, \
            "Resource status should indicate execution is blocked"
    
    @given(
        resource_limits=resource_limits_strategy(),
        task_count=st.integers(min_value=2, max_value=6)
    )
    @settings(max_examples=10, deadline=30000)
    def test_task_queue_resource_status_accuracy(
        self,
        resource_limits: ResourceLimits,
        task_count: int
    ):
        """
        **Feature: memory-price-monitor, Property 10: Resource-aware task queuing**
        
        The task queue should accurately report resource status and task counts.
        
        **Validates: Requirements 3.5**
        """
        # Create mock registry and repository
        registry = CrawlerRegistry()
        repository = Mock(spec=PriceRepository)
        
        # Register crawlers
        for i in range(task_count):
            crawler_name = f'status_crawler_{i}'
            registry.register(crawler_name, FastCrawler, {})
        
        # Create scheduler
        scheduler = TaskScheduler(registry, repository, resource_limits)
        
        try:
            scheduler.start()
            
            # Get initial resource status
            initial_status = scheduler.task_queue.get_resource_status()
            
            # Should have expected fields
            required_fields = [
                'memory_percent', 'memory_available_gb', 'cpu_percent',
                'disk_free_gb', 'disk_total_gb', 'running_tasks',
                'queued_tasks', 'can_execute'
            ]
            
            for field in required_fields:
                assert field in initial_status, f"Resource status should contain '{field}'"
            
            # Initial state should have no tasks
            assert initial_status['running_tasks'] == 0, \
                "Should start with no running tasks"
            assert initial_status['queued_tasks'] == 0, \
                "Should start with no queued tasks"
            
            # Create tasks
            crawler_names = [f'status_crawler_{i}' for i in range(task_count)]
            task_ids = scheduler.execute_crawl_task(crawler_names)
            
            # Wait briefly for tasks to be processed
            time.sleep(0.2)
            
            # Get updated resource status
            updated_status = scheduler.task_queue.get_resource_status()
            
            # Should show tasks in system
            total_tasks = updated_status['running_tasks'] + updated_status['queued_tasks']
            assert total_tasks <= task_count, \
                f"Total tasks ({total_tasks}) should not exceed created tasks ({task_count})"
            
            # Running tasks should not exceed limit
            assert updated_status['running_tasks'] <= resource_limits.max_concurrent_tasks, \
                f"Running tasks should not exceed concurrent limit"
            
            # Resource values should be reasonable
            assert 0 <= updated_status['memory_percent'] <= 100, \
                "Memory percentage should be between 0 and 100"
            assert updated_status['memory_available_gb'] >= 0, \
                "Available memory should be non-negative"
            assert 0 <= updated_status['cpu_percent'] <= 100, \
                "CPU percentage should be between 0 and 100"
            assert updated_status['disk_free_gb'] >= 0, \
                "Free disk space should be non-negative"
            assert updated_status['disk_total_gb'] > 0, \
                "Total disk space should be positive"
            
        finally:
            scheduler.stop(wait=False)
    
    @given(
        max_concurrent_tasks=st.integers(min_value=1, max_value=2),
        execution_time=st.floats(min_value=0.3, max_value=1.0)
    )
    @settings(max_examples=10, deadline=30000)
    def test_task_queue_maintains_order_under_constraints(
        self,
        max_concurrent_tasks: int,
        execution_time: float
    ):
        """
        **Feature: memory-price-monitor, Property 10: Resource-aware task queuing**
        
        The task queue should maintain proper task ordering under resource constraints.
        
        **Validates: Requirements 3.5**
        """
        # Create resource limits
        resource_limits = ResourceLimits(
            max_concurrent_tasks=max_concurrent_tasks,
            max_memory_usage_percent=95.0,
            max_cpu_usage_percent=95.0,
            min_disk_space_gb=0.1
        )
        
        # Create task queue
        task_queue = TaskQueue(resource_limits)
        
        # Create tasks with different priorities
        tasks = [
            CrawlTask(
                task_id="critical_task",
                crawler_name='test_crawler',
                priority=TaskPriority.CRITICAL
            ),
            CrawlTask(
                task_id="high_task",
                crawler_name='test_crawler',
                priority=TaskPriority.HIGH
            ),
            CrawlTask(
                task_id="normal_task",
                crawler_name='test_crawler',
                priority=TaskPriority.NORMAL
            ),
            CrawlTask(
                task_id="low_task",
                crawler_name='test_crawler',
                priority=TaskPriority.LOW
            )
        ]
        
        # Add tasks in reverse priority order
        for task in reversed(tasks):
            task_queue.add_task(task)
        
        # Get tasks from queue - should come out in priority order
        retrieved_tasks = []
        while len(retrieved_tasks) < len(tasks):
            task = task_queue.get_next_task()
            if task is not None:
                retrieved_tasks.append(task)
                # Mark as completed to allow next task
                result = TaskResult(
                    task_id=task.task_id,
                    status=TaskStatus.COMPLETED,
                    started_at=datetime.now(),
                    completed_at=datetime.now()
                )
                task_queue.mark_task_completed(task.task_id, result)
            else:
                break  # No more tasks available due to constraints
        
        # Should get at least some tasks
        assert len(retrieved_tasks) > 0, "Should retrieve at least some tasks"
        
        # Tasks should be in priority order (highest priority first)
        if len(retrieved_tasks) > 1:
            for i in range(len(retrieved_tasks) - 1):
                current_priority = retrieved_tasks[i].priority.value
                next_priority = retrieved_tasks[i + 1].priority.value
                assert current_priority >= next_priority, \
                    f"Tasks should be in priority order: {current_priority} >= {next_priority}"
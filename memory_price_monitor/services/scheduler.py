"""
Task scheduler and orchestration for memory price monitoring.
"""

import threading
import time
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
import queue
import psutil
import json
import uuid
import copy
from threading import Lock, RLock

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_JOB_MISSED
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.pool import ThreadPoolExecutor as APSThreadPoolExecutor

from memory_price_monitor.utils.logging import get_business_logger
from memory_price_monitor.utils.errors import SchedulerError
from memory_price_monitor.crawlers import CrawlerRegistry, BaseCrawler, CrawlResult
from memory_price_monitor.data.repository import PriceRepository
from memory_price_monitor.data.models import StandardizedProduct


logger = get_business_logger('scheduler')


class TaskStatus(Enum):
    """Task execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    CANCELLED = "cancelled"


class TaskPriority(Enum):
    """Task priority levels."""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4


@dataclass
class ConcurrentExecutionMetrics:
    """Metrics for concurrent crawler execution."""
    execution_id: str
    crawler_name: str
    thread_id: int
    started_at: datetime
    success: bool = False
    products_extracted: int = 0
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    memory_usage_mb: Optional[float] = None
    cpu_usage_percent: Optional[float] = None
    error_message: Optional[str] = None
    isolation_violations: List[str] = field(default_factory=list)


@dataclass
class CrawlerIsolationContext:
    """Context for maintaining crawler isolation."""
    execution_id: str
    crawler_name: str
    thread_id: int
    start_time: datetime
    temp_files: List[str] = field(default_factory=list)
    network_connections: List[str] = field(default_factory=list)
    memory_baseline: Optional[float] = None
    cpu_baseline: Optional[float] = None
    shared_resources_accessed: List[str] = field(default_factory=list)


@dataclass
class TaskResult:
    """Result of a task execution."""
    task_id: str
    status: TaskStatus
    started_at: datetime
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    result_data: Optional[Any] = None
    error_message: Optional[str] = None
    retry_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    concurrent_metrics: Optional[ConcurrentExecutionMetrics] = None


class ConcurrentCrawlerManager:
    """Manager for concurrent crawler execution with isolation monitoring."""
    
    def __init__(self, max_concurrent: int = 3):
        """
        Initialize concurrent crawler manager.
        
        Args:
            max_concurrent: Maximum number of concurrent crawlers
        """
        self.max_concurrent = max_concurrent
        self._active_executions: Dict[str, CrawlerIsolationContext] = {}
        self._execution_metrics: Dict[str, ConcurrentExecutionMetrics] = {}
        self._isolation_lock = RLock()
        self._shared_resource_registry: Dict[str, List[str]] = {}
        self.logger = get_business_logger('scheduler')
    
    def start_execution(self, crawler_name: str) -> str:
        """
        Start a new crawler execution with isolation monitoring.
        
        Args:
            crawler_name: Name of the crawler to execute
            
        Returns:
            Execution ID for tracking
        """
        execution_id = str(uuid.uuid4())
        thread_id = threading.get_ident()
        
        with self._isolation_lock:
            # Check concurrent limit
            if len(self._active_executions) >= self.max_concurrent:
                raise SchedulerError(f"Maximum concurrent crawlers ({self.max_concurrent}) already running")
            
            # Create isolation context
            context = CrawlerIsolationContext(
                execution_id=execution_id,
                crawler_name=crawler_name,
                thread_id=thread_id,
                start_time=datetime.now()
            )
            
            # Record baseline resource usage
            try:
                process = psutil.Process()
                context.memory_baseline = process.memory_info().rss / (1024 * 1024)  # MB
                context.cpu_baseline = process.cpu_percent()
            except Exception as e:
                self.logger.warning(f"Could not record baseline metrics: {e}")
            
            self._active_executions[execution_id] = context
            
            # Initialize metrics
            metrics = ConcurrentExecutionMetrics(
                execution_id=execution_id,
                crawler_name=crawler_name,
                thread_id=thread_id,
                started_at=datetime.now()
            )
            self._execution_metrics[execution_id] = metrics
            
            self.logger.info(f"Started crawler execution: {crawler_name} (ID: {execution_id})")
            
        return execution_id
    
    def end_execution(self, execution_id: str, success: bool = True, 
                     error_message: Optional[str] = None, 
                     products_extracted: int = 0) -> ConcurrentExecutionMetrics:
        """
        End a crawler execution and collect metrics.
        
        Args:
            execution_id: Execution ID to end
            success: Whether execution was successful
            error_message: Optional error message
            products_extracted: Number of products extracted
            
        Returns:
            Execution metrics
        """
        with self._isolation_lock:
            if execution_id not in self._active_executions:
                raise SchedulerError(f"Execution ID {execution_id} not found")
            
            context = self._active_executions[execution_id]
            metrics = self._execution_metrics[execution_id]
            
            # Update metrics
            metrics.completed_at = datetime.now()
            metrics.duration_seconds = (metrics.completed_at - metrics.started_at).total_seconds()
            metrics.success = success
            metrics.error_message = error_message
            metrics.products_extracted = products_extracted
            
            # Record final resource usage
            try:
                process = psutil.Process()
                current_memory = process.memory_info().rss / (1024 * 1024)  # MB
                current_cpu = process.cpu_percent()
                
                if context.memory_baseline:
                    metrics.memory_usage_mb = current_memory - context.memory_baseline
                else:
                    metrics.memory_usage_mb = current_memory
                    
                metrics.cpu_usage_percent = current_cpu
                
            except Exception as e:
                self.logger.warning(f"Could not record final metrics: {e}")
            
            # Check for isolation violations
            violations = self._check_isolation_violations(context)
            metrics.isolation_violations = violations
            
            # Clean up
            del self._active_executions[execution_id]
            
            self.logger.info(f"Ended crawler execution: {context.crawler_name} "
                           f"(ID: {execution_id}, success: {success}, "
                           f"violations: {len(violations)})")
            
            return metrics
    
    def register_shared_resource_access(self, execution_id: str, resource_name: str) -> None:
        """
        Register access to a shared resource.
        
        Args:
            execution_id: Execution ID accessing the resource
            resource_name: Name of the shared resource
        """
        with self._isolation_lock:
            if execution_id not in self._active_executions:
                return
            
            context = self._active_executions[execution_id]
            context.shared_resources_accessed.append(resource_name)
            
            # Track which executions are accessing this resource
            if resource_name not in self._shared_resource_registry:
                self._shared_resource_registry[resource_name] = []
            
            if execution_id not in self._shared_resource_registry[resource_name]:
                self._shared_resource_registry[resource_name].append(execution_id)
    
    def get_active_executions(self) -> List[CrawlerIsolationContext]:
        """Get list of currently active executions."""
        with self._isolation_lock:
            return list(self._active_executions.values())
    
    def get_execution_metrics(self, execution_id: str) -> Optional[ConcurrentExecutionMetrics]:
        """Get metrics for a specific execution."""
        return self._execution_metrics.get(execution_id)
    
    def get_all_metrics(self, limit: int = 100) -> List[ConcurrentExecutionMetrics]:
        """
        Get all execution metrics.
        
        Args:
            limit: Maximum number of metrics to return
            
        Returns:
            List of execution metrics, most recent first
        """
        metrics = list(self._execution_metrics.values())
        # Sort by start time, most recent first
        metrics.sort(key=lambda x: x.started_at, reverse=True)
        return metrics[:limit]
    
    def _check_isolation_violations(self, context: CrawlerIsolationContext) -> List[str]:
        """
        Check for isolation violations during execution.
        
        Args:
            context: Execution context to check
            
        Returns:
            List of violation descriptions
        """
        violations = []
        
        # Check for concurrent access to shared resources
        for resource_name in context.shared_resources_accessed:
            if resource_name in self._shared_resource_registry:
                concurrent_accesses = [
                    exec_id for exec_id in self._shared_resource_registry[resource_name]
                    if exec_id != context.execution_id and exec_id in self._active_executions
                ]
                
                if concurrent_accesses:
                    violations.append(
                        f"Concurrent access to shared resource '{resource_name}' "
                        f"with executions: {concurrent_accesses}"
                    )
        
        # Check for excessive resource usage that might affect other crawlers
        if context.memory_baseline and hasattr(self, '_execution_metrics'):
            metrics = self._execution_metrics.get(context.execution_id)
            if metrics and metrics.memory_usage_mb and metrics.memory_usage_mb > 500:  # 500MB threshold
                violations.append(f"Excessive memory usage: {metrics.memory_usage_mb:.1f}MB")
        
        return violations
    
    def cleanup_completed_executions(self, max_age_hours: int = 24) -> int:
        """
        Clean up old execution metrics.
        
        Args:
            max_age_hours: Maximum age of metrics to keep
            
        Returns:
            Number of metrics cleaned up
        """
        cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
        
        old_execution_ids = [
            exec_id for exec_id, metrics in self._execution_metrics.items()
            if metrics.completed_at and metrics.completed_at < cutoff_time
        ]
        
        for exec_id in old_execution_ids:
            del self._execution_metrics[exec_id]
        
        # Clean up shared resource registry
        for resource_name in list(self._shared_resource_registry.keys()):
            self._shared_resource_registry[resource_name] = [
                exec_id for exec_id in self._shared_resource_registry[resource_name]
                if exec_id not in old_execution_ids
            ]
            
            # Remove empty resource entries
            if not self._shared_resource_registry[resource_name]:
                del self._shared_resource_registry[resource_name]
        
        if old_execution_ids:
            self.logger.info(f"Cleaned up {len(old_execution_ids)} old execution metrics")
        
        return len(old_execution_ids)
    """Crawling task definition."""
    task_id: str
    crawler_name: str
    priority: TaskPriority = TaskPriority.NORMAL
    max_retries: int = 3
    retry_delay: float = 60.0  # seconds
    timeout: Optional[float] = None
    config_override: Optional[Dict[str, Any]] = None
    created_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CrawlTask:
    """Crawling task definition."""
    task_id: str
    crawler_name: str
    priority: TaskPriority = TaskPriority.NORMAL
    max_retries: int = 3
    retry_delay: float = 60.0  # seconds
    timeout: Optional[float] = None
    config_override: Optional[Dict[str, Any]] = None
    created_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)
    execution_id: Optional[str] = None  # For tracking concurrent execution


@dataclass
class ResourceLimits:
    """System resource limits for task execution."""
    max_concurrent_tasks: int = 3
    max_memory_usage_percent: float = 80.0
    max_cpu_usage_percent: float = 80.0
    min_disk_space_gb: float = 1.0
    check_interval_seconds: float = 30.0


class TaskQueue:
    """Priority-based task queue with resource awareness."""
    
    def __init__(self, resource_limits: ResourceLimits):
        """
        Initialize task queue.
        
        Args:
            resource_limits: Resource limits configuration
        """
        self.resource_limits = resource_limits
        self._queue = queue.PriorityQueue()
        self._running_tasks: Dict[str, CrawlTask] = {}
        self._completed_tasks: Dict[str, TaskResult] = {}
        self._lock = threading.RLock()
        self.logger = get_business_logger('scheduler')
    
    def add_task(self, task: CrawlTask) -> None:
        """
        Add a task to the queue.
        
        Args:
            task: Crawl task to add
        """
        with self._lock:
            # Priority queue uses tuple (priority, task_id, task)
            # Lower priority number = higher priority
            priority_value = -task.priority.value  # Negative for correct ordering
            self._queue.put((priority_value, task.task_id, task))
            self.logger.info(f"Task added to queue: {task.task_id} (priority: {task.priority.name})")
    
    def get_next_task(self) -> Optional[CrawlTask]:
        """
        Get the next task from the queue if resources allow.
        
        Returns:
            Next task to execute or None if no tasks or resource constraints
        """
        with self._lock:
            if self._queue.empty():
                return None
            
            # Check resource constraints
            if not self._can_execute_task():
                self.logger.debug("Resource constraints prevent task execution")
                return None
            
            try:
                priority, task_id, task = self._queue.get_nowait()
                self._running_tasks[task_id] = task
                self.logger.info(f"Task retrieved from queue: {task_id}")
                return task
            except queue.Empty:
                return None
    
    def mark_task_completed(self, task_id: str, result: TaskResult) -> None:
        """
        Mark a task as completed and remove from running tasks.
        
        Args:
            task_id: Task identifier
            result: Task execution result
        """
        with self._lock:
            if task_id in self._running_tasks:
                del self._running_tasks[task_id]
            self._completed_tasks[task_id] = result
            self.logger.info(f"Task marked as completed: {task_id} (status: {result.status.name})")
    
    def get_running_tasks(self) -> List[CrawlTask]:
        """Get list of currently running tasks."""
        with self._lock:
            return list(self._running_tasks.values())
    
    def get_queue_size(self) -> int:
        """Get number of tasks in queue."""
        return self._queue.qsize()
    
    def get_completed_tasks(self, limit: int = 100) -> List[TaskResult]:
        """
        Get recent completed tasks.
        
        Args:
            limit: Maximum number of results to return
            
        Returns:
            List of completed task results
        """
        with self._lock:
            # Sort by completion time, most recent first
            sorted_tasks = sorted(
                self._completed_tasks.values(),
                key=lambda x: x.completed_at or datetime.min,
                reverse=True
            )
            return sorted_tasks[:limit]
    
    def _can_execute_task(self) -> bool:
        """
        Check if system resources allow executing another task.
        
        Returns:
            True if task can be executed
        """
        # Check concurrent task limit
        if len(self._running_tasks) >= self.resource_limits.max_concurrent_tasks:
            return False
        
        try:
            # Check memory usage
            memory_percent = psutil.virtual_memory().percent
            if memory_percent > self.resource_limits.max_memory_usage_percent:
                self.logger.warning(f"Memory usage too high: {memory_percent}%")
                return False
            
            # Check CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            if cpu_percent > self.resource_limits.max_cpu_usage_percent:
                self.logger.warning(f"CPU usage too high: {cpu_percent}%")
                return False
            
            # Check disk space
            disk_usage = psutil.disk_usage('/')
            free_gb = disk_usage.free / (1024**3)
            if free_gb < self.resource_limits.min_disk_space_gb:
                self.logger.warning(f"Disk space too low: {free_gb:.2f}GB")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error checking system resources: {e}")
            # If we can't check resources, be conservative and don't execute
            return False
    
    def get_resource_status(self) -> Dict[str, Any]:
        """
        Get current system resource status.
        
        Returns:
            Dictionary with resource information
        """
        try:
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            return {
                'memory_percent': memory.percent,
                'memory_available_gb': memory.available / (1024**3),
                'cpu_percent': psutil.cpu_percent(interval=1),
                'disk_free_gb': disk.free / (1024**3),
                'disk_total_gb': disk.total / (1024**3),
                'running_tasks': len(self._running_tasks),
                'queued_tasks': self.get_queue_size(),
                'can_execute': self._can_execute_task()
            }
        except Exception as e:
            self.logger.error(f"Error getting resource status: {e}")
            return {
                'error': str(e),
                'running_tasks': len(self._running_tasks),
                'queued_tasks': self.get_queue_size(),
                'can_execute': False
            }


class TaskScheduler:
    """Main task scheduler for coordinating crawling operations."""
    
    def __init__(
        self,
        crawler_registry: CrawlerRegistry,
        repository: PriceRepository,
        resource_limits: Optional[ResourceLimits] = None,
        state_manager: Optional['StateManager'] = None
    ):
        """
        Initialize task scheduler.
        
        Args:
            crawler_registry: Registry of available crawlers
            repository: Data repository for storing results
            resource_limits: Optional resource limits configuration
            state_manager: Optional state manager for persistence
        """
        self.crawler_registry = crawler_registry
        self.repository = repository
        self.resource_limits = resource_limits or ResourceLimits()
        
        # Initialize state manager
        if state_manager is None:
            from memory_price_monitor.services.state_manager import StateManager
            self.state_manager = StateManager()
        else:
            self.state_manager = state_manager
        
        # Initialize concurrent crawler manager
        self.concurrent_manager = ConcurrentCrawlerManager(
            max_concurrent=self.resource_limits.max_concurrent_tasks
        )
        
        # Initialize task queue
        self.task_queue = TaskQueue(self.resource_limits)
        
        # Initialize APScheduler
        self.scheduler = BackgroundScheduler(
            jobstores={'default': MemoryJobStore()},
            executors={'default': APSThreadPoolExecutor(max_workers=1)},
            job_defaults={'coalesce': True, 'max_instances': 1}
        )
        
        # Thread pool for task execution
        self.executor = ThreadPoolExecutor(
            max_workers=self.resource_limits.max_concurrent_tasks,
            thread_name_prefix="crawler_task"
        )
        
        # Task execution state
        self._running = False
        self._task_futures: Dict[str, Future] = {}
        self._last_successful_crawl: Dict[str, datetime] = {}
        
        # Add scheduler event listeners
        self.scheduler.add_listener(self._job_executed, EVENT_JOB_EXECUTED)
        self.scheduler.add_listener(self._job_error, EVENT_JOB_ERROR)
        self.scheduler.add_listener(self._job_missed, EVENT_JOB_MISSED)
        
        self.logger = get_business_logger('scheduler')
    
    def start(self) -> None:
        """Start the scheduler and task execution loop."""
        if self._running:
            self.logger.warning("Scheduler is already running")
            return
        
        try:
            self.scheduler.start()
            self._running = True
            
            # Update state manager
            self.state_manager.set_scheduler_running(True)
            
            # Check if we're in recovery mode
            if self.state_manager.is_recovery_mode():
                self.logger.info("Starting in recovery mode - previous state restored")
                # Load previous success timestamps
                for crawler_name, crawler_state in self.state_manager.get_all_crawler_states().items():
                    if crawler_state.last_successful_crawl:
                        self._last_successful_crawl[crawler_name] = crawler_state.last_successful_crawl
                
                # Clear recovery mode after successful startup
                self.state_manager.clear_recovery_mode()
            
            # Start task execution loop
            self._start_task_execution_loop()
            
            self.logger.info("Task scheduler started successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to start scheduler: {e}")
            self.state_manager.set_scheduler_running(False)
            raise SchedulerError(f"Failed to start scheduler: {e}")
    
    def stop(self, wait: bool = True) -> None:
        """
        Stop the scheduler and task execution.
        
        Args:
            wait: Whether to wait for running tasks to complete
        """
        if not self._running:
            self.logger.warning("Scheduler is not running")
            return
        
        try:
            self._running = False
            
            # Update state manager
            self.state_manager.set_scheduler_running(False)
            
            # Stop APScheduler
            self.scheduler.shutdown(wait=wait)
            
            # Cancel running tasks if not waiting
            if not wait:
                for future in self._task_futures.values():
                    future.cancel()
            
            # Shutdown executor
            self.executor.shutdown(wait=wait)
            
            # Save final state
            self.state_manager.save_state()
            
            self.logger.info("Task scheduler stopped")
            
        except Exception as e:
            self.logger.error(f"Error stopping scheduler: {e}")
            raise SchedulerError(f"Error stopping scheduler: {e}")
    
    def schedule_daily_crawl(self, hour: int = 10, minute: int = 0) -> None:
        """
        Schedule daily crawling task.
        
        Args:
            hour: Hour to run (0-23)
            minute: Minute to run (0-59)
        """
        try:
            # Remove existing daily crawl job if it exists
            try:
                self.scheduler.remove_job('daily_crawl')
            except:
                pass
            
            # Add new daily crawl job
            trigger = CronTrigger(hour=hour, minute=minute)
            self.scheduler.add_job(
                func=self._execute_daily_crawl,
                trigger=trigger,
                id='daily_crawl',
                name='Daily Crawl Task',
                replace_existing=True
            )
            
            self.logger.info(f"Daily crawl scheduled for {hour:02d}:{minute:02d}")
            
        except Exception as e:
            self.logger.error(f"Failed to schedule daily crawl: {e}")
            raise SchedulerError(f"Failed to schedule daily crawl: {e}")
    
    def schedule_weekly_report(self, day_of_week: int = 0, hour: int = 9, minute: int = 0) -> None:
        """
        Schedule weekly report generation.
        
        Args:
            day_of_week: Day of week (0=Monday, 6=Sunday)
            hour: Hour to run (0-23)
            minute: Minute to run (0-59)
        """
        try:
            # Remove existing weekly report job if it exists
            try:
                self.scheduler.remove_job('weekly_report')
            except:
                pass
            
            # Add new weekly report job
            trigger = CronTrigger(day_of_week=day_of_week, hour=hour, minute=minute)
            self.scheduler.add_job(
                func=self._execute_weekly_report,
                trigger=trigger,
                id='weekly_report',
                name='Weekly Report Task',
                replace_existing=True
            )
            
            self.logger.info(f"Weekly report scheduled for day {day_of_week} at {hour:02d}:{minute:02d}")
            
        except Exception as e:
            self.logger.error(f"Failed to schedule weekly report: {e}")
            raise SchedulerError(f"Failed to schedule weekly report: {e}")
    
    def execute_crawl_task(self, crawler_names: Optional[List[str]] = None) -> List[str]:
        """
        Execute crawling task immediately.
        
        Args:
            crawler_names: Optional list of specific crawlers to run
            
        Returns:
            List of task IDs created
        """
        try:
            if crawler_names is None:
                crawler_names = self.crawler_registry.list_crawlers()
            
            task_ids = []
            for crawler_name in crawler_names:
                task_id = f"crawl_{crawler_name}_{int(time.time())}"
                task = CrawlTask(
                    task_id=task_id,
                    crawler_name=crawler_name,
                    priority=TaskPriority.NORMAL
                )
                
                self.task_queue.add_task(task)
                task_ids.append(task_id)
            
            self.logger.info(f"Created {len(task_ids)} crawl tasks")
            return task_ids
            
        except Exception as e:
            self.logger.error(f"Failed to execute crawl task: {e}")
            raise SchedulerError(f"Failed to execute crawl task: {e}")
    
    def execute_report_task(self) -> str:
        """
        Execute report generation task immediately.
        
        Returns:
            Task ID created
        """
        # This is a placeholder - actual report generation would be implemented
        # in a separate service module
        task_id = f"report_{int(time.time())}"
        self.logger.info(f"Report task created: {task_id}")
        return task_id
    
    def get_scheduler_status(self) -> Dict[str, Any]:
        """
        Get current scheduler status.
        
        Returns:
            Dictionary with scheduler status information
        """
        try:
            jobs = []
            for job in self.scheduler.get_jobs():
                jobs.append({
                    'id': job.id,
                    'name': job.name,
                    'next_run_time': job.next_run_time.isoformat() if job.next_run_time else None,
                    'trigger': str(job.trigger)
                })
            
            # Get last successful crawls from state manager
            last_successful_crawls = {}
            for crawler_name, crawler_state in self.state_manager.get_all_crawler_states().items():
                if crawler_state.last_successful_crawl:
                    last_successful_crawls[crawler_name] = crawler_state.last_successful_crawl.isoformat()
            
            # Also include any in-memory timestamps (for backward compatibility)
            for name, timestamp in self._last_successful_crawl.items():
                if name not in last_successful_crawls:
                    last_successful_crawls[name] = timestamp.isoformat()
            
            return {
                'running': self._running,
                'scheduler_running': self.scheduler.running,
                'jobs': jobs,
                'last_successful_crawls': last_successful_crawls,
                'resource_status': self.task_queue.get_resource_status(),
                'active_tasks': len(self._task_futures),
                'completed_tasks_count': len(self.task_queue.get_completed_tasks()),
                'concurrent_executions': self.get_concurrent_execution_status(),
                'state_health': self.state_manager.get_health_summary()
            }
            
        except Exception as e:
            self.logger.error(f"Error getting scheduler status: {e}")
            return {
                'running': self._running,
                'error': str(e)
            }
    
    def get_concurrent_execution_status(self) -> Dict[str, Any]:
        """
        Get status of concurrent crawler executions.
        
        Returns:
            Dictionary with concurrent execution information
        """
        try:
            active_executions = self.concurrent_manager.get_active_executions()
            recent_metrics = self.concurrent_manager.get_all_metrics(limit=50)
            
            # Calculate success rate
            completed_metrics = [m for m in recent_metrics if m.completed_at is not None]
            success_rate = 0.0
            if completed_metrics:
                successful = len([m for m in completed_metrics if m.success])
                success_rate = successful / len(completed_metrics)
            
            # Calculate average execution time
            avg_duration = 0.0
            if completed_metrics:
                durations = [m.duration_seconds for m in completed_metrics if m.duration_seconds]
                if durations:
                    avg_duration = sum(durations) / len(durations)
            
            # Count isolation violations
            total_violations = sum(len(m.isolation_violations) for m in recent_metrics)
            
            return {
                'active_executions': len(active_executions),
                'max_concurrent': self.concurrent_manager.max_concurrent,
                'recent_executions': len(recent_metrics),
                'success_rate': success_rate,
                'average_duration_seconds': avg_duration,
                'total_isolation_violations': total_violations,
                'active_execution_details': [
                    {
                        'execution_id': ctx.execution_id,
                        'crawler_name': ctx.crawler_name,
                        'thread_id': ctx.thread_id,
                        'started_at': ctx.start_time.isoformat(),
                        'duration_seconds': (datetime.now() - ctx.start_time).total_seconds(),
                        'shared_resources': len(ctx.shared_resources_accessed)
                    }
                    for ctx in active_executions
                ]
            }
            
        except Exception as e:
            self.logger.error(f"Error getting concurrent execution status: {e}")
            return {
                'error': str(e),
                'active_executions': 0
            }
    
    def get_execution_metrics(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get detailed execution metrics for monitoring and analysis.
        
        Args:
            limit: Maximum number of metrics to return
            
        Returns:
            List of execution metrics
        """
        try:
            metrics = self.concurrent_manager.get_all_metrics(limit)
            
            return [
                {
                    'execution_id': m.execution_id,
                    'crawler_name': m.crawler_name,
                    'thread_id': m.thread_id,
                    'started_at': m.started_at.isoformat(),
                    'completed_at': m.completed_at.isoformat() if m.completed_at else None,
                    'duration_seconds': m.duration_seconds,
                    'success': m.success,
                    'error_message': m.error_message,
                    'products_extracted': m.products_extracted,
                    'memory_usage_mb': m.memory_usage_mb,
                    'cpu_usage_percent': m.cpu_usage_percent,
                    'isolation_violations': m.isolation_violations
                }
                for m in metrics
            ]
            
        except Exception as e:
            self.logger.error(f"Error getting execution metrics: {e}")
            return []
    
    def cleanup_old_metrics(self, max_age_hours: int = 24) -> int:
        """
        Clean up old execution metrics.
        
        Args:
            max_age_hours: Maximum age of metrics to keep in hours
            
        Returns:
            Number of metrics cleaned up
        """
        try:
            return self.concurrent_manager.cleanup_completed_executions(max_age_hours)
        except Exception as e:
            self.logger.error(f"Error cleaning up metrics: {e}")
            return 0
    
    def get_health_status(self) -> Dict[str, Any]:
        """
        Get health check status for monitoring.
        
        Returns:
            Dictionary with health status
        """
        try:
            status = {
                'healthy': True,
                'timestamp': datetime.now().isoformat(),
                'scheduler_running': self._running and self.scheduler.running,
                'issues': []
            }
            
            # Check if scheduler is running
            if not self._running or not self.scheduler.running:
                status['healthy'] = False
                status['issues'].append('Scheduler is not running')
            
            # Check resource status
            resource_status = self.task_queue.get_resource_status()
            if 'error' in resource_status:
                status['healthy'] = False
                status['issues'].append(f"Resource check error: {resource_status['error']}")
            
            # Check if crawls are happening regularly
            now = datetime.now()
            for crawler_name in self.crawler_registry.list_crawlers():
                last_crawl = self._last_successful_crawl.get(crawler_name)
                if last_crawl:
                    hours_since_last = (now - last_crawl).total_seconds() / 3600
                    if hours_since_last > 25:  # More than 25 hours since last daily crawl
                        status['healthy'] = False
                        status['issues'].append(f"No successful crawl for {crawler_name} in {hours_since_last:.1f} hours")
            
            # Add resource information
            status['resources'] = resource_status
            
            return status
            
        except Exception as e:
            self.logger.error(f"Error getting health status: {e}")
            return {
                'healthy': False,
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }
    
    def _start_task_execution_loop(self) -> None:
        """Start the task execution loop in a separate thread."""
        def execution_loop():
            while self._running:
                try:
                    # Get next task from queue
                    task = self.task_queue.get_next_task()
                    if task is None:
                        time.sleep(1)  # No tasks or resource constraints
                        continue
                    
                    # Submit task for execution
                    future = self.executor.submit(self._execute_crawl_task, task)
                    self._task_futures[task.task_id] = future
                    
                    # Clean up completed futures
                    self._cleanup_completed_futures()
                    
                except Exception as e:
                    self.logger.error(f"Error in task execution loop: {e}")
                    time.sleep(5)  # Wait before retrying
        
        execution_thread = threading.Thread(target=execution_loop, name="task_execution_loop")
        execution_thread.daemon = True
        execution_thread.start()
    
    def _execute_crawl_task(self, task: CrawlTask) -> TaskResult:
        """
        Execute a single crawl task with concurrent isolation monitoring.
        
        Args:
            task: Crawl task to execute
            
        Returns:
            Task execution result
        """
        started_at = datetime.now()
        result = TaskResult(
            task_id=task.task_id,
            status=TaskStatus.RUNNING,
            started_at=started_at
        )
        
        execution_id = None
        
        try:
            self.logger.info(f"Executing crawl task: {task.task_id} (crawler: {task.crawler_name})")
            
            # Start concurrent execution monitoring
            execution_id = self.concurrent_manager.start_execution(task.crawler_name)
            task.execution_id = execution_id
            
            # Create isolated crawler configuration
            isolated_config = self._create_isolated_config(task.config_override or {})
            
            # Get crawler instance with isolated configuration
            crawler = self.crawler_registry.get_crawler(
                task.crawler_name, 
                isolated_config
            )
            
            # Register shared resource access for monitoring
            self.concurrent_manager.register_shared_resource_access(
                execution_id, f"crawler_registry_{task.crawler_name}"
            )
            
            # Execute crawler
            crawl_result = crawler.execute()
            
            # Process results if successful
            if crawl_result.success and crawl_result.products_extracted > 0:
                # This would typically involve data standardization and storage
                # For now, we'll just log the success
                self.logger.info(f"Crawl task completed successfully: {task.task_id} "
                               f"(products: {crawl_result.products_extracted})")
                
                result.status = TaskStatus.COMPLETED
                result.result_data = {
                    'products_found': crawl_result.products_found,
                    'products_extracted': crawl_result.products_extracted,
                    'errors': crawl_result.errors
                }
                
                # Update last successful crawl timestamp
                success_time = datetime.now()
                self._last_successful_crawl[task.crawler_name] = success_time
                
                # Update state manager with success
                self.state_manager.update_crawler_success(
                    task.crawler_name, 
                    crawl_result.products_extracted
                )
                
                # End concurrent execution monitoring (success)
                concurrent_metrics = self.concurrent_manager.end_execution(
                    execution_id, 
                    success=True, 
                    products_extracted=crawl_result.products_extracted
                )
                result.concurrent_metrics = concurrent_metrics
                
            else:
                result.status = TaskStatus.FAILED
                result.error_message = f"Crawl failed: {'; '.join(crawl_result.errors)}"
                
                # Update state manager with failure
                self.state_manager.update_crawler_failure(
                    task.crawler_name,
                    result.error_message
                )
                
                # End concurrent execution monitoring (failure)
                if execution_id:
                    concurrent_metrics = self.concurrent_manager.end_execution(
                        execution_id, 
                        success=False, 
                        error_message=result.error_message
                    )
                    result.concurrent_metrics = concurrent_metrics
                
                # Check if we should retry
                if task.retry_count < task.max_retries:
                    self._schedule_retry(task, result.error_message)
                    result.status = TaskStatus.RETRYING
            
        except Exception as e:
            self.logger.error(f"Error executing crawl task {task.task_id}: {e}")
            result.status = TaskStatus.FAILED
            result.error_message = str(e)
            
            # Update state manager with exception
            self.state_manager.update_crawler_failure(
                task.crawler_name,
                str(e)
            )
            
            # End concurrent execution monitoring (error)
            if execution_id:
                try:
                    concurrent_metrics = self.concurrent_manager.end_execution(
                        execution_id, 
                        success=False, 
                        error_message=str(e)
                    )
                    result.concurrent_metrics = concurrent_metrics
                except Exception as cleanup_error:
                    self.logger.error(f"Error cleaning up concurrent execution: {cleanup_error}")
            
            # Check if we should retry
            if task.retry_count < task.max_retries:
                self._schedule_retry(task, str(e))
                result.status = TaskStatus.RETRYING
        
        finally:
            result.completed_at = datetime.now()
            result.duration_seconds = (result.completed_at - result.started_at).total_seconds()
            
            # Mark task as completed in queue
            self.task_queue.mark_task_completed(task.task_id, result)
            
            # Remove from running futures
            if task.task_id in self._task_futures:
                del self._task_futures[task.task_id]
        
        return result
    
    def _create_isolated_config(self, base_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create an isolated configuration for concurrent crawler execution.
        
        Args:
            base_config: Base configuration to isolate
            
        Returns:
            Isolated configuration with thread-specific settings
        """
        # Deep copy to avoid shared state
        isolated_config = copy.deepcopy(base_config)
        
        # Add thread-specific identifiers
        thread_id = threading.get_ident()
        isolated_config['thread_id'] = thread_id
        isolated_config['isolation_prefix'] = f"thread_{thread_id}"
        
        # Ensure separate temporary directories if needed
        if 'temp_dir' in isolated_config:
            isolated_config['temp_dir'] = f"{isolated_config['temp_dir']}/thread_{thread_id}"
        
        # Ensure separate cache directories if needed
        if 'cache_dir' in isolated_config:
            isolated_config['cache_dir'] = f"{isolated_config['cache_dir']}/thread_{thread_id}"
        
        # Add randomization to avoid conflicts
        import random
        if 'user_agent_rotation' not in isolated_config:
            isolated_config['user_agent_rotation'] = True
        
        # Slightly randomize delays to avoid synchronized requests
        if 'min_delay' in isolated_config:
            base_delay = isolated_config['min_delay']
            isolated_config['min_delay'] = base_delay + random.uniform(0, base_delay * 0.2)
        
        if 'max_delay' in isolated_config:
            base_delay = isolated_config['max_delay']
            isolated_config['max_delay'] = base_delay + random.uniform(0, base_delay * 0.2)
        
        return isolated_config
    
    def _schedule_retry(self, task: CrawlTask, error_message: str) -> None:
        """
        Schedule a task for retry.
        
        Args:
            task: Original task to retry
            error_message: Error message from failed attempt
        """
        retry_task = CrawlTask(
            task_id=f"{task.task_id}_retry_{task.retry_count + 1}",
            crawler_name=task.crawler_name,
            priority=task.priority,
            max_retries=task.max_retries,
            retry_delay=task.retry_delay,
            timeout=task.timeout,
            config_override=task.config_override,
            metadata=task.metadata.copy()
        )
        retry_task.retry_count = task.retry_count + 1
        retry_task.metadata['original_task_id'] = task.task_id
        retry_task.metadata['retry_reason'] = error_message
        
        # Schedule retry with delay
        def schedule_retry():
            time.sleep(task.retry_delay)
            self.task_queue.add_task(retry_task)
        
        retry_thread = threading.Thread(target=schedule_retry, name=f"retry_{retry_task.task_id}")
        retry_thread.daemon = True
        retry_thread.start()
        
        self.logger.info(f"Scheduled retry for task {task.task_id} "
                        f"(attempt {retry_task.retry_count}/{task.max_retries}) "
                        f"in {task.retry_delay} seconds")
    
    def _cleanup_completed_futures(self) -> None:
        """Clean up completed task futures."""
        completed_task_ids = []
        for task_id, future in self._task_futures.items():
            if future.done():
                completed_task_ids.append(task_id)
        
        for task_id in completed_task_ids:
            del self._task_futures[task_id]
    
    def _execute_daily_crawl(self) -> None:
        """Execute daily crawl job."""
        try:
            self.logger.info("Executing scheduled daily crawl")
            task_ids = self.execute_crawl_task()
            self.logger.info(f"Daily crawl scheduled with {len(task_ids)} tasks")
        except Exception as e:
            self.logger.error(f"Error in scheduled daily crawl: {e}")
    
    def _execute_weekly_report(self) -> None:
        """Execute weekly report job."""
        try:
            self.logger.info("Executing scheduled weekly report")
            task_id = self.execute_report_task()
            self.logger.info(f"Weekly report scheduled: {task_id}")
        except Exception as e:
            self.logger.error(f"Error in scheduled weekly report: {e}")
    
    def request_graceful_shutdown(self) -> None:
        """Request graceful shutdown of the scheduler."""
        self.logger.info("Graceful shutdown requested")
        self.state_manager.request_graceful_shutdown()
        
        # Stop accepting new tasks
        self._running = False
        
        # Wait for current tasks to complete
        self.stop(wait=True)
    
    def is_graceful_shutdown_requested(self) -> bool:
        """Check if graceful shutdown has been requested."""
        return self.state_manager.is_graceful_shutdown_requested()
    
    def _job_executed(self, event) -> None:
        """Handle job execution event."""
        self.logger.debug(f"Job executed: {event.job_id}")
    
    def _job_error(self, event) -> None:
        """Handle job error event."""
        self.logger.error(f"Job error: {event.job_id} - {event.exception}")
    
    def _job_missed(self, event) -> None:
        """Handle job missed event."""
        self.logger.warning(f"Job missed: {event.job_id}")
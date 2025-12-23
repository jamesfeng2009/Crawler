"""
Data models for concurrent crawler framework.
"""

import threading
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List
from datetime import datetime
from enum import Enum

from memory_price_monitor.utils.errors import ValidationError


class TaskStatus(Enum):
    """Task execution status."""
    PENDING = "pending"
    ASSIGNED = "assigned"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"


class WorkerState(Enum):
    """Worker thread state."""
    IDLE = "idle"
    WORKING = "working"
    ERROR = "error"
    STOPPED = "stopped"
    STARTING = "starting"


@dataclass
class ConcurrentConfig:
    """Configuration for concurrent crawler system."""
    max_workers: int = 4
    requests_per_second: float = 2.0
    max_concurrent_requests: int = 10
    retry_attempts: int = 3
    retry_delay: float = 1.0
    timeout_seconds: int = 30
    enable_task_stealing: bool = True
    enable_adaptive_rate: bool = True
    queue_timeout: float = 5.0
    worker_health_check_interval: float = 10.0
    max_failures_before_backup: int = 3
    enable_resource_monitoring: bool = True
    browser_reuse_enabled: bool = True
    max_browser_instances: int = 2
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        self.validate()
    
    def validate(self) -> None:
        """
        Validate configuration parameters.
        
        Raises:
            ValidationError: If configuration is invalid
        """
        errors = []
        
        # Worker count validation
        if not (1 <= self.max_workers <= 50):
            errors.append("max_workers must be between 1 and 50")
        
        # Rate limiting validation
        if not (0.1 <= self.requests_per_second <= 10.0):
            errors.append("requests_per_second must be between 0.1 and 10.0")
        
        if not (1 <= self.max_concurrent_requests <= 100):
            errors.append("max_concurrent_requests must be between 1 and 100")
        
        # Timeout validation
        if not (5 <= self.timeout_seconds <= 300):
            errors.append("timeout_seconds must be between 5 and 300")
        
        # Retry validation
        if not (0 <= self.retry_attempts <= 10):
            errors.append("retry_attempts must be between 0 and 10")
        
        if not (0.1 <= self.retry_delay <= 60.0):
            errors.append("retry_delay must be between 0.1 and 60.0")
        
        # Queue timeout validation
        if not (1.0 <= self.queue_timeout <= 60.0):
            errors.append("queue_timeout must be between 1.0 and 60.0")
        
        # Health check interval validation
        if not (1.0 <= self.worker_health_check_interval <= 300.0):
            errors.append("worker_health_check_interval must be between 1.0 and 300.0")
        
        # Browser instance validation
        if not (1 <= self.max_browser_instances <= 10):
            errors.append("max_browser_instances must be between 1 and 10")
        
        if errors:
            raise ValidationError(
                "Concurrent configuration validation failed",
                {"errors": errors}
            )


@dataclass
class CrawlTask:
    """Individual crawl task for a brand or product category."""
    task_id: str
    brand: str
    estimated_complexity: int = 1
    priority: int = 0
    retry_count: int = 0
    assigned_worker: Optional[str] = None
    status: TaskStatus = TaskStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)
    assigned_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def assign_to_worker(self, worker_id: str) -> None:
        """Assign task to a worker."""
        self.assigned_worker = worker_id
        self.assigned_at = datetime.now()
        self.status = TaskStatus.ASSIGNED
    
    def start_execution(self) -> None:
        """Mark task as started."""
        self.started_at = datetime.now()
        self.status = TaskStatus.RUNNING
    
    def complete_successfully(self) -> None:
        """Mark task as completed successfully."""
        self.completed_at = datetime.now()
        self.status = TaskStatus.COMPLETED
    
    def fail_with_error(self, error_message: str) -> None:
        """Mark task as failed with error message."""
        self.completed_at = datetime.now()
        self.status = TaskStatus.FAILED
        self.error_message = error_message
    
    def retry(self) -> None:
        """Prepare task for retry."""
        self.retry_count += 1
        self.status = TaskStatus.RETRYING
        self.assigned_worker = None
        self.assigned_at = None
        self.started_at = None
        self.error_message = None
    
    def get_execution_time(self) -> Optional[float]:
        """Get task execution time in seconds."""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None


@dataclass
class WorkerStatus:
    """Status information for a worker thread."""
    worker_id: str
    state: WorkerState = WorkerState.IDLE
    current_task: Optional[str] = None
    tasks_completed: int = 0
    tasks_failed: int = 0
    last_activity: datetime = field(default_factory=datetime.now)
    created_at: datetime = field(default_factory=datetime.now)
    error_message: Optional[str] = None
    total_execution_time: float = 0.0
    
    def update_activity(self) -> None:
        """Update last activity timestamp."""
        self.last_activity = datetime.now()
    
    def start_task(self, task_id: str) -> None:
        """Mark worker as working on a task."""
        self.state = WorkerState.WORKING
        self.current_task = task_id
        self.update_activity()
    
    def complete_task(self, execution_time: float = 0.0) -> None:
        """Mark task as completed."""
        self.state = WorkerState.IDLE
        self.current_task = None
        self.tasks_completed += 1
        self.total_execution_time += execution_time
        self.update_activity()
    
    def fail_task(self, error_message: str, execution_time: float = 0.0) -> None:
        """Mark task as failed."""
        self.state = WorkerState.IDLE
        self.current_task = None
        self.tasks_failed += 1
        self.total_execution_time += execution_time
        self.error_message = error_message
        self.update_activity()
    
    def set_error_state(self, error_message: str) -> None:
        """Set worker to error state."""
        self.state = WorkerState.ERROR
        self.error_message = error_message
        self.update_activity()
    
    def get_uptime(self) -> float:
        """Get worker uptime in seconds."""
        return (datetime.now() - self.created_at).total_seconds()
    
    def get_average_task_time(self) -> float:
        """Get average task execution time."""
        if self.tasks_completed > 0:
            return self.total_execution_time / self.tasks_completed
        return 0.0


@dataclass
class TaskResult:
    """Result of executing a crawl task."""
    task_id: str
    worker_id: str
    success: bool
    products_found: int = 0
    products_extracted: int = 0
    execution_time: float = 0.0
    error_message: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CrawlResult:
    """Overall result of concurrent crawling operation."""
    total_tasks: int
    completed_tasks: int
    failed_tasks: int
    total_products_found: int
    total_products_saved: int
    execution_time: float
    worker_stats: Dict[str, WorkerStatus]
    started_at: datetime
    completed_at: Optional[datetime] = None
    errors: List[str] = field(default_factory=list)
    performance_metrics: Dict[str, Any] = field(default_factory=dict)
    
    def get_success_rate(self) -> float:
        """Get task success rate as percentage."""
        if self.total_tasks == 0:
            return 0.0
        return (self.completed_tasks / self.total_tasks) * 100.0
    
    def get_throughput(self) -> float:
        """Get products per second throughput."""
        if self.execution_time == 0:
            return 0.0
        return self.total_products_found / self.execution_time
    
    def get_efficiency(self) -> float:
        """Get extraction efficiency as percentage."""
        if self.total_products_found == 0:
            return 0.0
        return (self.total_products_saved / self.total_products_found) * 100.0


class ConcurrentResultCollector:
    """Thread-safe collector for task results."""
    
    def __init__(self):
        """Initialize result collector."""
        self._results: List[TaskResult] = []
        self._lock = threading.Lock()
        self._total_products_found = 0
        self._total_products_saved = 0
        self._total_execution_time = 0.0
    
    def add_result(self, result: TaskResult) -> None:
        """
        Add a task result to the collection.
        
        Args:
            result: Task result to add
        """
        with self._lock:
            self._results.append(result)
            self._total_products_found += result.products_found
            self._total_products_saved += result.products_extracted
            self._total_execution_time += result.execution_time
    
    def get_all_results(self) -> List[TaskResult]:
        """
        Get all collected results.
        
        Returns:
            List of all task results
        """
        with self._lock:
            return self._results.copy()
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Get summary statistics of all results.
        
        Returns:
            Dictionary with summary statistics
        """
        with self._lock:
            successful_results = [r for r in self._results if r.success]
            failed_results = [r for r in self._results if not r.success]
            
            return {
                "total_tasks": len(self._results),
                "successful_tasks": len(successful_results),
                "failed_tasks": len(failed_results),
                "success_rate": (len(successful_results) / len(self._results) * 100.0) if self._results else 0.0,
                "total_products_found": self._total_products_found,
                "total_products_saved": self._total_products_saved,
                "extraction_efficiency": (self._total_products_saved / self._total_products_found * 100.0) if self._total_products_found > 0 else 0.0,
                "total_execution_time": self._total_execution_time,
                "average_task_time": (self._total_execution_time / len(self._results)) if self._results else 0.0,
                "throughput_products_per_second": (self._total_products_found / self._total_execution_time) if self._total_execution_time > 0 else 0.0
            }
    
    def get_worker_stats(self) -> Dict[str, Dict[str, Any]]:
        """
        Get statistics grouped by worker.
        
        Returns:
            Dictionary mapping worker IDs to their statistics
        """
        with self._lock:
            worker_stats = {}
            
            for result in self._results:
                worker_id = result.worker_id
                if worker_id not in worker_stats:
                    worker_stats[worker_id] = {
                        "tasks_completed": 0,
                        "tasks_failed": 0,
                        "products_found": 0,
                        "products_saved": 0,
                        "total_execution_time": 0.0,
                        "average_task_time": 0.0
                    }
                
                stats = worker_stats[worker_id]
                if result.success:
                    stats["tasks_completed"] += 1
                else:
                    stats["tasks_failed"] += 1
                
                stats["products_found"] += result.products_found
                stats["products_saved"] += result.products_extracted
                stats["total_execution_time"] += result.execution_time
                
                # Update average task time
                total_tasks = stats["tasks_completed"] + stats["tasks_failed"]
                if total_tasks > 0:
                    stats["average_task_time"] = stats["total_execution_time"] / total_tasks
            
            return worker_stats
    
    def clear(self) -> None:
        """Clear all collected results."""
        with self._lock:
            self._results.clear()
            self._total_products_found = 0
            self._total_products_saved = 0
            self._total_execution_time = 0.0
    
    def get_results_count(self) -> int:
        """Get number of collected results."""
        with self._lock:
            return len(self._results)
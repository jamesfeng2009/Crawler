"""
Task scheduler for concurrent crawler system.
Handles task queue management, assignment, and load balancing.
"""

import threading
import time
from typing import List, Optional, Dict, Any, Set
from datetime import datetime, timedelta
from collections import defaultdict, deque

from memory_price_monitor.utils.logging import get_logger
from memory_price_monitor.utils.errors import CrawlerError
from .models import CrawlTask, TaskStatus, WorkerStatus, WorkerState
from .thread_safe import ThreadSafeQueue, ThreadSafeCounter, ThreadSafeSet


logger = get_logger(__name__)


class TaskScheduler:
    """
    Task scheduler for managing crawl task distribution and execution.
    
    Provides task queue management, intelligent task assignment, and basic load balancing
    for the concurrent crawler system.
    """
    
    def __init__(self, max_queue_size: int = 1000, enable_task_stealing: bool = True):
        """
        Initialize task scheduler.
        
        Args:
            max_queue_size: Maximum number of tasks in queue
            enable_task_stealing: Whether to enable task stealing for load balancing
        """
        self.max_queue_size = max_queue_size
        self.enable_task_stealing = enable_task_stealing
        self.logger = get_logger(__name__)
        
        # Core task queue
        self._task_queue = ThreadSafeQueue(maxsize=max_queue_size)
        
        # Task tracking
        self._all_tasks: Dict[str, CrawlTask] = {}
        self._pending_tasks = ThreadSafeSet()
        self._assigned_tasks = ThreadSafeSet()
        self._completed_tasks = ThreadSafeSet()
        self._failed_tasks = ThreadSafeSet()
        
        # Worker assignment tracking
        self._worker_tasks: Dict[str, Optional[str]] = {}  # worker_id -> task_id
        self._task_assignments: Dict[str, str] = {}  # task_id -> worker_id
        
        # Task stealing support
        self._worker_queues: Dict[str, deque] = defaultdict(deque)  # Per-worker task queues
        
        # Statistics and metrics
        self._tasks_submitted = ThreadSafeCounter()
        self._tasks_assigned = ThreadSafeCounter()
        self._tasks_completed = ThreadSafeCounter()
        self._tasks_failed = ThreadSafeCounter()
        
        # Historical data for complexity estimation
        self._brand_complexity_history: Dict[str, List[float]] = defaultdict(list)
        self._brand_avg_complexity: Dict[str, float] = {}
        
        # Thread safety
        self._lock = threading.RLock()
        
        # Scheduler state
        self._shutdown = False
        self._created_at = datetime.now()
        
        self.logger.info(f"TaskScheduler initialized with max_queue_size={max_queue_size}, task_stealing={enable_task_stealing}")
    
    def add_tasks(self, tasks: List[CrawlTask]) -> None:
        """
        Add multiple tasks to the scheduler.
        
        Args:
            tasks: List of crawl tasks to add
            
        Raises:
            CrawlerError: If scheduler is shutdown or queue is full
        """
        if self._shutdown:
            raise CrawlerError("Cannot add tasks: scheduler is shutdown")
        
        if not tasks:
            self.logger.warning("No tasks provided to add_tasks")
            return
        
        with self._lock:
            # Validate and prepare tasks
            valid_tasks = []
            for task in tasks:
                if task.task_id in self._all_tasks:
                    self.logger.warning(f"Task {task.task_id} already exists, skipping")
                    continue
                
                # Estimate complexity based on historical data
                task.estimated_complexity = self._estimate_task_complexity(task)
                
                # Store task
                self._all_tasks[task.task_id] = task
                self._pending_tasks.add(task.task_id)
                valid_tasks.append(task)
            
            # Sort tasks by priority and complexity for better distribution
            valid_tasks.sort(key=lambda t: (-t.priority, t.estimated_complexity))
            
            # Add tasks to queue
            added_count = 0
            for task in valid_tasks:
                try:
                    self._task_queue.put_nowait(task)
                    self._tasks_submitted.increment()
                    added_count += 1
                    self.logger.debug(f"Added task {task.task_id} for brand {task.brand}")
                except Exception as e:
                    self.logger.error(f"Failed to add task {task.task_id}: {str(e)}")
                    # Remove from tracking if failed to add to queue
                    self._all_tasks.pop(task.task_id, None)
                    self._pending_tasks.remove(task.task_id)
            
            self.logger.info(f"Added {added_count}/{len(tasks)} tasks to scheduler")
    
    def get_next_task(self, worker_id: str, timeout: Optional[float] = None) -> Optional[CrawlTask]:
        """
        Get next available task for a worker.
        
        Args:
            worker_id: Unique identifier for the requesting worker
            timeout: Optional timeout for waiting for tasks
            
        Returns:
            Next available task or None if no tasks available
        """
        if self._shutdown:
            return None
        
        # Initialize worker if not seen before
        if worker_id not in self._worker_tasks:
            with self._lock:
                self._worker_tasks[worker_id] = None
        
        # Try to get task from main queue first
        task = self._get_task_from_main_queue(worker_id, timeout)
        
        # If no task from main queue and task stealing is enabled, try stealing
        if task is None and self.enable_task_stealing:
            task = self._try_steal_task(worker_id)
        
        if task:
            self._assign_task_to_worker(task, worker_id)
            self.logger.debug(f"Assigned task {task.task_id} to worker {worker_id}")
        
        return task
    
    def mark_task_completed(self, task_id: str, result: Optional[Dict[str, Any]] = None) -> None:
        """
        Mark a task as successfully completed.
        
        Args:
            task_id: ID of the completed task
            result: Optional result data from task execution
        """
        with self._lock:
            task = self._all_tasks.get(task_id)
            if not task:
                self.logger.warning(f"Cannot mark unknown task {task_id} as completed")
                return
            
            # Update task status
            task.complete_successfully()
            
            # Update tracking sets
            self._assigned_tasks.discard(task_id)
            self._completed_tasks.add(task_id)
            
            # Clear worker assignment
            worker_id = self._task_assignments.pop(task_id, None)
            if worker_id:
                self._worker_tasks[worker_id] = None
            
            # Update statistics
            self._tasks_completed.increment()
            
            # Update complexity history for future estimation
            if task.get_execution_time():
                self._update_complexity_history(task.brand, task.get_execution_time())
            
            self.logger.debug(f"Task {task_id} marked as completed by worker {worker_id}")
    
    def mark_task_failed(self, task_id: str, error: Exception, retry: bool = True) -> None:
        """
        Mark a task as failed.
        
        Args:
            task_id: ID of the failed task
            error: Exception that caused the failure
            retry: Whether to retry the task if retry attempts remain
        """
        with self._lock:
            task = self._all_tasks.get(task_id)
            if not task:
                self.logger.warning(f"Cannot mark unknown task {task_id} as failed")
                return
            
            # Clear worker assignment
            worker_id = self._task_assignments.pop(task_id, None)
            if worker_id:
                self._worker_tasks[worker_id] = None
            
            # Check if we should retry
            max_retries = 3  # Could be made configurable
            if retry and task.retry_count < max_retries:
                # Prepare for retry
                task.retry()
                
                # Move back to pending
                self._assigned_tasks.discard(task_id)
                self._pending_tasks.add(task_id)
                
                # Re-add to queue with delay
                try:
                    self._task_queue.put_nowait(task)
                    self.logger.info(f"Task {task_id} queued for retry (attempt {task.retry_count}/{max_retries})")
                except Exception as e:
                    self.logger.error(f"Failed to re-queue task {task_id} for retry: {str(e)}")
                    # If can't re-queue, mark as permanently failed
                    self._mark_task_permanently_failed(task, error)
            else:
                # Mark as permanently failed
                self._mark_task_permanently_failed(task, error)
    
    def get_queue_status(self) -> Dict[str, Any]:
        """
        Get current queue status and statistics.
        
        Returns:
            Dictionary with queue status information
        """
        with self._lock:
            queue_stats = self._task_queue.get_stats()
            
            return {
                "queue_size": queue_stats["size"],
                "queue_empty": queue_stats["empty"],
                "queue_full": queue_stats["full"],
                "tasks_submitted": self._tasks_submitted.get_value(),
                "tasks_assigned": self._tasks_assigned.get_value(),
                "tasks_completed": self._tasks_completed.get_value(),
                "tasks_failed": self._tasks_failed.get_value(),
                "pending_tasks": self._pending_tasks.size(),
                "assigned_tasks": self._assigned_tasks.size(),
                "completed_tasks": self._completed_tasks.size(),
                "failed_tasks": self._failed_tasks.size(),
                "active_workers": len([w for w in self._worker_tasks.values() if w is not None]),
                "total_workers": len(self._worker_tasks),
                "task_stealing_enabled": self.enable_task_stealing,
                "uptime_seconds": (datetime.now() - self._created_at).total_seconds()
            }
    
    def get_worker_assignments(self) -> Dict[str, Optional[str]]:
        """
        Get current worker task assignments.
        
        Returns:
            Dictionary mapping worker IDs to their assigned task IDs
        """
        with self._lock:
            return self._worker_tasks.copy()
    
    def get_task_details(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a specific task.
        
        Args:
            task_id: ID of the task to get details for
            
        Returns:
            Dictionary with task details or None if task not found
        """
        with self._lock:
            task = self._all_tasks.get(task_id)
            if not task:
                return None
            
            return {
                "task_id": task.task_id,
                "brand": task.brand,
                "status": task.status.value,
                "priority": task.priority,
                "estimated_complexity": task.estimated_complexity,
                "retry_count": task.retry_count,
                "assigned_worker": task.assigned_worker,
                "created_at": task.created_at.isoformat(),
                "assigned_at": task.assigned_at.isoformat() if task.assigned_at else None,
                "started_at": task.started_at.isoformat() if task.started_at else None,
                "completed_at": task.completed_at.isoformat() if task.completed_at else None,
                "execution_time": task.get_execution_time(),
                "error_message": task.error_message,
                "metadata": task.metadata
            }
    
    def shutdown(self) -> Dict[str, Any]:
        """
        Shutdown the scheduler gracefully.
        
        Returns:
            Dictionary with shutdown statistics
        """
        self.logger.info("Shutting down task scheduler...")
        
        with self._lock:
            self._shutdown = True
            
            # Get final statistics
            stats = self.get_queue_status()
            
            # Clear remaining tasks from queue
            remaining_tasks = self._task_queue.clear()
            
            self.logger.info(f"Task scheduler shutdown complete. Cleared {remaining_tasks} remaining tasks.")
            
            return {
                "shutdown_time": datetime.now().isoformat(),
                "remaining_tasks_cleared": remaining_tasks,
                "final_statistics": stats
            }
    
    def _get_task_from_main_queue(self, worker_id: str, timeout: Optional[float]) -> Optional[CrawlTask]:
        """
        Get task from main task queue.
        
        Args:
            worker_id: ID of requesting worker
            timeout: Timeout for queue get operation
            
        Returns:
            Task from queue or None if no task available
        """
        try:
            if timeout is None:
                timeout = 1.0  # Default timeout
            
            task = self._task_queue.get(timeout=timeout)
            return task
        except Exception:
            # Queue empty or timeout - this is normal
            return None
    
    def _try_steal_task(self, worker_id: str) -> Optional[CrawlTask]:
        """
        Try to steal a task from another worker's queue.
        
        Args:
            worker_id: ID of worker trying to steal
            
        Returns:
            Stolen task or None if no task available to steal
        """
        # Task stealing implementation - simplified for now
        # In a more advanced implementation, this would look at worker-specific queues
        # For now, we'll just return None as task stealing requires more complex infrastructure
        return None
    
    def _assign_task_to_worker(self, task: CrawlTask, worker_id: str) -> None:
        """
        Assign a task to a specific worker.
        
        Args:
            task: Task to assign
            worker_id: ID of worker to assign to
        """
        with self._lock:
            # Update task
            task.assign_to_worker(worker_id)
            
            # Update tracking
            self._pending_tasks.discard(task.task_id)
            self._assigned_tasks.add(task.task_id)
            
            # Update worker assignment
            self._worker_tasks[worker_id] = task.task_id
            self._task_assignments[task.task_id] = worker_id
            
            # Update statistics
            self._tasks_assigned.increment()
    
    def _mark_task_permanently_failed(self, task: CrawlTask, error: Exception) -> None:
        """
        Mark a task as permanently failed.
        
        Args:
            task: Task to mark as failed
            error: Exception that caused the failure
        """
        task.fail_with_error(str(error))
        
        # Update tracking
        self._assigned_tasks.discard(task.task_id)
        self._failed_tasks.add(task.task_id)
        
        # Update statistics
        self._tasks_failed.increment()
        
        self.logger.warning(f"Task {task.task_id} permanently failed after {task.retry_count} retries: {str(error)}")
    
    def _estimate_task_complexity(self, task: CrawlTask) -> int:
        """
        Estimate task complexity based on historical data.
        
        Args:
            task: Task to estimate complexity for
            
        Returns:
            Estimated complexity score
        """
        # Use historical average if available
        if task.brand in self._brand_avg_complexity:
            return max(1, int(self._brand_avg_complexity[task.brand]))
        
        # Default complexity estimation based on brand name characteristics
        # This is a simple heuristic - could be improved with ML or more data
        base_complexity = 1
        
        # Longer brand names might indicate more complex products/categories
        if len(task.brand) > 10:
            base_complexity += 1
        
        # Some brands might be known to be more complex
        complex_brands = {'samsung', 'corsair', 'gskill', 'kingston'}
        if task.brand.lower() in complex_brands:
            base_complexity += 2
        
        return base_complexity
    
    def _update_complexity_history(self, brand: str, execution_time: float) -> None:
        """
        Update complexity history for a brand based on execution time.
        
        Args:
            brand: Brand name
            execution_time: Actual execution time in seconds
        """
        # Convert execution time to complexity score (simple linear mapping)
        complexity_score = max(1, int(execution_time * 2))  # 2 complexity points per second
        
        # Add to history (keep last 10 entries)
        history = self._brand_complexity_history[brand]
        history.append(complexity_score)
        if len(history) > 10:
            history.pop(0)
        
        # Update average
        self._brand_avg_complexity[brand] = sum(history) / len(history)
        
        self.logger.debug(f"Updated complexity for brand {brand}: avg={self._brand_avg_complexity[brand]:.1f}")
    
    def get_brand_complexity_stats(self) -> Dict[str, Dict[str, Any]]:
        """
        Get complexity statistics for all brands.
        
        Returns:
            Dictionary mapping brand names to their complexity statistics
        """
        with self._lock:
            stats = {}
            for brand, avg_complexity in self._brand_avg_complexity.items():
                history = self._brand_complexity_history[brand]
                stats[brand] = {
                    "average_complexity": avg_complexity,
                    "sample_count": len(history),
                    "min_complexity": min(history) if history else 0,
                    "max_complexity": max(history) if history else 0,
                    "recent_complexity": history[-1] if history else 0
                }
            return stats
    
    def is_empty(self) -> bool:
        """
        Check if scheduler has no pending or assigned tasks.
        
        Returns:
            True if no tasks are pending or assigned
        """
        with self._lock:
            return (self._pending_tasks.size() == 0 and 
                   self._assigned_tasks.size() == 0 and 
                   self._task_queue.empty())
    
    def get_pending_task_count(self) -> int:
        """
        Get number of pending tasks.
        
        Returns:
            Number of tasks waiting to be assigned
        """
        return self._pending_tasks.size()
    
    def get_assigned_task_count(self) -> int:
        """
        Get number of assigned tasks.
        
        Returns:
            Number of tasks currently assigned to workers
        """
        return self._assigned_tasks.size()
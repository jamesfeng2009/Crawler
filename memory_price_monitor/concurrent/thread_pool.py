"""
Thread pool manager for concurrent crawler system.
"""

import threading
import time
import traceback
from typing import List, Dict, Optional, Callable, Any
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, Future
from queue import Empty

from memory_price_monitor.utils.logging import get_logger
from memory_price_monitor.utils.errors import CrawlerError
from .models import (
    ConcurrentConfig,
    WorkerStatus,
    WorkerState,
    CrawlTask,
    TaskResult
)
from .thread_safe import ThreadSafeQueue, ThreadSafeCounter


logger = get_logger(__name__)


class WorkerThread(threading.Thread):
    """Individual worker thread for processing crawl tasks."""
    
    def __init__(
        self,
        worker_id: str,
        task_queue: ThreadSafeQueue,
        result_callback: Callable[[TaskResult], None],
        shutdown_event: threading.Event,
        config: ConcurrentConfig,
        task_processor: Callable[[CrawlTask, str], TaskResult]
    ):
        """
        Initialize worker thread.
        
        Args:
            worker_id: Unique identifier for this worker
            task_queue: Queue to get tasks from
            result_callback: Callback to report task results
            shutdown_event: Event to signal shutdown
            config: Concurrent crawler configuration
            task_processor: Function to process individual tasks
        """
        super().__init__(name=f"CrawlerWorker-{worker_id}", daemon=True)
        
        self.worker_id = worker_id
        self.task_queue = task_queue
        self.result_callback = result_callback
        self.shutdown_event = shutdown_event
        self.config = config
        self.task_processor = task_processor
        
        # Worker state
        self.status = WorkerStatus(worker_id=worker_id, state=WorkerState.STARTING)
        self.logger = get_logger(f"{__name__}.{worker_id}")
        
        # Exception handling
        self._exception_count = ThreadSafeCounter()
        self._last_exception: Optional[Exception] = None
        self._restart_requested = threading.Event()
        
        # Performance tracking
        self._start_time = datetime.now()
        self._last_heartbeat = datetime.now()
        
        self.logger.debug(f"Worker {worker_id} initialized")
    
    def run(self) -> None:
        """Main worker thread execution loop."""
        self.logger.info(f"Worker {self.worker_id} starting")
        self.status.state = WorkerState.IDLE
        self.status.update_activity()
        
        try:
            while not self.shutdown_event.is_set():
                try:
                    # Update heartbeat
                    self._last_heartbeat = datetime.now()
                    
                    # Try to get a task from the queue
                    task = self._get_next_task()
                    if task is None:
                        # No task available, check if we should continue waiting
                        if self._should_continue_waiting():
                            continue
                        else:
                            break
                    
                    # Process the task
                    self._process_task(task)
                    
                except Exception as e:
                    self._handle_worker_exception(e)
                    
                    # Check if we've had too many exceptions
                    if self._exception_count.get_value() > self.config.max_failures_before_backup:
                        self.logger.error(f"Worker {self.worker_id} exceeded maximum failures, stopping")
                        self.status.set_error_state(f"Too many failures: {str(e)}")
                        break
        
        except Exception as e:
            self.logger.error(f"Fatal error in worker {self.worker_id}: {str(e)}")
            self.status.set_error_state(f"Fatal error: {str(e)}")
        
        finally:
            self.status.state = WorkerState.STOPPED
            self.status.update_activity()
            self.logger.info(f"Worker {self.worker_id} stopped")
    
    def _get_next_task(self) -> Optional[CrawlTask]:
        """
        Get next task from queue with timeout.
        
        Returns:
            Next task or None if timeout/shutdown
        """
        try:
            task = self.task_queue.get(timeout=self.config.queue_timeout)
            self.logger.debug(f"Worker {self.worker_id} got task {task.task_id}")
            return task
        except Empty:
            # Queue timeout - this is normal
            return None
        except Exception as e:
            self.logger.warning(f"Worker {self.worker_id} error getting task: {str(e)}")
            return None
    
    def _process_task(self, task: CrawlTask) -> None:
        """
        Process a single crawl task.
        
        Args:
            task: Task to process
        """
        start_time = datetime.now()
        
        try:
            # Update status
            self.status.start_task(task.task_id)
            task.assign_to_worker(self.worker_id)
            task.start_execution()
            
            self.logger.debug(f"Worker {self.worker_id} processing task {task.task_id} for brand {task.brand}")
            
            # Process the task using the provided processor function
            result = self.task_processor(task, self.worker_id)
            
            # Calculate execution time
            execution_time = (datetime.now() - start_time).total_seconds()
            result.execution_time = execution_time
            
            # Update task status
            if result.success:
                task.complete_successfully()
                self.status.complete_task(execution_time)
                self.logger.debug(f"Worker {self.worker_id} completed task {task.task_id}")
            else:
                task.fail_with_error(result.error_message or "Unknown error")
                self.status.fail_task(result.error_message or "Unknown error", execution_time)
                self.logger.warning(f"Worker {self.worker_id} failed task {task.task_id}: {result.error_message}")
            
            # Report result
            self.result_callback(result)
            
        except Exception as e:
            # Handle task processing exception
            execution_time = (datetime.now() - start_time).total_seconds()
            error_message = f"Task processing error: {str(e)}"
            
            task.fail_with_error(error_message)
            self.status.fail_task(error_message, execution_time)
            
            # Create error result
            error_result = TaskResult(
                task_id=task.task_id,
                worker_id=self.worker_id,
                success=False,
                error_message=error_message,
                execution_time=execution_time
            )
            
            self.result_callback(error_result)
            self.logger.error(f"Worker {self.worker_id} task {task.task_id} failed: {str(e)}")
            
            # Increment exception count
            self._exception_count.increment()
    
    def _should_continue_waiting(self) -> bool:
        """
        Check if worker should continue waiting for tasks.
        
        Returns:
            True if should continue waiting
        """
        # Continue waiting if not shutdown and not too many consecutive empty gets
        return not self.shutdown_event.is_set()
    
    def _handle_worker_exception(self, exception: Exception) -> None:
        """
        Handle worker thread exception.
        
        Args:
            exception: Exception that occurred
        """
        self._last_exception = exception
        self._exception_count.increment()
        
        error_msg = f"Worker exception: {str(exception)}"
        self.logger.error(f"Worker {self.worker_id} exception: {error_msg}")
        self.logger.debug(f"Worker {self.worker_id} exception traceback: {traceback.format_exc()}")
        
        # Set error state temporarily
        self.status.set_error_state(error_msg)
        
        # Wait a bit before continuing
        time.sleep(self.config.retry_delay)
        
        # Reset to idle state if not too many failures
        if self._exception_count.get_value() <= self.config.max_failures_before_backup:
            self.status.state = WorkerState.IDLE
            self.status.update_activity()
    
    def request_restart(self) -> None:
        """Request worker restart."""
        self.logger.info(f"Restart requested for worker {self.worker_id}")
        self._restart_requested.set()
    
    def is_healthy(self) -> bool:
        """
        Check if worker is healthy.
        
        Returns:
            True if worker is healthy
        """
        # Check if thread is alive
        if not self.is_alive():
            return False
        
        # Check if too many exceptions
        if self._exception_count.get_value() > self.config.max_failures_before_backup:
            return False
        
        # Check if worker is in error state
        if self.status.state == WorkerState.ERROR:
            return False
        
        # Check heartbeat (if enabled)
        if self.config.enable_resource_monitoring:
            heartbeat_age = (datetime.now() - self._last_heartbeat).total_seconds()
            if heartbeat_age > self.config.worker_health_check_interval * 2:
                return False
        
        return True
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get worker statistics.
        
        Returns:
            Dictionary with worker stats
        """
        uptime = (datetime.now() - self._start_time).total_seconds()
        
        return {
            "worker_id": self.worker_id,
            "state": self.status.state.value,
            "current_task": self.status.current_task,
            "tasks_completed": self.status.tasks_completed,
            "tasks_failed": self.status.tasks_failed,
            "exception_count": self._exception_count.get_value(),
            "last_exception": str(self._last_exception) if self._last_exception else None,
            "uptime_seconds": uptime,
            "average_task_time": self.status.get_average_task_time(),
            "last_activity": self.status.last_activity.isoformat(),
            "is_healthy": self.is_healthy(),
            "is_alive": self.is_alive()
        }
    
    def shutdown(self, timeout: float = 5.0) -> bool:
        """
        Shutdown worker gracefully.
        
        Args:
            timeout: Maximum time to wait for shutdown
            
        Returns:
            True if shutdown completed within timeout
        """
        self.logger.info(f"Shutting down worker {self.worker_id}")
        
        # Signal shutdown
        self.shutdown_event.set()
        
        # Wait for thread to finish
        self.join(timeout=timeout)
        
        if self.is_alive():
            self.logger.warning(f"Worker {self.worker_id} did not shutdown within timeout")
            return False
        
        self.logger.info(f"Worker {self.worker_id} shutdown completed")
        return True


class ThreadPoolManager:
    """Manager for worker thread pool."""
    
    def __init__(self, config: ConcurrentConfig):
        """
        Initialize thread pool manager.
        
        Args:
            config: Concurrent crawler configuration
        """
        self.config = config
        self.logger = get_logger(__name__)
        
        # Worker management
        self._workers: Dict[str, WorkerThread] = {}
        self._shutdown_event = threading.Event()
        self._task_queue: Optional[ThreadSafeQueue] = None
        self._result_callback: Optional[Callable[[TaskResult], None]] = None
        self._task_processor: Optional[Callable[[CrawlTask, str], TaskResult]] = None
        
        # Health monitoring
        self._health_check_thread: Optional[threading.Thread] = None
        self._health_check_running = False
        
        # Statistics
        self._workers_created = ThreadSafeCounter()
        self._workers_restarted = ThreadSafeCounter()
        self._total_exceptions = ThreadSafeCounter()
        
        # Thread safety
        self._lock = threading.RLock()
        
        self.logger.debug("ThreadPoolManager initialized")
    
    def initialize(
        self,
        task_queue: ThreadSafeQueue,
        result_callback: Callable[[TaskResult], None],
        task_processor: Callable[[CrawlTask, str], TaskResult]
    ) -> None:
        """
        Initialize thread pool with required components.
        
        Args:
            task_queue: Queue for tasks
            result_callback: Callback for task results
            task_processor: Function to process tasks
        """
        self._task_queue = task_queue
        self._result_callback = result_callback
        self._task_processor = task_processor
        
        self.logger.debug("ThreadPoolManager components initialized")
    
    def create_workers(self, count: Optional[int] = None) -> List[WorkerThread]:
        """
        Create worker threads.
        
        Args:
            count: Number of workers to create (defaults to config.max_workers)
            
        Returns:
            List of created worker threads
            
        Raises:
            CrawlerError: If components not initialized or creation fails
        """
        if self._task_queue is None or self._result_callback is None or self._task_processor is None:
            raise CrawlerError("ThreadPoolManager not properly initialized")
        
        worker_count = count or self.config.max_workers
        
        with self._lock:
            self.logger.info(f"Creating {worker_count} worker threads")
            
            created_workers = []
            
            for i in range(worker_count):
                worker_id = f"worker_{i}"
                
                try:
                    worker = WorkerThread(
                        worker_id=worker_id,
                        task_queue=self._task_queue,
                        result_callback=self._result_callback,
                        shutdown_event=self._shutdown_event,
                        config=self.config,
                        task_processor=self._task_processor
                    )
                    
                    self._workers[worker_id] = worker
                    created_workers.append(worker)
                    self._workers_created.increment()
                    
                    self.logger.debug(f"Created worker {worker_id}")
                    
                except Exception as e:
                    self.logger.error(f"Failed to create worker {worker_id}: {str(e)}")
                    raise CrawlerError(f"Failed to create worker {worker_id}: {str(e)}")
            
            # Start health monitoring if enabled
            if self.config.enable_resource_monitoring:
                self._start_health_monitoring()
            
            self.logger.info(f"Successfully created {len(created_workers)} workers")
            return created_workers
    
    def start_workers(self) -> None:
        """Start all created worker threads."""
        with self._lock:
            if not self._workers:
                raise CrawlerError("No workers created")
            
            self.logger.info(f"Starting {len(self._workers)} worker threads")
            
            for worker_id, worker in self._workers.items():
                try:
                    worker.start()
                    self.logger.debug(f"Started worker {worker_id}")
                except Exception as e:
                    self.logger.error(f"Failed to start worker {worker_id}: {str(e)}")
                    worker.status.set_error_state(f"Failed to start: {str(e)}")
            
            self.logger.info("All workers started")
    
    def shutdown_workers(self, timeout: int = 10) -> None:
        """
        Shutdown all worker threads gracefully.
        
        Args:
            timeout: Maximum time to wait for shutdown in seconds
        """
        with self._lock:
            self.logger.info(f"Shutting down {len(self._workers)} workers")
            
            # Signal shutdown
            self._shutdown_event.set()
            
            # Stop health monitoring
            self._stop_health_monitoring()
            
            # Shutdown each worker
            shutdown_results = {}
            for worker_id, worker in self._workers.items():
                try:
                    success = worker.shutdown(timeout=timeout)
                    shutdown_results[worker_id] = success
                    
                    if success:
                        self.logger.debug(f"Worker {worker_id} shutdown successfully")
                    else:
                        self.logger.warning(f"Worker {worker_id} shutdown timed out")
                        
                except Exception as e:
                    self.logger.error(f"Error shutting down worker {worker_id}: {str(e)}")
                    shutdown_results[worker_id] = False
            
            # Report shutdown results
            successful_shutdowns = sum(1 for success in shutdown_results.values() if success)
            self.logger.info(f"Shutdown completed: {successful_shutdowns}/{len(self._workers)} workers shutdown successfully")
            
            # Clear workers
            self._workers.clear()
    
    def get_worker_status(self) -> Dict[str, WorkerStatus]:
        """
        Get status of all workers.
        
        Returns:
            Dictionary mapping worker IDs to their status
        """
        with self._lock:
            return {worker_id: worker.status for worker_id, worker in self._workers.items()}
    
    def restart_failed_worker(self, worker_id: str) -> bool:
        """
        Restart a failed worker.
        
        Args:
            worker_id: ID of worker to restart
            
        Returns:
            True if restart was successful
        """
        with self._lock:
            if worker_id not in self._workers:
                self.logger.warning(f"Cannot restart worker {worker_id}: not found")
                return False
            
            old_worker = self._workers[worker_id]
            
            try:
                self.logger.info(f"Restarting failed worker {worker_id}")
                
                # Shutdown old worker
                old_worker.shutdown(timeout=5.0)
                
                # Create new worker
                new_worker = WorkerThread(
                    worker_id=worker_id,
                    task_queue=self._task_queue,
                    result_callback=self._result_callback,
                    shutdown_event=self._shutdown_event,
                    config=self.config,
                    task_processor=self._task_processor
                )
                
                # Replace old worker
                self._workers[worker_id] = new_worker
                
                # Start new worker
                new_worker.start()
                
                self._workers_restarted.increment()
                self.logger.info(f"Worker {worker_id} restarted successfully")
                
                return True
                
            except Exception as e:
                self.logger.error(f"Failed to restart worker {worker_id}: {str(e)}")
                return False
    
    def get_healthy_workers(self) -> List[str]:
        """
        Get list of healthy worker IDs.
        
        Returns:
            List of healthy worker IDs
        """
        with self._lock:
            return [worker_id for worker_id, worker in self._workers.items() if worker.is_healthy()]
    
    def get_failed_workers(self) -> List[str]:
        """
        Get list of failed worker IDs.
        
        Returns:
            List of failed worker IDs
        """
        with self._lock:
            return [worker_id for worker_id, worker in self._workers.items() if not worker.is_healthy()]
    
    def get_pool_stats(self) -> Dict[str, Any]:
        """
        Get thread pool statistics.
        
        Returns:
            Dictionary with pool statistics
        """
        with self._lock:
            healthy_workers = self.get_healthy_workers()
            failed_workers = self.get_failed_workers()
            
            worker_states = {}
            for state in WorkerState:
                worker_states[state.value] = len([
                    w for w in self._workers.values() 
                    if w.status.state == state
                ])
            
            total_tasks_completed = sum(w.status.tasks_completed for w in self._workers.values())
            total_tasks_failed = sum(w.status.tasks_failed for w in self._workers.values())
            
            return {
                "total_workers": len(self._workers),
                "healthy_workers": len(healthy_workers),
                "failed_workers": len(failed_workers),
                "worker_states": worker_states,
                "workers_created": self._workers_created.get_value(),
                "workers_restarted": self._workers_restarted.get_value(),
                "total_exceptions": self._total_exceptions.get_value(),
                "total_tasks_completed": total_tasks_completed,
                "total_tasks_failed": total_tasks_failed,
                "health_monitoring_active": self._health_check_running
            }
    
    def _start_health_monitoring(self) -> None:
        """Start health monitoring thread."""
        if self._health_check_running:
            return
        
        self._health_check_running = True
        self._health_check_thread = threading.Thread(
            target=self._health_check_loop,
            name="HealthMonitor",
            daemon=True
        )
        self._health_check_thread.start()
        
        self.logger.debug("Health monitoring started")
    
    def _stop_health_monitoring(self) -> None:
        """Stop health monitoring thread."""
        self._health_check_running = False
        
        if self._health_check_thread and self._health_check_thread.is_alive():
            self._health_check_thread.join(timeout=5.0)
        
        self.logger.debug("Health monitoring stopped")
    
    def _health_check_loop(self) -> None:
        """Health monitoring loop."""
        self.logger.debug("Health monitoring loop started")
        
        while self._health_check_running and not self._shutdown_event.is_set():
            try:
                self._perform_health_check()
                time.sleep(self.config.worker_health_check_interval)
            except Exception as e:
                self.logger.error(f"Health check error: {str(e)}")
                time.sleep(5.0)  # Wait before retrying
        
        self.logger.debug("Health monitoring loop stopped")
    
    def _perform_health_check(self) -> None:
        """Perform health check on all workers."""
        with self._lock:
            failed_workers = []
            
            for worker_id, worker in self._workers.items():
                if not worker.is_healthy():
                    failed_workers.append(worker_id)
                    self.logger.warning(f"Worker {worker_id} failed health check")
            
            # Restart failed workers if auto-restart is enabled
            for worker_id in failed_workers:
                if self.config.enable_adaptive_rate:  # Using this as a proxy for auto-restart
                    self.logger.info(f"Auto-restarting failed worker {worker_id}")
                    self.restart_failed_worker(worker_id)
    
    def is_running(self) -> bool:
        """
        Check if thread pool is running.
        
        Returns:
            True if any workers are running
        """
        with self._lock:
            return any(worker.is_alive() for worker in self._workers.values())
    
    def get_worker_count(self) -> int:
        """
        Get number of workers.
        
        Returns:
            Number of workers
        """
        with self._lock:
            return len(self._workers)
    
    def get_active_worker_count(self) -> int:
        """
        Get number of active (working) workers.
        
        Returns:
            Number of active workers
        """
        with self._lock:
            return len([
                w for w in self._workers.values() 
                if w.status.state == WorkerState.WORKING
            ])
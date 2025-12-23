"""
Basic monitoring for concurrent crawler system.
Provides real-time status monitoring and progress tracking.
"""

import time
import threading
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from dataclasses import dataclass, field

from memory_price_monitor.utils.logging import get_logger


@dataclass
class ThreadStatus:
    """Individual thread status information."""
    thread_id: str
    thread_name: str
    is_alive: bool
    current_task_id: Optional[str]
    task_start_time: Optional[datetime]
    tasks_completed: int
    tasks_failed: int
    last_activity: datetime
    state: str  # 'idle', 'working', 'error', 'shutdown'
    current_error: Optional[str]


@dataclass
class ErrorInfo:
    """Error information with context."""
    timestamp: datetime
    error_type: str
    error_message: str
    thread_id: Optional[str]
    task_id: Optional[str]
    stack_trace: Optional[str]
    severity: str  # 'low', 'medium', 'high', 'critical'


@dataclass
class ProgressInfo:
    """Detailed progress information."""
    total_tasks: int
    completed_tasks: int
    failed_tasks: int
    in_progress_tasks: int
    queued_tasks: int
    progress_percentage: float
    estimated_completion_time: Optional[datetime]
    throughput_per_second: float
    average_task_duration: float


@dataclass
class MonitoringSnapshot:
    """Snapshot of system monitoring data at a point in time."""
    timestamp: datetime
    system_status: Dict[str, Any]
    worker_stats: Dict[str, Any]
    scheduler_stats: Dict[str, Any]
    rate_controller_stats: Dict[str, Any]
    repository_stats: Dict[str, Any]
    performance_metrics: Dict[str, Any]
    # Enhanced monitoring data
    thread_statuses: List[ThreadStatus]
    progress_info: ProgressInfo
    recent_errors: List[ErrorInfo]


@dataclass
class MonitoringConfig:
    """Configuration for monitoring system."""
    update_interval: float = 5.0  # seconds
    max_snapshots: int = 100  # Keep last 100 snapshots
    enable_console_output: bool = True
    console_update_interval: float = 10.0  # seconds


class ConcurrentCrawlerMonitor:
    """
    Real-time monitoring for concurrent crawler system.
    
    Provides:
    - Real-time status updates
    - Progress tracking with detailed percentages
    - Performance metrics
    - Thread status monitoring
    - Error collection and analysis
    - Historical data collection
    """
    
    def __init__(self, controller, config: Optional[MonitoringConfig] = None):
        """
        Initialize monitoring system.
        
        Args:
            controller: ConcurrentCrawlerController instance to monitor
            config: Optional monitoring configuration
        """
        self.controller = controller
        self.config = config or MonitoringConfig()
        self.logger = get_logger(__name__)
        
        # Monitoring state
        self._monitoring_active = False
        self._monitoring_thread: Optional[threading.Thread] = None
        self._console_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        
        # Data collection
        self._snapshots: List[MonitoringSnapshot] = []
        self._snapshots_lock = threading.Lock()
        
        # Enhanced monitoring data
        self._thread_statuses: Dict[str, ThreadStatus] = {}
        self._thread_statuses_lock = threading.Lock()
        self._error_history: List[ErrorInfo] = []
        self._error_history_lock = threading.Lock()
        self._max_error_history = 100
        
        # Statistics
        self._start_time: Optional[datetime] = None
        self._last_console_update = datetime.now()
        self._task_completion_times: List[float] = []
        self._task_times_lock = threading.Lock()
        
        self.logger.debug("Concurrent crawler monitor initialized")
    
    def start_monitoring(self) -> None:
        """Start real-time monitoring."""
        if self._monitoring_active:
            self.logger.warning("Monitoring is already active")
            return
        
        self._monitoring_active = True
        self._start_time = datetime.now()
        self._stop_event.clear()
        
        # Start monitoring thread
        self._monitoring_thread = threading.Thread(
            target=self._monitoring_loop,
            name="CrawlerMonitor",
            daemon=True
        )
        self._monitoring_thread.start()
        
        # Start console output thread if enabled
        if self.config.enable_console_output:
            self._console_thread = threading.Thread(
                target=self._console_output_loop,
                name="MonitorConsole",
                daemon=True
            )
            self._console_thread.start()
        
        self.logger.info("Concurrent crawler monitoring started")
    
    def stop_monitoring(self) -> None:
        """Stop monitoring."""
        if not self._monitoring_active:
            return
        
        self._monitoring_active = False
        self._stop_event.set()
        
        # Wait for threads to finish
        if self._monitoring_thread:
            self._monitoring_thread.join(timeout=5.0)
        
        if self._console_thread:
            self._console_thread.join(timeout=5.0)
        
        self.logger.info("Concurrent crawler monitoring stopped")
    
    def _monitoring_loop(self) -> None:
        """Main monitoring loop."""
        while self._monitoring_active and not self._stop_event.wait(self.config.update_interval):
            try:
                snapshot = self._collect_snapshot()
                self._store_snapshot(snapshot)
            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {str(e)}")
    
    def _console_output_loop(self) -> None:
        """Console output loop for real-time status display."""
        while self._monitoring_active and not self._stop_event.wait(self.config.console_update_interval):
            try:
                self._print_status_update()
            except Exception as e:
                self.logger.error(f"Error in console output loop: {str(e)}")
    
    def _collect_snapshot(self) -> MonitoringSnapshot:
        """Collect current system snapshot with enhanced monitoring data."""
        timestamp = datetime.now()
        
        # Get status from controller
        system_status = self.controller.get_status()
        
        # Get performance report
        performance_report = self.controller.get_performance_report()
        
        # Collect thread statuses
        thread_statuses = self._collect_thread_statuses()
        
        # Calculate detailed progress information
        progress_info = self._calculate_progress_info(system_status, performance_report)
        
        # Get recent errors
        recent_errors = self._get_recent_errors(minutes=5)
        
        return MonitoringSnapshot(
            timestamp=timestamp,
            system_status=system_status,
            worker_stats=performance_report.get('component_statistics', {}).get('thread_pool', {}),
            scheduler_stats=performance_report.get('component_statistics', {}).get('scheduler', {}),
            rate_controller_stats=performance_report.get('component_statistics', {}).get('rate_controller', {}),
            repository_stats=performance_report.get('component_statistics', {}).get('repository', {}),
            performance_metrics=performance_report.get('summary', {}),
            thread_statuses=thread_statuses,
            progress_info=progress_info,
            recent_errors=recent_errors
        )
    
    def _collect_thread_statuses(self) -> List[ThreadStatus]:
        """Collect current status of all worker threads."""
        thread_statuses = []
        
        try:
            # Get thread pool manager from controller
            thread_pool = getattr(self.controller, '_thread_pool_manager', None)
            if not thread_pool:
                return thread_statuses
            
            # Get worker information
            workers = getattr(thread_pool, '_workers', [])
            
            for worker in workers:
                thread_id = getattr(worker, 'worker_id', 'unknown')
                thread = getattr(worker, '_thread', None)
                
                if thread:
                    # Get current task information
                    current_task = getattr(worker, '_current_task', None)
                    task_start_time = getattr(worker, '_task_start_time', None)
                    
                    # Get worker statistics
                    worker_stats = getattr(worker, '_stats', {})
                    
                    status = ThreadStatus(
                        thread_id=thread_id,
                        thread_name=thread.name,
                        is_alive=thread.is_alive(),
                        current_task_id=current_task.task_id if current_task else None,
                        task_start_time=task_start_time,
                        tasks_completed=worker_stats.get('tasks_completed', 0),
                        tasks_failed=worker_stats.get('tasks_failed', 0),
                        last_activity=worker_stats.get('last_activity', datetime.now()),
                        state=self._determine_thread_state(worker),
                        current_error=getattr(worker, '_last_error', None)
                    )
                    
                    thread_statuses.append(status)
                    
                    # Update internal tracking
                    with self._thread_statuses_lock:
                        self._thread_statuses[thread_id] = status
        
        except Exception as e:
            self.logger.warning(f"Failed to collect thread statuses: {str(e)}")
        
        return thread_statuses
    
    def _determine_thread_state(self, worker) -> str:
        """Determine the current state of a worker thread."""
        try:
            if not getattr(worker, '_thread', None) or not worker._thread.is_alive():
                return 'shutdown'
            
            if getattr(worker, '_last_error', None):
                return 'error'
            
            if getattr(worker, '_current_task', None):
                return 'working'
            
            return 'idle'
        
        except Exception:
            return 'unknown'
    
    def _calculate_progress_info(self, system_status: Dict[str, Any], performance_report: Dict[str, Any]) -> ProgressInfo:
        """Calculate detailed progress information."""
        try:
            total_tasks = system_status.get('total_tasks', 0)
            completed_tasks = system_status.get('completed_tasks', 0)
            failed_tasks = system_status.get('failed_tasks', 0)
            
            # Calculate in-progress and queued tasks
            scheduler_stats = performance_report.get('component_statistics', {}).get('scheduler', {})
            queued_tasks = scheduler_stats.get('pending_tasks', 0)
            in_progress_tasks = max(0, total_tasks - completed_tasks - failed_tasks - queued_tasks)
            
            # Calculate progress percentage
            progress_percentage = 0.0
            if total_tasks > 0:
                progress_percentage = ((completed_tasks + failed_tasks) / total_tasks) * 100.0
            
            # Calculate throughput
            elapsed_time = system_status.get('elapsed_time', 0)
            throughput_per_second = 0.0
            if elapsed_time > 0:
                throughput_per_second = (completed_tasks + failed_tasks) / elapsed_time
            
            # Calculate average task duration
            average_task_duration = 0.0
            with self._task_times_lock:
                if self._task_completion_times:
                    average_task_duration = sum(self._task_completion_times) / len(self._task_completion_times)
            
            # Estimate completion time
            estimated_completion_time = None
            if throughput_per_second > 0 and queued_tasks + in_progress_tasks > 0:
                remaining_seconds = (queued_tasks + in_progress_tasks) / throughput_per_second
                estimated_completion_time = datetime.now() + timedelta(seconds=remaining_seconds)
            
            return ProgressInfo(
                total_tasks=total_tasks,
                completed_tasks=completed_tasks,
                failed_tasks=failed_tasks,
                in_progress_tasks=in_progress_tasks,
                queued_tasks=queued_tasks,
                progress_percentage=progress_percentage,
                estimated_completion_time=estimated_completion_time,
                throughput_per_second=throughput_per_second,
                average_task_duration=average_task_duration
            )
        
        except Exception as e:
            self.logger.warning(f"Failed to calculate progress info: {str(e)}")
            return ProgressInfo(
                total_tasks=0, completed_tasks=0, failed_tasks=0,
                in_progress_tasks=0, queued_tasks=0, progress_percentage=0.0,
                estimated_completion_time=None, throughput_per_second=0.0,
                average_task_duration=0.0
            )
    
    def _store_snapshot(self, snapshot: MonitoringSnapshot) -> None:
        """Store snapshot in history."""
        with self._snapshots_lock:
            self._snapshots.append(snapshot)
            
            # Keep only the last N snapshots
            if len(self._snapshots) > self.config.max_snapshots:
                self._snapshots.pop(0)
    
    def _print_status_update(self) -> None:
        """Print enhanced status update to console."""
        if not self._snapshots:
            return
        
        with self._snapshots_lock:
            latest = self._snapshots[-1]
        
        # Calculate uptime
        uptime = (datetime.now() - self._start_time).total_seconds() if self._start_time else 0
        
        # Print status
        print(f"\n{'='*80}")
        print(f"CONCURRENT CRAWLER STATUS - {latest.timestamp.strftime('%H:%M:%S')}")
        print(f"{'='*80}")
        print(f"Uptime: {uptime:.0f}s")
        
        # System status
        status = latest.system_status.get('status', 'unknown')
        print(f"Status: {status}")
        
        if status == 'running':
            # Enhanced progress information
            progress = latest.progress_info
            print(f"Progress: {progress.progress_percentage:.1f}% ({progress.completed_tasks}/{progress.total_tasks} tasks)")
            print(f"  Completed: {progress.completed_tasks}")
            print(f"  Failed: {progress.failed_tasks}")
            print(f"  In Progress: {progress.in_progress_tasks}")
            print(f"  Queued: {progress.queued_tasks}")
            
            # Throughput and timing
            print(f"Throughput: {progress.throughput_per_second:.2f} tasks/s")
            print(f"Avg Task Duration: {progress.average_task_duration:.2f}s")
            
            # Estimated completion time
            if progress.estimated_completion_time:
                eta_seconds = (progress.estimated_completion_time - datetime.now()).total_seconds()
                print(f"ETA: {eta_seconds:.0f}s ({progress.estimated_completion_time.strftime('%H:%M:%S')})")
        
        # Thread status summary
        thread_statuses = latest.thread_statuses
        if thread_statuses:
            alive_threads = sum(1 for t in thread_statuses if t.is_alive)
            working_threads = sum(1 for t in thread_statuses if t.state == 'working')
            idle_threads = sum(1 for t in thread_statuses if t.state == 'idle')
            error_threads = sum(1 for t in thread_statuses if t.state == 'error')
            
            print(f"Threads: {alive_threads} alive ({working_threads} working, {idle_threads} idle, {error_threads} error)")
            
            # Show individual thread details
            for thread in thread_statuses[:5]:  # Show first 5 threads
                task_info = f"Task: {thread.current_task_id}" if thread.current_task_id else "Idle"
                duration = ""
                if thread.task_start_time:
                    duration = f" ({(datetime.now() - thread.task_start_time).total_seconds():.1f}s)"
                
                print(f"  {thread.thread_id}: {thread.state} - {task_info}{duration}")
        
        # Rate controller stats
        rate_stats = latest.rate_controller_stats
        if rate_stats:
            current_rate = rate_stats.get('current_rate', 0)
            rate_limit = rate_stats.get('requests_per_second_limit', 0)
            active_requests = rate_stats.get('active_requests', 0)
            print(f"Rate: {current_rate:.1f}/{rate_limit} req/s, Active: {active_requests}")
        
        # Error summary
        recent_errors = latest.recent_errors
        if recent_errors:
            error_count = len(recent_errors)
            critical_errors = sum(1 for e in recent_errors if e.severity == 'critical')
            high_errors = sum(1 for e in recent_errors if e.severity == 'high')
            
            print(f"Recent Errors: {error_count} total ({critical_errors} critical, {high_errors} high)")
            
            # Show latest error
            if recent_errors:
                latest_error = recent_errors[-1]
                print(f"  Latest: [{latest_error.severity}] {latest_error.error_type}: {latest_error.error_message[:50]}...")
        
        # Performance metrics
        perf = latest.performance_metrics
        if perf:
            products_found = perf.get('total_products_found', 0)
            products_saved = perf.get('total_products_saved', 0)
            
            print(f"Products: {products_found} found, {products_saved} saved")
        
        print(f"{'='*80}")
    
    def record_task_completion(self, task_id: str, execution_time: float) -> None:
        """
        Record task completion for progress tracking.
        
        Args:
            task_id: ID of completed task
            execution_time: Task execution time in seconds
        """
        with self._task_times_lock:
            self._task_completion_times.append(execution_time)
            
            # Keep only recent completion times (last 100)
            if len(self._task_completion_times) > 100:
                self._task_completion_times.pop(0)
    
    def record_error(self, error_type: str, error_message: str, 
                    thread_id: Optional[str] = None, task_id: Optional[str] = None,
                    stack_trace: Optional[str] = None, severity: str = 'medium') -> None:
        """
        Record an error for monitoring.
        
        Args:
            error_type: Type of error (e.g., 'CrawlerError', 'NetworkError')
            error_message: Error message
            thread_id: ID of thread where error occurred
            task_id: ID of task that caused error
            stack_trace: Optional stack trace
            severity: Error severity level
        """
        error_info = ErrorInfo(
            timestamp=datetime.now(),
            error_type=error_type,
            error_message=error_message,
            thread_id=thread_id,
            task_id=task_id,
            stack_trace=stack_trace,
            severity=severity
        )
        
        with self._error_history_lock:
            self._error_history.append(error_info)
            
            # Keep only recent errors
            if len(self._error_history) > self._max_error_history:
                self._error_history.pop(0)
        
        # Log error based on severity
        if severity == 'critical':
            self.logger.critical(f"Critical error in {thread_id}: {error_message}")
        elif severity == 'high':
            self.logger.error(f"High severity error in {thread_id}: {error_message}")
        else:
            self.logger.warning(f"Error in {thread_id}: {error_message}")
    
    def _get_recent_errors(self, minutes: int = 5) -> List[ErrorInfo]:
        """
        Get errors from the last N minutes.
        
        Args:
            minutes: Number of minutes to look back
            
        Returns:
            List of recent errors
        """
        cutoff_time = datetime.now() - timedelta(minutes=minutes)
        
        with self._error_history_lock:
            return [e for e in self._error_history if e.timestamp >= cutoff_time]
    
    def get_thread_status(self, thread_id: str) -> Optional[ThreadStatus]:
        """
        Get status of specific thread.
        
        Args:
            thread_id: Thread ID
            
        Returns:
            Thread status or None if not found
        """
        with self._thread_statuses_lock:
            return self._thread_statuses.get(thread_id)
    
    def get_all_thread_statuses(self) -> List[ThreadStatus]:
        """
        Get status of all threads.
        
        Returns:
            List of all thread statuses
        """
        with self._thread_statuses_lock:
            return list(self._thread_statuses.values())
    
    def get_error_summary(self, hours: int = 1) -> Dict[str, Any]:
        """
        Get error summary for the last N hours.
        
        Args:
            hours: Number of hours to analyze
            
        Returns:
            Error summary statistics
        """
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        with self._error_history_lock:
            recent_errors = [e for e in self._error_history if e.timestamp >= cutoff_time]
        
        if not recent_errors:
            return {
                'total_errors': 0,
                'error_rate_per_hour': 0.0,
                'severity_breakdown': {},
                'error_types': {},
                'affected_threads': []
            }
        
        # Analyze errors
        severity_counts = {}
        type_counts = {}
        affected_threads = set()
        
        for error in recent_errors:
            # Count by severity
            severity_counts[error.severity] = severity_counts.get(error.severity, 0) + 1
            
            # Count by type
            type_counts[error.error_type] = type_counts.get(error.error_type, 0) + 1
            
            # Track affected threads
            if error.thread_id:
                affected_threads.add(error.thread_id)
        
        error_rate = len(recent_errors) / hours
        
        return {
            'total_errors': len(recent_errors),
            'error_rate_per_hour': error_rate,
            'severity_breakdown': severity_counts,
            'error_types': type_counts,
            'affected_threads': list(affected_threads),
            'time_range_hours': hours
        }
    
    def get_current_status(self) -> Optional[MonitoringSnapshot]:
        """Get current monitoring snapshot."""
        with self._snapshots_lock:
            return self._snapshots[-1] if self._snapshots else None
    
    def get_historical_data(self, minutes: int = 10) -> List[MonitoringSnapshot]:
        """
        Get historical monitoring data.
        
        Args:
            minutes: Number of minutes of history to return
            
        Returns:
            List of snapshots from the last N minutes
        """
        cutoff_time = datetime.now() - timedelta(minutes=minutes)
        
        with self._snapshots_lock:
            return [s for s in self._snapshots if s.timestamp >= cutoff_time]
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get performance summary over monitoring period."""
        if not self._snapshots:
            return {}
        
        with self._snapshots_lock:
            snapshots = self._snapshots.copy()
        
        if len(snapshots) < 2:
            return {}
        
        first = snapshots[0]
        latest = snapshots[-1]
        
        # Calculate averages and trends
        total_duration = (latest.timestamp - first.timestamp).total_seconds()
        
        # Rate statistics
        rates = [s.rate_controller_stats.get('current_rate', 0) for s in snapshots if s.rate_controller_stats]
        avg_rate = sum(rates) / len(rates) if rates else 0
        max_rate = max(rates) if rates else 0
        
        # Worker statistics
        worker_counts = [s.worker_stats.get('healthy_workers', 0) for s in snapshots if s.worker_stats]
        avg_workers = sum(worker_counts) / len(worker_counts) if worker_counts else 0
        
        # Performance metrics
        throughputs = [s.performance_metrics.get('throughput_products_per_second', 0) 
                      for s in snapshots if s.performance_metrics]
        avg_throughput = sum(throughputs) / len(throughputs) if throughputs else 0
        max_throughput = max(throughputs) if throughputs else 0
        
        return {
            'monitoring_duration_seconds': total_duration,
            'snapshots_collected': len(snapshots),
            'average_request_rate': avg_rate,
            'max_request_rate': max_rate,
            'average_healthy_workers': avg_workers,
            'average_throughput': avg_throughput,
            'max_throughput': max_throughput,
            'first_snapshot': first.timestamp.isoformat(),
            'latest_snapshot': latest.timestamp.isoformat()
        }
    
    def export_monitoring_data(self) -> List[Dict[str, Any]]:
        """Export all monitoring data as list of dictionaries."""
        with self._snapshots_lock:
            return [
                {
                    'timestamp': s.timestamp.isoformat(),
                    'system_status': s.system_status,
                    'worker_stats': s.worker_stats,
                    'scheduler_stats': s.scheduler_stats,
                    'rate_controller_stats': s.rate_controller_stats,
                    'repository_stats': s.repository_stats,
                    'performance_metrics': s.performance_metrics
                }
                for s in self._snapshots
            ]
    
    def is_monitoring_active(self) -> bool:
        """Check if monitoring is currently active."""
        return self._monitoring_active
    
    def get_monitoring_stats(self) -> Dict[str, Any]:
        """Get statistics about the monitoring system itself."""
        with self._snapshots_lock:
            snapshot_count = len(self._snapshots)
        
        return {
            'monitoring_active': self._monitoring_active,
            'snapshots_collected': snapshot_count,
            'update_interval': self.config.update_interval,
            'console_output_enabled': self.config.enable_console_output,
            'start_time': self._start_time.isoformat() if self._start_time else None,
            'uptime_seconds': (datetime.now() - self._start_time).total_seconds() if self._start_time else 0
        }
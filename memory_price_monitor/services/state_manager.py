"""
System state management and persistence for memory price monitoring.
"""

import json
import os
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field, asdict
from pathlib import Path
import signal
import atexit

from memory_price_monitor.utils.logging import get_logger
from memory_price_monitor.utils.errors import StateManagementError


@dataclass
class CrawlerState:
    """State information for a crawler."""
    name: str
    last_successful_crawl: Optional[datetime] = None
    last_attempt: Optional[datetime] = None
    consecutive_failures: int = 0
    total_successful_crawls: int = 0
    total_failed_crawls: int = 0
    is_enabled: bool = True
    last_error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SystemState:
    """Overall system state."""
    started_at: datetime
    last_updated: datetime
    crawlers: Dict[str, CrawlerState] = field(default_factory=dict)
    scheduler_running: bool = False
    total_products_processed: int = 0
    total_errors: int = 0
    uptime_seconds: float = 0.0
    graceful_shutdown_requested: bool = False
    recovery_mode: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


class StateManager:
    """Manages system state persistence and recovery."""
    
    def __init__(self, state_file_path: str = "system_state.json"):
        """
        Initialize state manager.
        
        Args:
            state_file_path: Path to state persistence file
        """
        self.state_file_path = Path(state_file_path)
        self.state_file_path.parent.mkdir(parents=True, exist_ok=True)
        
        self._state: SystemState = SystemState(
            started_at=datetime.now(),
            last_updated=datetime.now()
        )
        self._lock = threading.RLock()
        self._auto_save_interval = 30.0  # seconds
        self._auto_save_thread: Optional[threading.Thread] = None
        self._shutdown_requested = False
        
        self.logger = get_logger(__name__)
        
        # Register shutdown handlers
        self._register_shutdown_handlers()
        
        # Load existing state if available
        self._load_state()
        
        # Start auto-save thread
        self._start_auto_save()
    
    def _register_shutdown_handlers(self) -> None:
        """Register handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            self.logger.info(f"Received signal {signum}, initiating graceful shutdown")
            self.request_graceful_shutdown()
        
        # Register signal handlers
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        
        # Register atexit handler
        atexit.register(self._cleanup_on_exit)
    
    def _cleanup_on_exit(self) -> None:
        """Cleanup when process exits."""
        try:
            self.logger.info("Process exiting, saving final state")
            self.save_state()
            self.stop_auto_save()
        except Exception as e:
            self.logger.error(f"Error during exit cleanup: {e}")
    
    def _load_state(self) -> None:
        """Load state from persistence file."""
        try:
            if self.state_file_path.exists():
                with open(self.state_file_path, 'r') as f:
                    data = json.load(f)
                
                # Convert datetime strings back to datetime objects
                self._state = self._deserialize_state(data)
                
                # Update startup time and mark as recovery mode
                self._state.recovery_mode = True
                self._state.last_updated = datetime.now()
                
                self.logger.info(f"Loaded existing state from {self.state_file_path}")
                self.logger.info(f"Previous session started at: {self._state.started_at}")
                
            else:
                self.logger.info("No existing state file found, starting with fresh state")
                
        except Exception as e:
            self.logger.error(f"Failed to load state from {self.state_file_path}: {e}")
            # Continue with fresh state
            self._state = SystemState(
                started_at=datetime.now(),
                last_updated=datetime.now()
            )
    
    def save_state(self) -> None:
        """Save current state to persistence file."""
        try:
            with self._lock:
                # Update last_updated timestamp
                self._state.last_updated = datetime.now()
                
                # Calculate uptime
                self._state.uptime_seconds = (
                    self._state.last_updated - self._state.started_at
                ).total_seconds()
                
                # Serialize state to JSON
                data = self._serialize_state(self._state)
                
                # Write to temporary file first, then rename (atomic operation)
                temp_file = self.state_file_path.with_suffix('.tmp')
                with open(temp_file, 'w') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                
                # Atomic rename
                temp_file.replace(self.state_file_path)
                
                self.logger.debug(f"State saved to {self.state_file_path}")
                
        except Exception as e:
            self.logger.error(f"Failed to save state: {e}")
            raise StateManagementError(f"Failed to save state: {e}")
    
    def _serialize_state(self, state: SystemState) -> Dict[str, Any]:
        """Serialize state to JSON-compatible dictionary."""
        data = asdict(state)
        
        # Convert datetime objects to ISO strings
        data['started_at'] = state.started_at.isoformat()
        data['last_updated'] = state.last_updated.isoformat()
        
        # Convert crawler states
        for crawler_name, crawler_state in data['crawlers'].items():
            if crawler_state['last_successful_crawl']:
                crawler_state['last_successful_crawl'] = crawler_state['last_successful_crawl'].isoformat()
            if crawler_state['last_attempt']:
                crawler_state['last_attempt'] = crawler_state['last_attempt'].isoformat()
        
        return data
    
    def _deserialize_state(self, data: Dict[str, Any]) -> SystemState:
        """Deserialize state from JSON dictionary."""
        # Convert datetime strings back to datetime objects
        started_at = datetime.fromisoformat(data['started_at'])
        last_updated = datetime.fromisoformat(data['last_updated'])
        
        # Convert crawler states
        crawlers = {}
        for crawler_name, crawler_data in data.get('crawlers', {}).items():
            last_successful_crawl = None
            if crawler_data.get('last_successful_crawl'):
                last_successful_crawl = datetime.fromisoformat(crawler_data['last_successful_crawl'])
            
            last_attempt = None
            if crawler_data.get('last_attempt'):
                last_attempt = datetime.fromisoformat(crawler_data['last_attempt'])
            
            crawlers[crawler_name] = CrawlerState(
                name=crawler_data['name'],
                last_successful_crawl=last_successful_crawl,
                last_attempt=last_attempt,
                consecutive_failures=crawler_data.get('consecutive_failures', 0),
                total_successful_crawls=crawler_data.get('total_successful_crawls', 0),
                total_failed_crawls=crawler_data.get('total_failed_crawls', 0),
                is_enabled=crawler_data.get('is_enabled', True),
                last_error=crawler_data.get('last_error'),
                metadata=crawler_data.get('metadata', {})
            )
        
        return SystemState(
            started_at=started_at,
            last_updated=last_updated,
            crawlers=crawlers,
            scheduler_running=data.get('scheduler_running', False),
            total_products_processed=data.get('total_products_processed', 0),
            total_errors=data.get('total_errors', 0),
            uptime_seconds=data.get('uptime_seconds', 0.0),
            graceful_shutdown_requested=data.get('graceful_shutdown_requested', False),
            recovery_mode=data.get('recovery_mode', False),
            metadata=data.get('metadata', {})
        )
    
    def _start_auto_save(self) -> None:
        """Start automatic state saving thread."""
        def auto_save_loop():
            while not self._shutdown_requested:
                try:
                    time.sleep(self._auto_save_interval)
                    if not self._shutdown_requested:
                        self.save_state()
                except Exception as e:
                    self.logger.error(f"Error in auto-save loop: {e}")
        
        self._auto_save_thread = threading.Thread(
            target=auto_save_loop,
            name="state_auto_save",
            daemon=True
        )
        self._auto_save_thread.start()
        self.logger.info(f"Started auto-save thread (interval: {self._auto_save_interval}s)")
    
    def stop_auto_save(self) -> None:
        """Stop automatic state saving."""
        self._shutdown_requested = True
        if self._auto_save_thread and self._auto_save_thread.is_alive():
            self._auto_save_thread.join(timeout=5.0)
            self.logger.info("Stopped auto-save thread")
    
    def update_crawler_success(self, crawler_name: str, products_processed: int = 0) -> None:
        """
        Update crawler state after successful crawl.
        
        Args:
            crawler_name: Name of the crawler
            products_processed: Number of products processed
        """
        with self._lock:
            now = datetime.now()
            
            if crawler_name not in self._state.crawlers:
                self._state.crawlers[crawler_name] = CrawlerState(name=crawler_name)
            
            crawler_state = self._state.crawlers[crawler_name]
            crawler_state.last_successful_crawl = now
            crawler_state.last_attempt = now
            crawler_state.consecutive_failures = 0
            crawler_state.total_successful_crawls += 1
            crawler_state.last_error = None
            
            # Update system totals
            self._state.total_products_processed += products_processed
            
            self.logger.info(f"Updated success state for crawler {crawler_name}: "
                           f"products={products_processed}, "
                           f"total_successes={crawler_state.total_successful_crawls}")
    
    def update_crawler_failure(self, crawler_name: str, error_message: str) -> None:
        """
        Update crawler state after failed crawl.
        
        Args:
            crawler_name: Name of the crawler
            error_message: Error message from failed crawl
        """
        with self._lock:
            now = datetime.now()
            
            if crawler_name not in self._state.crawlers:
                self._state.crawlers[crawler_name] = CrawlerState(name=crawler_name)
            
            crawler_state = self._state.crawlers[crawler_name]
            crawler_state.last_attempt = now
            crawler_state.consecutive_failures += 1
            crawler_state.total_failed_crawls += 1
            crawler_state.last_error = error_message
            
            # Update system totals
            self._state.total_errors += 1
            
            self.logger.warning(f"Updated failure state for crawler {crawler_name}: "
                              f"consecutive_failures={crawler_state.consecutive_failures}, "
                              f"error={error_message}")
    
    def get_crawler_state(self, crawler_name: str) -> Optional[CrawlerState]:
        """
        Get state for a specific crawler.
        
        Args:
            crawler_name: Name of the crawler
            
        Returns:
            Crawler state or None if not found
        """
        with self._lock:
            return self._state.crawlers.get(crawler_name)
    
    def get_last_successful_crawl(self, crawler_name: str) -> Optional[datetime]:
        """
        Get last successful crawl timestamp for a crawler.
        
        Args:
            crawler_name: Name of the crawler
            
        Returns:
            Last successful crawl timestamp or None
        """
        crawler_state = self.get_crawler_state(crawler_name)
        return crawler_state.last_successful_crawl if crawler_state else None
    
    def get_all_crawler_states(self) -> Dict[str, CrawlerState]:
        """Get states for all crawlers."""
        with self._lock:
            return self._state.crawlers.copy()
    
    def get_system_state(self) -> SystemState:
        """Get current system state."""
        with self._lock:
            # Update uptime before returning
            self._state.uptime_seconds = (
                datetime.now() - self._state.started_at
            ).total_seconds()
            return self._state
    
    def set_scheduler_running(self, running: bool) -> None:
        """Update scheduler running state."""
        with self._lock:
            self._state.scheduler_running = running
            self.logger.info(f"Scheduler running state updated: {running}")
    
    def request_graceful_shutdown(self) -> None:
        """Request graceful shutdown of the system."""
        with self._lock:
            self._state.graceful_shutdown_requested = True
            self.logger.info("Graceful shutdown requested")
    
    def is_graceful_shutdown_requested(self) -> bool:
        """Check if graceful shutdown has been requested."""
        with self._lock:
            return self._state.graceful_shutdown_requested
    
    def is_recovery_mode(self) -> bool:
        """Check if system is in recovery mode (restarted from saved state)."""
        with self._lock:
            return self._state.recovery_mode
    
    def clear_recovery_mode(self) -> None:
        """Clear recovery mode flag."""
        with self._lock:
            self._state.recovery_mode = False
            self.logger.info("Recovery mode cleared")
    
    def get_health_summary(self) -> Dict[str, Any]:
        """
        Get health summary for monitoring.
        
        Returns:
            Dictionary with health information
        """
        with self._lock:
            now = datetime.now()
            uptime = (now - self._state.started_at).total_seconds()
            
            # Calculate crawler health metrics
            healthy_crawlers = 0
            unhealthy_crawlers = 0
            stale_crawlers = 0
            
            for crawler_state in self._state.crawlers.values():
                if not crawler_state.is_enabled:
                    continue
                
                if crawler_state.consecutive_failures >= 3:
                    unhealthy_crawlers += 1
                elif crawler_state.last_successful_crawl:
                    hours_since_success = (now - crawler_state.last_successful_crawl).total_seconds() / 3600
                    if hours_since_success > 25:  # More than 25 hours
                        stale_crawlers += 1
                    else:
                        healthy_crawlers += 1
            
            return {
                'uptime_seconds': uptime,
                'uptime_hours': uptime / 3600,
                'scheduler_running': self._state.scheduler_running,
                'total_crawlers': len(self._state.crawlers),
                'healthy_crawlers': healthy_crawlers,
                'unhealthy_crawlers': unhealthy_crawlers,
                'stale_crawlers': stale_crawlers,
                'total_products_processed': self._state.total_products_processed,
                'total_errors': self._state.total_errors,
                'recovery_mode': self._state.recovery_mode,
                'graceful_shutdown_requested': self._state.graceful_shutdown_requested,
                'last_state_save': self._state.last_updated.isoformat()
            }
    
    def cleanup_old_crawler_states(self, max_age_days: int = 30) -> int:
        """
        Clean up old crawler states that haven't been active.
        
        Args:
            max_age_days: Maximum age in days for inactive crawlers
            
        Returns:
            Number of crawler states removed
        """
        with self._lock:
            cutoff_time = datetime.now() - timedelta(days=max_age_days)
            
            old_crawlers = []
            for name, state in self._state.crawlers.items():
                last_activity = state.last_attempt or state.last_successful_crawl
                if last_activity and last_activity < cutoff_time:
                    old_crawlers.append(name)
            
            for name in old_crawlers:
                del self._state.crawlers[name]
            
            if old_crawlers:
                self.logger.info(f"Cleaned up {len(old_crawlers)} old crawler states")
            
            return len(old_crawlers)
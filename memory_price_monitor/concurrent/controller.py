"""
Main concurrent crawler controller.
Integrates all concurrent components for coordinated parallel crawling.
"""

import threading
import time
from typing import List, Dict, Any, Optional, Callable
from datetime import datetime

from memory_price_monitor.utils.logging import get_logger
from memory_price_monitor.utils.errors import CrawlerError
from memory_price_monitor.crawlers.base import BaseCrawler, CrawlerRegistry
from memory_price_monitor.crawlers.playwright_zol_crawler import PlaywrightZOLCrawler
from memory_price_monitor.data.database_factory import DatabaseFactory
from memory_price_monitor.data.models import StandardizedProduct
from .models import (
    ConcurrentConfig, 
    CrawlTask, 
    WorkerStatus, 
    CrawlResult,
    TaskResult,
    ConcurrentResultCollector,
    WorkerState,
    TaskStatus
)
from .thread_safe import ThreadSafeCounter, ThreadSafeQueue
from .scheduler import TaskScheduler
from .thread_pool import ThreadPoolManager
from .rate_controller import RateController


logger = get_logger(__name__)


class ConcurrentCrawlerController:
    """Main controller for concurrent crawler system."""
    
    def __init__(self, config: ConcurrentConfig):
        """
        Initialize concurrent crawler controller.
        
        Args:
            config: Concurrent crawler configuration
        """
        self.config = config
        self.logger = get_logger(__name__)
        
        # Core components
        self._task_scheduler: Optional[TaskScheduler] = None
        self._thread_pool_manager: Optional[ThreadPoolManager] = None
        self._rate_controller: Optional[RateController] = None
        self._result_collector: Optional[ConcurrentResultCollector] = None
        self._repository: Optional[Any] = None  # Will be ThreadSafePriceRepository
        
        # Crawler registry and instances
        self._crawler_registry = CrawlerRegistry()
        self._crawler_instances: Dict[str, BaseCrawler] = {}
        
        # Control flags
        self._shutdown_event = threading.Event()
        self._started = False
        self._completed = False
        
        # Counters
        self._tasks_submitted = ThreadSafeCounter()
        self._tasks_completed = ThreadSafeCounter()
        self._tasks_failed = ThreadSafeCounter()
        
        # Timing
        self._start_time: Optional[datetime] = None
        self._end_time: Optional[datetime] = None
        
        # Thread safety
        self._lock = threading.Lock()
        
        # Initialize components
        self._initialize_system()
    
    def _initialize_system(self) -> None:
        """Initialize all system components with improved error handling."""
        initialization_steps = []
        
        try:
            self.logger.info("Initializing concurrent crawler system...")
            
            # Step 1: Initialize database and repository
            initialization_steps.append("database")
            self._initialize_database()
            self.logger.debug("✓ Database and repository initialized")
            
            # Step 2: Initialize crawler registry
            initialization_steps.append("crawlers")
            self._initialize_crawlers()
            self.logger.debug("✓ Crawler registry initialized")
            
            # Step 3: Initialize core concurrent components
            initialization_steps.append("concurrent_components")
            self._initialize_concurrent_components()
            self.logger.debug("✓ Concurrent components initialized")
            
            self.logger.info("Concurrent crawler system initialized successfully")
            
        except Exception as e:
            error_msg = f"System initialization failed at step '{initialization_steps[-1] if initialization_steps else 'unknown'}': {str(e)}"
            self.logger.error(error_msg)
            
            # Attempt to cleanup any partially initialized components
            try:
                self._cleanup()
            except Exception as cleanup_error:
                self.logger.error(f"Cleanup during failed initialization also failed: {str(cleanup_error)}")
            
            raise CrawlerError(error_msg)
    
    def _initialize_database(self) -> None:
        """Initialize database connection and thread-safe repository."""
        try:
            # Import here to avoid circular import
            from memory_price_monitor.data.thread_safe_repository import ThreadSafePriceRepository
            
            # Create database configuration (default to SQLite)
            from config import DatabaseConfig
            db_config = DatabaseConfig(
                db_type="sqlite",
                sqlite_path="data/memory_price_monitor.db"
            )
            
            # Create database manager using factory
            db_manager = DatabaseFactory.create_database_manager(db_config)
            
            # Create thread-safe repository
            self._repository = ThreadSafePriceRepository(
                db_manager=db_manager,
                max_connections=self.config.max_workers + 2  # Extra connections for overhead
            )
            
            self.logger.debug("Database and repository initialized")
            
        except Exception as e:
            raise CrawlerError(f"Database initialization failed: {str(e)}")
    
    def _initialize_crawlers(self) -> None:
        """Initialize crawler registry and register available crawlers."""
        try:
            # Register ZOL Playwright crawler
            zol_config = {
                'headless': True,
                'max_pages': 2,  # Limit pages for concurrent crawling
                'min_delay': 1.0,
                'max_delay': 3.0,
                'search_keywords': ['内存条', 'DDR4内存', 'DDR5内存']
            }
            
            self._crawler_registry.register(
                name='zol_playwright',
                crawler_class=PlaywrightZOLCrawler,
                default_config=zol_config
            )
            
            self.logger.debug("Crawler registry initialized with available crawlers")
            
        except Exception as e:
            raise CrawlerError(f"Crawler initialization failed: {str(e)}")
    
    def _initialize_concurrent_components(self) -> None:
        """Initialize concurrent processing components."""
        try:
            # Initialize task scheduler
            self._task_scheduler = TaskScheduler(
                max_queue_size=1000,
                enable_task_stealing=self.config.enable_task_stealing
            )
            
            # Initialize rate controller
            self._rate_controller = RateController(
                requests_per_second=self.config.requests_per_second,
                max_concurrent_requests=self.config.max_concurrent_requests
            )
            
            # Initialize result collector
            self._result_collector = ConcurrentResultCollector()
            
            # Initialize thread pool manager
            self._thread_pool_manager = ThreadPoolManager(self.config)
            
            # Set up thread pool with required components
            self._thread_pool_manager.initialize(
                task_queue=self._task_scheduler._task_queue,  # Access scheduler's queue
                result_callback=self._handle_task_result,
                task_processor=self._process_crawl_task
            )
            
            self.logger.debug("Concurrent components initialized")
            
        except Exception as e:
            raise CrawlerError(f"Concurrent components initialization failed: {str(e)}")
    
    def _process_crawl_task(self, task: CrawlTask, worker_id: str) -> TaskResult:
        """
        Process a single crawl task.
        
        Args:
            task: Crawl task to process
            worker_id: ID of worker processing the task
            
        Returns:
            Task result
        """
        start_time = datetime.now()
        
        try:
            self.logger.debug(f"Worker {worker_id} processing task {task.task_id} for brand {task.brand}")
            
            # Acquire rate limit permit
            if not self._rate_controller.wait_for_permit(worker_id, timeout=30.0):
                raise CrawlerError("Failed to acquire rate limit permit")
            
            try:
                # Get or create crawler instance for this brand
                crawler = self._get_crawler_for_brand(task.brand)
                
                # Execute crawling
                crawl_result = crawler.execute()
                
                if not crawl_result.success:
                    raise CrawlerError(f"Crawl failed: {', '.join(crawl_result.errors)}")
                
                # Process and save products
                products_saved = 0
                for raw_product in crawler.fetch_products():
                    try:
                        # Convert to standardized product
                        standardized_product = self._convert_to_standardized_product(raw_product, task.brand)
                        
                        # Save to repository
                        if self._repository.save_price_record(standardized_product, use_batch=True):
                            products_saved += 1
                        
                    except Exception as e:
                        self.logger.warning(f"Failed to process product from {task.brand}: {str(e)}")
                        continue
                
                # Create successful result
                execution_time = (datetime.now() - start_time).total_seconds()
                
                return TaskResult(
                    task_id=task.task_id,
                    worker_id=worker_id,
                    success=True,
                    products_found=crawl_result.products_found,
                    products_extracted=products_saved,
                    execution_time=execution_time,
                    metadata={
                        'brand': task.brand,
                        'crawl_errors': crawl_result.errors,
                        'crawler_type': 'zol_playwright'
                    }
                )
                
            finally:
                # Always release rate limit permit
                self._rate_controller.release_permit(worker_id)
        
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            error_message = f"Task processing failed: {str(e)}"
            
            self.logger.error(f"Worker {worker_id} failed task {task.task_id}: {error_message}")
            
            return TaskResult(
                task_id=task.task_id,
                worker_id=worker_id,
                success=False,
                error_message=error_message,
                execution_time=execution_time,
                metadata={'brand': task.brand}
            )
    
    def _get_crawler_for_brand(self, brand: str) -> BaseCrawler:
        """
        Get or create crawler instance for a specific brand.
        
        Args:
            brand: Brand name
            
        Returns:
            Crawler instance
        """
        if brand not in self._crawler_instances:
            # Create brand-specific configuration
            brand_config = {
                'search_keywords': [brand, f"{brand}内存", f"{brand}内存条"],
                'max_pages': 1,  # Limit pages per brand for concurrent crawling
                'min_delay': 1.0,
                'max_delay': 2.0
            }
            
            # Get crawler from registry
            crawler = self._crawler_registry.get_crawler('zol_playwright', brand_config)
            self._crawler_instances[brand] = crawler
            
            self.logger.debug(f"Created crawler instance for brand: {brand}")
        
        return self._crawler_instances[brand]
    
    def _convert_to_standardized_product(self, raw_product, brand: str) -> StandardizedProduct:
        """
        Convert raw product to standardized product format.
        
        Args:
            raw_product: Raw product from crawler
            brand: Brand name
            
        Returns:
            Standardized product
        """
        from decimal import Decimal
        
        raw_data = raw_product.raw_data
        
        # Extract standardized fields
        return StandardizedProduct(
            source=raw_product.source,
            product_id=raw_product.product_id,
            brand=raw_data.get('brand', brand),
            model=raw_data.get('name', ''),
            capacity=self._extract_capacity(raw_data.get('name', '')),
            frequency=self._extract_frequency(raw_data.get('name', '')),
            type=self._extract_memory_type(raw_data.get('name', '')),
            current_price=Decimal(str(raw_data.get('current_price', 0))),
            original_price=Decimal(str(raw_data.get('market_price', 0))),
            url=raw_product.url,
            timestamp=raw_product.timestamp,
            metadata={
                'image_url': raw_data.get('image_url', ''),
                'rating': raw_data.get('rating', 0),
                'review_count': raw_data.get('review_count', 0),
                'specs': raw_data.get('specs', {}),
                'search_keyword': raw_data.get('search_keyword', brand)
            }
        )
    
    def _extract_capacity(self, name: str) -> str:
        """Extract memory capacity from product name."""
        import re
        
        # Look for patterns like "8GB", "16G", "32GB"
        capacity_match = re.search(r'(\d+)\s*GB?', name, re.IGNORECASE)
        if capacity_match:
            return f"{capacity_match.group(1)}GB"
        
        return ""
    
    def _extract_frequency(self, name: str) -> str:
        """Extract memory frequency from product name."""
        import re
        
        # Look for patterns like "3200MHz", "DDR4-3200"
        freq_match = re.search(r'(\d{4})\s*MHz?|DDR\d-(\d{4})', name, re.IGNORECASE)
        if freq_match:
            freq = freq_match.group(1) or freq_match.group(2)
            return f"{freq}MHz"
        
        return ""
    
    def _extract_memory_type(self, name: str) -> str:
        """Extract memory type from product name."""
        import re
        
        # Look for DDR4, DDR5, etc.
        type_match = re.search(r'DDR\d+', name, re.IGNORECASE)
        if type_match:
            return type_match.group(0).upper()
        
        return "DDR4"  # Default assumption
    
    def _handle_task_result(self, result: TaskResult) -> None:
        """
        Handle completed task result.
        
        Args:
            result: Task result from worker
        """
        try:
            # Add result to collector
            self._result_collector.add_result(result)
            
            # Record task completion time for monitoring
            if hasattr(self, '_monitor') and self._monitor:
                self._monitor.record_task_completion(result.task_id, result.execution_time)
            
            # Update counters
            if result.success:
                self._tasks_completed.increment()
                self._task_scheduler.mark_task_completed(result.task_id, result.metadata)
                self.logger.debug(f"Task {result.task_id} completed successfully in {result.execution_time:.2f}s")
            else:
                self._tasks_failed.increment()
                error = Exception(result.error_message or "Unknown error")
                self._task_scheduler.mark_task_failed(result.task_id, error, retry=True)
                
                # Record error for monitoring
                if hasattr(self, '_monitor') and self._monitor:
                    self._monitor.record_error(
                        error_type="TaskExecutionError",
                        error_message=result.error_message or "Unknown error",
                        thread_id=result.worker_id,
                        task_id=result.task_id,
                        severity='high' if 'critical' in (result.error_message or '').lower() else 'medium'
                    )
                
                self.logger.warning(f"Task {result.task_id} failed: {result.error_message}")
            
        except Exception as e:
            self.logger.error(f"Failed to handle task result: {str(e)}")
            
            # Record monitoring system error
            if hasattr(self, '_monitor') and self._monitor:
                self._monitor.record_error(
                    error_type="MonitoringError",
                    error_message=f"Failed to handle task result: {str(e)}",
                    severity='medium'
                )
    
    def start_concurrent_crawling(self, brands: List[str], additional_config: Optional[Dict[str, Any]] = None) -> CrawlResult:
        """
        Start concurrent crawling for specified brands with improved resource management.
        
        Args:
            brands: List of brand names to crawl
            additional_config: Optional additional configuration
            
        Returns:
            Crawling result with statistics
            
        Raises:
            CrawlerError: If crawling fails to start or complete
        """
        # Validate input
        if not brands:
            raise CrawlerError("No brands provided for crawling")
        
        with self._lock:
            if self._started:
                raise CrawlerError("Concurrent crawling is already running")
            
            self._started = True
            self._start_time = datetime.now()
        
        monitor = None
        execution_successful = False
        
        try:
            # Initialize monitoring if requested
            if additional_config and additional_config.get('enable_monitoring', True):
                monitor = self._initialize_monitoring(additional_config)
            
            self.logger.info(f"Starting concurrent crawling for {len(brands)} brands: {brands}")
            
            # Validate system is ready
            self._validate_system_ready()
            
            # Reset system state for new session
            self._reset_system_state()
            
            # Create and submit tasks
            tasks = self._create_tasks_for_brands(brands)
            self.logger.info(f"Created {len(tasks)} crawl tasks")
            
            if not tasks:
                raise CrawlerError("No valid tasks created from provided brands")
            
            # Submit tasks to scheduler
            self._task_scheduler.add_tasks(tasks)
            for _ in tasks:
                self._tasks_submitted.increment()
            
            # Create and start worker threads
            workers = self._thread_pool_manager.create_workers()
            if not workers:
                raise CrawlerError("Failed to create worker threads")
            
            self._thread_pool_manager.start_workers()
            self.logger.info(f"Started {len(workers)} worker threads")
            
            # Wait for completion with timeout
            self._wait_for_completion()
            
            # Mark as successful
            execution_successful = True
            
            # Generate and return final result
            return self._generate_final_result()
            
        except Exception as e:
            self.logger.error(f"Concurrent crawling failed: {str(e)}")
            
            # Record critical error in monitoring if available
            if monitor:
                try:
                    monitor.record_error(
                        error_type="SystemError",
                        error_message=f"Concurrent crawling failed: {str(e)}",
                        severity='critical'
                    )
                except Exception:
                    pass  # Ignore monitoring errors during failure
            
            # If execution failed, ensure we clean up properly
            if not execution_successful:
                try:
                    self._emergency_cleanup()
                except Exception as cleanup_error:
                    self.logger.error(f"Emergency cleanup failed: {str(cleanup_error)}")
            
            raise CrawlerError(f"Concurrent crawling failed: {str(e)}")
        
        finally:
            # Always stop monitoring and cleanup
            if monitor:
                try:
                    monitor.stop_monitoring()
                    self.logger.debug("Monitoring system stopped")
                except Exception as e:
                    self.logger.debug(f"Error stopping monitoring: {str(e)}")
            
            # Always perform cleanup, even if there were errors
            try:
                self._cleanup()
            except Exception as e:
                self.logger.error(f"Error during final cleanup: {str(e)}")
    
    def _initialize_monitoring(self, additional_config: Dict[str, Any]):
        """Initialize monitoring system with error handling."""
        try:
            from .monitoring import ConcurrentCrawlerMonitor, MonitoringConfig
            
            monitor_config = MonitoringConfig(
                update_interval=additional_config.get('monitor_update_interval', 5.0),
                enable_console_output=additional_config.get('monitor_console_output', True),
                console_update_interval=additional_config.get('monitor_console_interval', 10.0)
            )
            
            monitor = ConcurrentCrawlerMonitor(self, monitor_config)
            self._monitor = monitor
            monitor.start_monitoring()
            
            self.logger.info("Monitoring system started")
            return monitor
            
        except Exception as e:
            self.logger.warning(f"Failed to start monitoring: {str(e)}")
            return None
    
    def _emergency_cleanup(self) -> None:
        """Emergency cleanup when execution fails."""
        self.logger.warning("Performing emergency cleanup...")
        
        try:
            # Signal immediate shutdown
            self._shutdown_event.set()
            
            # Force stop workers with shorter timeout
            if self._thread_pool_manager:
                self._thread_pool_manager.shutdown_workers(timeout=10)
            
            # Clear task queue
            if self._task_scheduler:
                self._task_scheduler.shutdown()
            
            self.logger.info("Emergency cleanup completed")
            
        except Exception as e:
            self.logger.error(f"Emergency cleanup failed: {str(e)}")
            # Don't re-raise - this is best-effort cleanup
    
    def _validate_system_ready(self) -> None:
        """Validate that the system is ready for concurrent crawling."""
        validation_errors = []
        
        # Check core components
        if not self._task_scheduler:
            validation_errors.append("Task scheduler not initialized")
        
        if not self._thread_pool_manager:
            validation_errors.append("Thread pool manager not initialized")
        
        if not self._rate_controller:
            validation_errors.append("Rate controller not initialized")
        
        if not self._result_collector:
            validation_errors.append("Result collector not initialized")
        
        if not self._repository:
            validation_errors.append("Repository not initialized")
        
        # Check crawler registry
        if not self._crawler_registry:
            validation_errors.append("Crawler registry not initialized")
        
        available_crawlers = self._crawler_registry.list_crawlers()
        if not available_crawlers:
            validation_errors.append("No crawlers registered")
        
        # Check configuration
        try:
            self.config.validate()
        except Exception as e:
            validation_errors.append(f"Configuration validation failed: {str(e)}")
        
        if validation_errors:
            error_msg = f"System validation failed: {'; '.join(validation_errors)}"
            self.logger.error(error_msg)
            raise CrawlerError(error_msg)
        
        self.logger.debug("System validation passed")
    
    def get_system_health(self) -> Dict[str, Any]:
        """
        Get comprehensive system health status.
        
        Returns:
            Dictionary with system health information
        """
        try:
            health_status = {
                "overall_status": "healthy",
                "timestamp": datetime.now().isoformat(),
                "components": {},
                "issues": []
            }
            
            # Check each component
            components = {
                "task_scheduler": self._task_scheduler,
                "thread_pool_manager": self._thread_pool_manager,
                "rate_controller": self._rate_controller,
                "result_collector": self._result_collector,
                "repository": self._repository,
                "crawler_registry": self._crawler_registry
            }
            
            for name, component in components.items():
                if component is None:
                    health_status["components"][name] = "not_initialized"
                    health_status["issues"].append(f"{name} is not initialized")
                else:
                    health_status["components"][name] = "healthy"
            
            # Check worker health if thread pool is available
            if self._thread_pool_manager:
                try:
                    healthy_workers = self._thread_pool_manager.get_healthy_workers()
                    failed_workers = self._thread_pool_manager.get_failed_workers()
                    
                    health_status["workers"] = {
                        "healthy_count": len(healthy_workers),
                        "failed_count": len(failed_workers),
                        "healthy_workers": healthy_workers,
                        "failed_workers": failed_workers
                    }
                    
                    if failed_workers:
                        health_status["issues"].append(f"{len(failed_workers)} workers have failed")
                        
                except Exception as e:
                    health_status["issues"].append(f"Failed to check worker health: {str(e)}")
            
            # Check rate controller status
            if self._rate_controller:
                try:
                    rate_stats = self._rate_controller.get_statistics()
                    health_status["rate_controller"] = {
                        "current_rate": rate_stats.get("current_rate", 0),
                        "rate_limit": rate_stats.get("requests_per_second_limit", 0),
                        "active_requests": rate_stats.get("active_requests", 0),
                        "rate_limit_errors": rate_stats.get("rate_limit_errors", 0)
                    }
                    
                    if rate_stats.get("rate_limit_errors", 0) > 10:
                        health_status["issues"].append("High number of rate limit errors")
                        
                except Exception as e:
                    health_status["issues"].append(f"Failed to check rate controller: {str(e)}")
            
            # Determine overall status
            if health_status["issues"]:
                if any("not_initialized" in issue for issue in health_status["issues"]):
                    health_status["overall_status"] = "unhealthy"
                else:
                    health_status["overall_status"] = "degraded"
            
            return health_status
            
        except Exception as e:
            return {
                "overall_status": "error",
                "timestamp": datetime.now().isoformat(),
                "error": str(e),
                "components": {},
                "issues": [f"Health check failed: {str(e)}"]
            }
    
    def _reset_system_state(self) -> None:
        """Reset system state for new crawling session."""
        # Reset counters
        self._tasks_submitted.reset()
        self._tasks_completed.reset()
        self._tasks_failed.reset()
        
        # Clear result collector
        self._result_collector.clear()
        
        # Reset shutdown event
        self._shutdown_event.clear()
        
        # Clear crawler instances to get fresh instances
        for crawler in self._crawler_instances.values():
            crawler.cleanup()
        self._crawler_instances.clear()
        
        self.logger.debug("System state reset for new crawling session")
    
    def _create_tasks_for_brands(self, brands: List[str]) -> List[CrawlTask]:
        """
        Create crawl tasks for each brand.
        
        Args:
            brands: List of brand names
            
        Returns:
            List of crawl tasks
        """
        tasks = []
        for i, brand in enumerate(brands):
            task = CrawlTask(
                task_id=f"brand_{brand}_{i}_{int(time.time())}",
                brand=brand,
                estimated_complexity=self._estimate_brand_complexity(brand),
                priority=0
            )
            tasks.append(task)
        
        return tasks
    
    def _estimate_brand_complexity(self, brand: str) -> int:
        """
        Estimate crawling complexity for a brand.
        
        Args:
            brand: Brand name
            
        Returns:
            Complexity score (1-5)
        """
        # Simple heuristic based on brand characteristics
        complex_brands = {'samsung', 'corsair', 'gskill', 'kingston'}
        
        if brand.lower() in complex_brands:
            return 3
        elif len(brand) > 8:
            return 2
        else:
            return 1
    
    def _wait_for_completion(self) -> None:
        """Wait for all tasks to complete with improved timeout handling."""
        self.logger.info("Waiting for task completion...")
        
        max_wait_time = 600  # 10 minutes maximum wait time
        check_interval = 2.0  # Check every 2 seconds
        start_wait = time.time()
        last_progress_log = start_wait
        
        while (time.time() - start_wait) < max_wait_time:
            # Check if all tasks are done
            total_processed = self._tasks_completed.get_value() + self._tasks_failed.get_value()
            total_submitted = self._tasks_submitted.get_value()
            
            # Log progress periodically
            current_time = time.time()
            if current_time - last_progress_log >= 30.0:  # Every 30 seconds
                progress_pct = (total_processed / total_submitted * 100.0) if total_submitted > 0 else 0.0
                self.logger.info(f"Progress: {total_processed}/{total_submitted} tasks ({progress_pct:.1f}%)")
                last_progress_log = current_time
            
            # Check completion conditions
            if total_processed >= total_submitted and self._task_scheduler.is_empty():
                self.logger.info("All tasks completed successfully")
                break
            
            # Check if workers are still active
            if not self._thread_pool_manager.is_running():
                self.logger.warning("No active workers detected, checking for remaining tasks...")
                
                # Give a grace period for workers to restart or tasks to be reassigned
                time.sleep(5.0)
                
                # Check again
                if not self._thread_pool_manager.is_running() and not self._task_scheduler.is_empty():
                    self.logger.error("Workers stopped but tasks remain - this may indicate a problem")
                    break
            
            # Check for system shutdown signal
            if self._shutdown_event.is_set():
                self.logger.info("Shutdown signal received, stopping wait")
                break
            
            time.sleep(check_interval)
        
        # Check if we timed out
        elapsed_time = time.time() - start_wait
        if elapsed_time >= max_wait_time:
            self.logger.warning(f"Wait for completion timed out after {elapsed_time:.1f}s")
        
        # Signal shutdown and wait for workers to finish
        self.logger.info("Signaling shutdown to all workers...")
        self._shutdown_event.set()
        
        # Give workers time to finish current tasks
        worker_shutdown_timeout = min(30, max_wait_time - elapsed_time) if elapsed_time < max_wait_time else 10
        self._thread_pool_manager.shutdown_workers(timeout=worker_shutdown_timeout)
        
        # Wait for repository batch completion
        if self._repository:
            try:
                self.logger.debug("Waiting for repository batch completion...")
                self._repository.wait_for_batch_completion(timeout=10.0)
                self.logger.debug("Repository batch completion finished")
            except Exception as e:
                self.logger.warning(f"Repository batch completion error: {str(e)}")
        
        self._end_time = datetime.now()
        self._completed = True
        
        # Final statistics
        final_completed = self._tasks_completed.get_value()
        final_failed = self._tasks_failed.get_value()
        final_total = self._tasks_submitted.get_value()
        
        self.logger.info(f"Task completion finished: {final_completed} completed, {final_failed} failed, {final_total} total")
    
    def _generate_final_result(self) -> CrawlResult:
        """
        Generate final crawl result.
        
        Returns:
            Final crawl result with all statistics
        """
        summary = self._result_collector.get_summary()
        execution_time = (self._end_time - self._start_time).total_seconds() if self._end_time and self._start_time else 0
        
        # Get worker statistics from thread pool
        worker_stats = {}
        if self._thread_pool_manager:
            pool_stats = self._thread_pool_manager.get_pool_stats()
            worker_stats = pool_stats
        
        return CrawlResult(
            total_tasks=summary["total_tasks"],
            completed_tasks=summary["successful_tasks"],
            failed_tasks=summary["failed_tasks"],
            total_products_found=summary["total_products_found"],
            total_products_saved=summary["total_products_saved"],
            execution_time=execution_time,
            worker_stats=worker_stats,
            started_at=self._start_time,
            completed_at=self._end_time,
            performance_metrics=summary
        )
    
    def _cleanup(self) -> None:
        """Clean up resources with improved error handling."""
        with self._lock:
            self._started = False
        
        cleanup_errors = []
        
        try:
            # 1. Stop monitoring first if running
            if hasattr(self, '_monitor') and self._monitor:
                try:
                    self._monitor.stop_monitoring()
                    self.logger.debug("Monitoring stopped during cleanup")
                except Exception as e:
                    cleanup_errors.append(f"Monitor cleanup: {str(e)}")
            
            # 2. Shutdown thread pool manager (this stops all workers)
            if self._thread_pool_manager:
                try:
                    self._thread_pool_manager.shutdown_workers(timeout=20)  # Increased timeout
                    self.logger.debug("Thread pool shutdown completed")
                except Exception as e:
                    cleanup_errors.append(f"Thread pool cleanup: {str(e)}")
            
            # 3. Shutdown task scheduler
            if self._task_scheduler:
                try:
                    self._task_scheduler.shutdown()
                    self.logger.debug("Task scheduler shutdown completed")
                except Exception as e:
                    cleanup_errors.append(f"Scheduler cleanup: {str(e)}")
            
            # 4. Cleanup crawler instances (these may have browser contexts)
            for crawler_id, crawler in list(self._crawler_instances.items()):
                try:
                    if hasattr(crawler, 'cleanup'):
                        crawler.cleanup()
                    self.logger.debug(f"Crawler {crawler_id} cleaned up")
                except Exception as e:
                    cleanup_errors.append(f"Crawler {crawler_id} cleanup: {str(e)}")
            self._crawler_instances.clear()
            
            # 5. Wait for repository batch completion and close
            if self._repository:
                try:
                    # Wait for any pending batch operations
                    if hasattr(self._repository, 'wait_for_batch_completion'):
                        self._repository.wait_for_batch_completion(timeout=5.0)
                    
                    # Close repository connections
                    if hasattr(self._repository, 'close'):
                        self._repository.close()
                    
                    self.logger.debug("Repository closed")
                except Exception as e:
                    cleanup_errors.append(f"Repository cleanup: {str(e)}")
            
            # 6. Reset component references
            self._thread_pool_manager = None
            self._task_scheduler = None
            self._rate_controller = None
            self._result_collector = None
            self._repository = None
            
            if cleanup_errors:
                self.logger.warning(f"Cleanup completed with {len(cleanup_errors)} errors: {'; '.join(cleanup_errors)}")
            else:
                self.logger.debug("Cleanup completed successfully")
            
        except Exception as e:
            self.logger.error(f"Critical error during cleanup: {str(e)}")
            # Don't re-raise - cleanup should be best-effort
    
    def stop_crawling(self) -> None:
        """Stop concurrent crawling gracefully."""
        self.logger.info("Stopping concurrent crawling...")
        self._shutdown_event.set()
        
        # Stop thread pool
        if self._thread_pool_manager:
            self._thread_pool_manager.shutdown_workers(timeout=15)
        
        # Stop monitoring if running
        if hasattr(self, '_monitor') and self._monitor:
            try:
                self._monitor.stop_monitoring()
            except Exception as e:
                self.logger.debug(f"Error stopping monitoring: {str(e)}")
        
        self.logger.info("Concurrent crawling stopped")
    
    def shutdown(self) -> None:
        """
        Shutdown the entire concurrent crawler system.
        This is a more comprehensive shutdown than stop_crawling.
        """
        self.logger.info("Shutting down concurrent crawler system...")
        
        try:
            # Stop any running crawling first
            if self.is_running():
                self.logger.info("Stopping active crawling session...")
                self.stop_crawling()
                
                # Wait a moment for graceful stop
                import time
                time.sleep(1.0)
            
            # Perform comprehensive cleanup
            self._cleanup()
            
            self.logger.info("Concurrent crawler system shutdown complete")
            
        except Exception as e:
            self.logger.error(f"Error during system shutdown: {str(e)}")
            # Still attempt cleanup even if stop_crawling failed
            try:
                self._cleanup()
            except Exception as cleanup_error:
                self.logger.error(f"Cleanup also failed: {str(cleanup_error)}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with automatic cleanup."""
        try:
            self.shutdown()
        except Exception as e:
            self.logger.error(f"Error in context manager cleanup: {str(e)}")
        
        # Don't suppress exceptions
        return False
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get current crawling status.
        
        Returns:
            Dictionary with current status information
        """
        with self._lock:
            if not self._started:
                return {
                    "status": "not_started",
                    "message": "Crawling has not been started"
                }
            
            if self._completed:
                return {
                    "status": "completed",
                    "message": "Crawling has completed",
                    "execution_time": (self._end_time - self._start_time).total_seconds() if self._end_time else 0
                }
            
            # Calculate progress
            total_tasks = self._tasks_submitted.get_value()
            completed_tasks = self._tasks_completed.get_value()
            failed_tasks = self._tasks_failed.get_value()
            
            progress_percentage = 0.0
            if total_tasks > 0:
                progress_percentage = ((completed_tasks + failed_tasks) / total_tasks) * 100.0
            
            # Estimate remaining time
            elapsed_time = (datetime.now() - self._start_time).total_seconds() if self._start_time else 0
            estimated_remaining = 0.0
            if progress_percentage > 0:
                estimated_total_time = elapsed_time / (progress_percentage / 100.0)
                estimated_remaining = max(0, estimated_total_time - elapsed_time)
            
            # Get component status
            scheduler_status = self._task_scheduler.get_queue_status() if self._task_scheduler else {}
            pool_stats = self._thread_pool_manager.get_pool_stats() if self._thread_pool_manager else {}
            rate_stats = self._rate_controller.get_statistics() if self._rate_controller else {}
            
            return {
                "status": "running",
                "progress_percentage": progress_percentage,
                "total_tasks": total_tasks,
                "completed_tasks": completed_tasks,
                "failed_tasks": failed_tasks,
                "elapsed_time": elapsed_time,
                "estimated_remaining_time": estimated_remaining,
                "scheduler": scheduler_status,
                "thread_pool": pool_stats,
                "rate_controller": rate_stats
            }
    
    def get_performance_report(self) -> Dict[str, Any]:
        """
        Get detailed performance report.
        
        Returns:
            Dictionary with performance metrics
        """
        if not self._result_collector:
            return {"error": "No results available"}
        
        summary = self._result_collector.get_summary()
        worker_stats = self._result_collector.get_worker_stats()
        
        # Get component statistics
        scheduler_stats = self._task_scheduler.get_queue_status() if self._task_scheduler else {}
        pool_stats = self._thread_pool_manager.get_pool_stats() if self._thread_pool_manager else {}
        rate_stats = self._rate_controller.get_statistics() if self._rate_controller else {}
        repo_stats = self._repository.get_thread_safety_stats() if self._repository else {}
        
        return {
            "summary": summary,
            "worker_statistics": worker_stats,
            "configuration": {
                "max_workers": self.config.max_workers,
                "requests_per_second": self.config.requests_per_second,
                "max_concurrent_requests": self.config.max_concurrent_requests,
                "retry_attempts": self.config.retry_attempts,
                "timeout_seconds": self.config.timeout_seconds
            },
            "execution_details": {
                "start_time": self._start_time.isoformat() if self._start_time else None,
                "end_time": self._end_time.isoformat() if self._end_time else None,
                "total_execution_time": (self._end_time - self._start_time).total_seconds() if self._end_time and self._start_time else 0
            },
            "component_statistics": {
                "scheduler": scheduler_stats,
                "thread_pool": pool_stats,
                "rate_controller": rate_stats,
                "repository": repo_stats
            }
        }
    
    def is_running(self) -> bool:
        """
        Check if crawling is currently running.
        
        Returns:
            True if crawling is running
        """
        with self._lock:
            return self._started and not self._completed
    
    def get_worker_count(self) -> int:
        """
        Get number of active worker threads.
        
        Returns:
            Number of active workers
        """
        if self._thread_pool_manager:
            return self._thread_pool_manager.get_active_worker_count()
        return 0
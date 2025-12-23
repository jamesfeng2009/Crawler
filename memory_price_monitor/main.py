"""
Main application entry point for memory price monitoring system.
"""

import sys
import signal
import time
import argparse
import asyncio
from typing import Optional, Dict, Any
from pathlib import Path
from datetime import datetime, timedelta

from memory_price_monitor.utils.logging import get_logger
from memory_price_monitor.services.state_manager import StateManager
from memory_price_monitor.services.scheduler import TaskScheduler, ResourceLimits
from memory_price_monitor.services.notification import NotificationService
from memory_price_monitor.services.report_generator import WeeklyReportGenerator
from memory_price_monitor.crawlers import CrawlerRegistry
from memory_price_monitor.data.repository import PriceRepository
from memory_price_monitor.data.database import DatabaseManager
from memory_price_monitor.analysis.analyzer import PriceAnalyzer
from memory_price_monitor.visualization.chart_generator import ChartGenerator
from memory_price_monitor.utils.errors import StateManagementError, SchedulerError
from config import ConfigManager, SystemConfig, get_config


logger = get_logger(__name__)

# Optional imports for services that may have additional dependencies
try:
    from memory_price_monitor.services.health_check import HealthCheckService
    HEALTH_CHECK_AVAILABLE = True
except ImportError:
    logger.warning("Health check service not available (missing dependencies)")
    HealthCheckService = None
    HEALTH_CHECK_AVAILABLE = False

try:
    from memory_price_monitor.services.monitoring import MonitoringService
    MONITORING_AVAILABLE = True
except ImportError:
    logger.warning("Monitoring service not available (missing dependencies)")
    MonitoringService = None
    MONITORING_AVAILABLE = False


class MemoryPriceMonitorApp:
    """Main application class for memory price monitoring with dependency injection."""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the application with dependency injection.
        
        Args:
            config_path: Optional path to configuration file
        """
        self.config_path = config_path
        self.config_manager: Optional[ConfigManager] = None
        self.config: Optional[SystemConfig] = None
        
        # Core components
        self.state_manager: Optional[StateManager] = None
        self.db_manager: Optional[DatabaseManager] = None
        self.repository: Optional[PriceRepository] = None
        self.crawler_registry: Optional[CrawlerRegistry] = None
        
        # Services
        self.scheduler: Optional[TaskScheduler] = None
        self.notification_service: Optional[NotificationService] = None
        self.report_generator: Optional[WeeklyReportGenerator] = None
        self.health_check_service: Optional[HealthCheckService] = None
        self.monitoring_service: Optional[MonitoringService] = None
        
        # Analysis and visualization
        self.price_analyzer: Optional[PriceAnalyzer] = None
        self.chart_generator: Optional[ChartGenerator] = None
        
        # Application state
        self._shutdown_requested = False
        self._initialized = False
        
        # Register signal handlers
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
    
    def _signal_handler(self, signum: int, frame) -> None:
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, initiating graceful shutdown")
        self._shutdown_requested = True
        
        if self.scheduler:
            self.scheduler.request_graceful_shutdown()
    
    def initialize(self, minimal: bool = False) -> None:
        """
        Initialize all application components with dependency injection.
        
        Args:
            minimal: If True, initialize only components needed for basic operations
        """
        try:
            logger.info("Initializing Memory Price Monitor application")
            
            # Initialize configuration manager
            self._initialize_config()
            
            # Initialize crawler registry (always needed)
            self.crawler_registry = CrawlerRegistry()
            self._minimal_init = minimal
            self._register_crawlers()
            
            if not minimal:
                # Initialize core components
                self._initialize_core_components()
                
                # Initialize services
                self._initialize_services()
                
                # Initialize analysis and visualization components
                self._initialize_analysis_components()
                
                # Wire components together
                self._wire_components()
            
            self._initialized = True
            logger.info("Application components initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize application: {e}")
            raise
    
    def _initialize_config(self) -> None:
        """Initialize configuration management."""
        if self.config_path:
            self.config_manager = ConfigManager(self.config_path)
        else:
            self.config_manager = ConfigManager()
        
        self.config = self.config_manager.load_config()
        
        # Start configuration monitoring
        self.config_manager.start_monitoring()
        
        logger.info(f"Configuration loaded from {self.config_manager.config_path}")
    
    def _initialize_core_components(self) -> None:
        """Initialize core application components."""
        # Initialize state manager
        self.state_manager = StateManager("data/system_state.json")
        
        # Initialize database manager
        from memory_price_monitor.data.database_factory import DatabaseFactory
        self.db_manager = DatabaseFactory.create_database_manager(self.config.database)
        self.db_manager.initialize()
        
        # Initialize repository
        self.repository = PriceRepository(self.db_manager)
        
        logger.info("Core components initialized")
    
    def _initialize_services(self) -> None:
        """Initialize application services."""
        # Initialize notification service
        self.notification_service = NotificationService(
            config=self.config.notification
        )
        
        # Initialize health check service (optional)
        if HEALTH_CHECK_AVAILABLE:
            self.health_check_service = HealthCheckService(
                db_manager=self.db_manager,
                crawler_registry=self.crawler_registry,
                state_manager=self.state_manager
            )
        else:
            logger.info("Health check service disabled (dependencies not available)")
        
        # Initialize monitoring service (optional)
        if MONITORING_AVAILABLE and self.health_check_service:
            self.monitoring_service = MonitoringService(
                repository=self.repository,
                notification_service=self.notification_service,
                health_check_service=self.health_check_service
            )
        else:
            logger.info("Monitoring service disabled (dependencies not available)")
        
        logger.info("Services initialized")
    
    def _initialize_analysis_components(self) -> None:
        """Initialize analysis and visualization components."""
        # Initialize price analyzer
        self.price_analyzer = PriceAnalyzer()
        
        # Initialize chart generator
        self.chart_generator = ChartGenerator()
        
        # Initialize report generator
        self.report_generator = WeeklyReportGenerator(
            repository=self.repository,
            analyzer=self.price_analyzer,
            chart_generator=self.chart_generator
        )
        
        logger.info("Analysis components initialized")
    
    def _wire_components(self) -> None:
        """Wire components together and initialize scheduler."""
        # Initialize scheduler with all dependencies
        resource_limits = ResourceLimits(
            max_concurrent_tasks=self.config.crawler.concurrent_limit,
            max_memory_usage_percent=85.0,
            max_cpu_usage_percent=85.0,
            min_disk_space_gb=1.0
        )
        
        self.scheduler = TaskScheduler(
            crawler_registry=self.crawler_registry,
            repository=self.repository,
            resource_limits=resource_limits,
            state_manager=self.state_manager
        )
        
        # Add configuration change listener to scheduler
        if self.config_manager:
            self.config_manager.add_change_listener(self.scheduler)
        
        logger.info("Components wired together successfully")
    
    def _register_crawlers(self) -> None:
        """Register available crawlers."""
        try:
            if hasattr(self, '_minimal_init') and self._minimal_init:
                # For minimal initialization, just register the names
                # The actual classes will be imported when needed
                self.crawler_registry._crawlers = {'jd': None, 'zol': None}
                logger.info("Registered crawler names: jd, zol (minimal mode)")
            else:
                # Import and register crawlers
                from memory_price_monitor.crawlers.jd_crawler import JDCrawler
                from memory_price_monitor.crawlers.zol_crawler import ZOLCrawler
                
                self.crawler_registry.register('jd', JDCrawler)
                self.crawler_registry.register('zol', ZOLCrawler)
                
                logger.info("Registered crawlers: jd, zol")
            
        except ImportError as e:
            logger.warning(f"Some crawlers could not be imported: {e}")
        except Exception as e:
            logger.error(f"Failed to register crawlers: {e}")
            # Don't raise here for minimal initialization
            if not hasattr(self, '_minimal_init') or not self._minimal_init:
                raise
    
    def start(self) -> None:
        """Start the application."""
        if not self._initialized:
            raise RuntimeError("Application must be initialized before starting")
        
        try:
            logger.info("Starting Memory Price Monitor application")
            
            # Check if we're recovering from a previous session
            if self.state_manager.is_recovery_mode():
                logger.info("Detected previous session state, entering recovery mode")
                self._handle_recovery()
            
            # Start monitoring service
            if self.monitoring_service:
                self.monitoring_service.start()
            
            # Start health check service
            if self.health_check_service:
                self.health_check_service.start()
            
            # Start scheduler
            self.scheduler.start()
            
            # Schedule tasks based on configuration
            self._schedule_tasks()
            
            logger.info("Application started successfully")
            
        except Exception as e:
            logger.error(f"Failed to start application: {e}")
            raise
    
    def _schedule_tasks(self) -> None:
        """Schedule recurring tasks based on configuration."""
        # Schedule daily crawls
        self.scheduler.schedule_daily_crawl(
            hour=self.config.scheduler.daily_crawl_hour,
            minute=0
        )
        
        # Schedule weekly reports
        self.scheduler.schedule_weekly_report(
            day_of_week=self.config.scheduler.weekly_report_day,
            hour=self.config.scheduler.weekly_report_hour,
            minute=0
        )
        
        logger.info(f"Scheduled daily crawl at {self.config.scheduler.daily_crawl_hour}:00")
        logger.info(f"Scheduled weekly report on day {self.config.scheduler.weekly_report_day} at {self.config.scheduler.weekly_report_hour}:00")
    
    def _handle_recovery(self) -> None:
        """Handle recovery from previous session."""
        try:
            system_state = self.state_manager.get_system_state()
            
            logger.info(f"Recovering from session started at: {system_state.started_at}")
            logger.info(f"Previous uptime: {system_state.uptime_seconds / 3600:.1f} hours")
            
            # Log crawler states
            for name, crawler_state in system_state.crawlers.items():
                if crawler_state.last_successful_crawl:
                    hours_ago = (
                        system_state.last_updated - crawler_state.last_successful_crawl
                    ).total_seconds() / 3600
                    
                    logger.info(f"Crawler {name}: last success {hours_ago:.1f} hours ago, "
                              f"failures: {crawler_state.consecutive_failures}")
                else:
                    logger.info(f"Crawler {name}: no previous successful crawls")
            
            # Check for stale crawlers that might need immediate attention
            stale_crawlers = []
            for name, crawler_state in system_state.crawlers.items():
                if crawler_state.last_successful_crawl:
                    hours_since_success = (
                        system_state.last_updated - crawler_state.last_successful_crawl
                    ).total_seconds() / 3600
                    
                    if hours_since_success > 25:  # More than 25 hours
                        stale_crawlers.append(name)
            
            if stale_crawlers:
                logger.warning(f"Found stale crawlers that may need attention: {stale_crawlers}")
                # Could trigger immediate crawl for stale crawlers
                # self.scheduler.execute_crawl_task(stale_crawlers)
            
        except Exception as e:
            logger.error(f"Error during recovery handling: {e}")
            # Continue startup even if recovery handling fails
    
    def run(self) -> None:
        """Run the application main loop."""
        try:
            logger.info("Entering main application loop")
            
            while not self._shutdown_requested:
                # Check for graceful shutdown request
                if self.scheduler and self.scheduler.is_graceful_shutdown_requested():
                    logger.info("Graceful shutdown requested by scheduler")
                    break
                
                # Perform periodic maintenance
                self._perform_maintenance()
                
                # Sleep for a short interval
                time.sleep(10)
            
            logger.info("Exiting main application loop")
            
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        except Exception as e:
            logger.error(f"Error in main application loop: {e}")
            raise
    
    def _perform_maintenance(self) -> None:
        """Perform periodic maintenance tasks."""
        try:
            # Clean up old execution metrics
            if self.scheduler:
                cleaned_metrics = self.scheduler.cleanup_old_metrics(max_age_hours=24)
                if cleaned_metrics > 0:
                    logger.debug(f"Cleaned up {cleaned_metrics} old execution metrics")
            
            # Clean up old crawler states
            if self.state_manager:
                cleaned_states = self.state_manager.cleanup_old_crawler_states(max_age_days=30)
                if cleaned_states > 0:
                    logger.info(f"Cleaned up {cleaned_states} old crawler states")
            
            # Perform health checks
            if self.health_check_service:
                health_status = self.health_check_service.get_overall_health()
                if not health_status.is_healthy:
                    logger.warning(f"Health check failed: {health_status.issues}")
            
        except Exception as e:
            logger.error(f"Error during maintenance: {e}")
    
    def stop(self) -> None:
        """Stop the application gracefully."""
        try:
            logger.info("Stopping Memory Price Monitor application")
            
            # Stop configuration monitoring
            if self.config_manager:
                self.config_manager.stop_monitoring()
            
            # Stop services
            if self.monitoring_service:
                self.monitoring_service.stop()
            
            if self.health_check_service:
                self.health_check_service.stop()
            
            # Stop scheduler
            if self.scheduler:
                self.scheduler.stop(wait=True)
            
            # Stop state manager auto-save
            if self.state_manager:
                self.state_manager.stop_auto_save()
            
            # Close database connections
            if self.db_manager:
                self.db_manager.close()
            
            logger.info("Application stopped successfully")
            
        except Exception as e:
            logger.error(f"Error stopping application: {e}")
            raise
    
    def get_status(self) -> Dict[str, Any]:
        """Get current application status."""
        try:
            status = {
                'application': 'Memory Price Monitor',
                'initialized': self._initialized,
                'shutdown_requested': self._shutdown_requested,
                'timestamp': datetime.now().isoformat()
            }
            
            if self.scheduler:
                status['scheduler'] = self.scheduler.get_scheduler_status()
            
            if self.state_manager:
                status['state'] = self.state_manager.get_health_summary()
            
            if self.health_check_service:
                status['health'] = self.health_check_service.get_overall_health().__dict__
            
            if self.monitoring_service:
                status['monitoring'] = self.monitoring_service.get_status()
            
            return status
            
        except Exception as e:
            logger.error(f"Error getting application status: {e}")
            return {'error': str(e)}
    
    # Manual operation methods for CLI
    def manual_crawl(self, crawler_names: Optional[list] = None) -> Dict[str, Any]:
        """Execute manual crawl operation."""
        if not self._initialized:
            raise RuntimeError("Application must be initialized before manual operations")
        
        try:
            logger.info(f"Starting manual crawl for crawlers: {crawler_names or 'all'}")
            
            if crawler_names:
                # Crawl specific crawlers
                results = {}
                for name in crawler_names:
                    if name in self.crawler_registry.list_crawlers():
                        result = self.scheduler.execute_crawl_task([name])
                        results[name] = result
                    else:
                        results[name] = {'error': f'Crawler {name} not found'}
                return results
            else:
                # Crawl all registered crawlers
                return self.scheduler.execute_crawl_task()
                
        except Exception as e:
            logger.error(f"Manual crawl failed: {e}")
            return {'error': str(e)}
    
    def manual_report(self) -> Dict[str, Any]:
        """Generate and send manual report."""
        if not self._initialized:
            raise RuntimeError("Application must be initialized before manual operations")
        
        try:
            logger.info("Generating manual report")
            return self.scheduler.execute_report_task()
            
        except Exception as e:
            logger.error(f"Manual report generation failed: {e}")
            return {'error': str(e)}
    
    def list_crawlers(self) -> list:
        """List available crawlers."""
        if self.crawler_registry:
            if hasattr(self, '_minimal_init') and self._minimal_init:
                # In minimal mode, return the keys directly
                return list(self.crawler_registry._crawlers.keys())
            else:
                return self.crawler_registry.list_crawlers()
        return []
    
    def get_crawler_status(self, crawler_name: str) -> Dict[str, Any]:
        """Get status of specific crawler."""
        if not self.state_manager:
            return {'error': 'State manager not initialized'}
        
        try:
            system_state = self.state_manager.get_system_state()
            if crawler_name in system_state.crawlers:
                crawler_state = system_state.crawlers[crawler_name]
                return {
                    'name': crawler_name,
                    'last_successful_crawl': crawler_state.last_successful_crawl.isoformat() if crawler_state.last_successful_crawl else None,
                    'consecutive_failures': crawler_state.consecutive_failures,
                    'total_crawls': crawler_state.total_crawls,
                    'total_products_found': crawler_state.total_products_found,
                    'is_enabled': crawler_state.is_enabled
                }
            else:
                return {'error': f'Crawler {crawler_name} not found in state'}
                
        except Exception as e:
            return {'error': str(e)}


def create_cli_parser() -> argparse.ArgumentParser:
    """Create command-line interface parser."""
    parser = argparse.ArgumentParser(
        description='Memory Price Monitor - Automated price tracking system',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                          # Run the application normally
  %(prog)s --config custom.json     # Use custom configuration file
  %(prog)s --crawl                  # Execute manual crawl
  %(prog)s --crawl --crawler jd     # Crawl specific source
  %(prog)s --report                 # Generate manual report
  %(prog)s --status                 # Show application status
  %(prog)s --list-crawlers          # List available crawlers
        """
    )
    
    # Configuration options
    parser.add_argument(
        '--config', '-c',
        type=str,
        help='Path to configuration file (default: config.json)'
    )
    
    # Operation modes
    operation_group = parser.add_mutually_exclusive_group()
    
    operation_group.add_argument(
        '--daemon', '-d',
        action='store_true',
        help='Run as daemon (default mode)'
    )
    
    operation_group.add_argument(
        '--crawl',
        action='store_true',
        help='Execute manual crawl and exit'
    )
    
    operation_group.add_argument(
        '--report',
        action='store_true',
        help='Generate manual report and exit'
    )
    
    operation_group.add_argument(
        '--status',
        action='store_true',
        help='Show application status and exit'
    )
    
    operation_group.add_argument(
        '--list-crawlers',
        action='store_true',
        help='List available crawlers and exit'
    )
    
    # Crawler-specific options
    parser.add_argument(
        '--crawler',
        type=str,
        action='append',
        help='Specify crawler(s) to use (can be used multiple times)'
    )
    
    # Output options
    parser.add_argument(
        '--output', '-o',
        type=str,
        choices=['json', 'text'],
        default='text',
        help='Output format for status and results (default: text)'
    )
    
    # Logging options
    parser.add_argument(
        '--log-level',
        type=str,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Override log level from configuration'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output (equivalent to --log-level DEBUG)'
    )
    
    # Development options
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Perform dry run without making actual changes'
    )
    
    return parser


def format_output(data: Any, format_type: str) -> str:
    """Format output data according to specified format."""
    if format_type == 'json':
        import json
        return json.dumps(data, indent=2, default=str, ensure_ascii=False)
    else:
        # Text format
        if isinstance(data, dict):
            lines = []
            for key, value in data.items():
                if isinstance(value, dict):
                    lines.append(f"{key}:")
                    for sub_key, sub_value in value.items():
                        lines.append(f"  {sub_key}: {sub_value}")
                elif isinstance(value, list):
                    lines.append(f"{key}: {', '.join(map(str, value))}")
                else:
                    lines.append(f"{key}: {value}")
            return '\n'.join(lines)
        elif isinstance(data, list):
            return '\n'.join(map(str, data))
        else:
            return str(data)


def handle_manual_operations(app: MemoryPriceMonitorApp, args: argparse.Namespace) -> int:
    """Handle manual operations and return exit code."""
    try:
        if args.list_crawlers:
            crawlers = app.list_crawlers()
            print(format_output(crawlers, args.output))
            return 0
        
        if args.status:
            status = app.get_status()
            print(format_output(status, args.output))
            return 0
        
        if args.crawl:
            result = app.manual_crawl(args.crawler)
            print(format_output(result, args.output))
            
            # Check if any crawl failed
            if isinstance(result, dict):
                for crawler_result in result.values():
                    if isinstance(crawler_result, dict) and 'error' in crawler_result:
                        return 1
            return 0
        
        if args.report:
            result = app.manual_report()
            print(format_output(result, args.output))
            
            # Check if report generation failed
            if isinstance(result, dict) and 'error' in result:
                return 1
            return 0
        
        return 0
        
    except Exception as e:
        logger.error(f"Manual operation failed: {e}")
        error_output = {'error': str(e)}
        print(format_output(error_output, args.output))
        return 1


def requires_full_initialization(args: argparse.Namespace) -> bool:
    """Check if the operation requires full application initialization."""
    # Simple operations that only need crawler registry
    simple_operations = [args.list_crawlers]
    
    # Operations that need database and other services
    complex_operations = [args.status, args.crawl, args.report]
    
    if any(simple_operations):
        return False
    elif any(complex_operations):
        return True
    else:
        # Default daemon mode needs full initialization
        return True


def main():
    """Main entry point with command-line interface."""
    parser = create_cli_parser()
    args = parser.parse_args()
    
    # Set up logging level
    if args.verbose:
        log_level = 'DEBUG'
    elif args.log_level:
        log_level = args.log_level
    else:
        log_level = None
    
    if log_level:
        import logging
        logging.getLogger().setLevel(getattr(logging, log_level))
    
    app = None
    exit_code = 0
    
    try:
        # Create and initialize application
        app = MemoryPriceMonitorApp(config_path=args.config)
        
        # Determine if we need full initialization
        needs_full_init = requires_full_initialization(args)
        app.initialize(minimal=not needs_full_init)
        
        # Handle manual operations
        if args.list_crawlers or args.status or args.crawl or args.report:
            exit_code = handle_manual_operations(app, args)
        else:
            # Default daemon mode
            logger.info("Starting Memory Price Monitor in daemon mode")
            
            # Start application
            app.start()
            
            # Run main loop
            app.run()
        
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
        exit_code = 0
    except (StateManagementError, SchedulerError) as e:
        logger.error(f"Application error: {e}")
        exit_code = 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        exit_code = 1
    finally:
        # Ensure cleanup
        if app:
            try:
                app.stop()
            except Exception as e:
                logger.error(f"Error during cleanup: {e}")
                exit_code = 1
    
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
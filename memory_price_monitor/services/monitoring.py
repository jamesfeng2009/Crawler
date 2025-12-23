"""
Monitoring and alerting system for the memory price monitor.

This module provides comprehensive monitoring capabilities including:
- Parsing failure detection and alerts
- Data integrity monitoring
- System health checks and performance metrics
- Administrator notification system
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
import threading
import queue
from collections import defaultdict, deque

from memory_price_monitor.data.models import StandardizedProduct, PriceRecord
from memory_price_monitor.crawlers.base import CrawlResult
from memory_price_monitor.utils.errors import MemoryPriceMonitorError, ValidationError
from memory_price_monitor.utils.logging import get_logger
from memory_price_monitor.services.notification import NotificationService


class AlertSeverity(Enum):
    """Alert severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class AlertType(Enum):
    """Types of alerts that can be generated."""
    PARSING_FAILURE = "parsing_failure"
    DATA_INTEGRITY = "data_integrity"
    SYSTEM_HEALTH = "system_health"
    PERFORMANCE = "performance"
    CRAWLER_BLOCKED = "crawler_blocked"
    DATABASE_ERROR = "database_error"


@dataclass
class Alert:
    """Represents a system alert."""
    id: str
    type: AlertType
    severity: AlertSeverity
    title: str
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    source: Optional[str] = None
    resolved: bool = False
    resolved_at: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert alert to dictionary for serialization."""
        return {
            'id': self.id,
            'type': self.type.value,
            'severity': self.severity.value,
            'title': self.title,
            'message': self.message,
            'details': self.details,
            'timestamp': self.timestamp.isoformat(),
            'source': self.source,
            'resolved': self.resolved,
            'resolved_at': self.resolved_at.isoformat() if self.resolved_at else None
        }


@dataclass
class SystemHealthMetrics:
    """System health and performance metrics."""
    timestamp: datetime = field(default_factory=datetime.now)
    cpu_usage_percent: float = 0.0
    memory_usage_percent: float = 0.0
    disk_usage_percent: float = 0.0
    active_crawlers: int = 0
    database_connections: int = 0
    last_successful_crawl: Optional[datetime] = None
    total_products_crawled_today: int = 0
    total_errors_today: int = 0
    average_response_time_ms: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary."""
        return {
            'timestamp': self.timestamp.isoformat(),
            'cpu_usage_percent': self.cpu_usage_percent,
            'memory_usage_percent': self.memory_usage_percent,
            'disk_usage_percent': self.disk_usage_percent,
            'active_crawlers': self.active_crawlers,
            'database_connections': self.database_connections,
            'last_successful_crawl': self.last_successful_crawl.isoformat() if self.last_successful_crawl else None,
            'total_products_crawled_today': self.total_products_crawled_today,
            'total_errors_today': self.total_errors_today,
            'average_response_time_ms': self.average_response_time_ms
        }


class DataIntegrityMonitor:
    """Monitors data integrity and flags suspicious data points."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize data integrity monitor."""
        self.config = config or {}
        self.logger = get_logger(__name__)
        self.flagged_data_points: List[Dict[str, Any]] = []
        self.integrity_checks_performed: List[Dict[str, Any]] = []
        
        # Configuration parameters
        self.price_anomaly_threshold = self.config.get('price_anomaly_threshold', 3.0)
        self.temporal_inconsistency_threshold = self.config.get('temporal_inconsistency_threshold', 10.0)
        self.max_flagged_points = self.config.get('max_flagged_points', 1000)
    
    def check_price_anomalies(self, products: List[StandardizedProduct]) -> List[Dict[str, Any]]:
        """
        Check for price anomalies that might indicate data integrity issues.
        
        Args:
            products: List of products to check
            
        Returns:
            List of detected anomalies
        """
        anomalies = []
        
        if not products:
            return anomalies
        
        try:
            # Calculate price statistics by category
            price_groups = defaultdict(list)
            for product in products:
                key = (product.brand, product.capacity, product.type)
                price_groups[key].append(float(product.current_price))
            
            # Check for anomalies within each group
            for (brand, capacity, mem_type), prices in price_groups.items():
                if len(prices) < 2:
                    continue
                    
                mean_price = sum(prices) / len(prices)
                
                # Find products with anomalous prices
                for product in products:
                    if (product.brand, product.capacity, product.type) == (brand, capacity, mem_type):
                        price = float(product.current_price)
                        deviation_factor = price / mean_price if mean_price > 0 else float('inf')
                        
                        if (deviation_factor >= self.price_anomaly_threshold or 
                            deviation_factor <= 1.0 / self.price_anomaly_threshold):
                            
                            anomaly = {
                                'product_id': product.product_id,
                                'source': product.source,
                                'brand': product.brand,
                                'capacity': product.capacity,
                                'type': product.type,
                                'price': price,
                                'mean_price': mean_price,
                                'deviation_factor': deviation_factor,
                                'type': 'price_anomaly',
                                'timestamp': product.timestamp,
                                'severity': 'high' if deviation_factor >= 5.0 or deviation_factor <= 0.2 else 'medium'
                            }
                            anomalies.append(anomaly)
                            self._add_flagged_point(anomaly)
            
            self._record_integrity_check('price_anomalies', len(products), len(anomalies))
            
        except Exception as e:
            self.logger.error(f"Error checking price anomalies: {str(e)}")
        
        return anomalies
    
    def check_duplicate_data(self, products: List[StandardizedProduct]) -> List[Dict[str, Any]]:
        """
        Check for duplicate data entries.
        
        Args:
            products: List of products to check
            
        Returns:
            List of detected duplicates
        """
        duplicates = []
        seen_products = {}
        
        try:
            for product in products:
                # Create key based on source, product_id, and date
                key = (product.source, product.product_id, product.timestamp.date())
                
                if key in seen_products:
                    duplicate = {
                        'product_id': product.product_id,
                        'source': product.source,
                        'original_timestamp': seen_products[key].timestamp,
                        'duplicate_timestamp': product.timestamp,
                        'type': 'duplicate_data',
                        'severity': 'medium'
                    }
                    duplicates.append(duplicate)
                    self._add_flagged_point(duplicate)
                else:
                    seen_products[key] = product
            
            self._record_integrity_check('duplicate_data', len(products), len(duplicates))
            
        except Exception as e:
            self.logger.error(f"Error checking duplicate data: {str(e)}")
        
        return duplicates
    
    def check_missing_required_fields(self, products: List[StandardizedProduct]) -> List[Dict[str, Any]]:
        """
        Check for products with missing required fields.
        
        Args:
            products: List of products to check
            
        Returns:
            List of products with missing fields
        """
        missing_fields = []
        
        try:
            for product in products:
                issues = []
                
                if not product.brand or product.brand.strip() == '':
                    issues.append('brand')
                if not product.model or product.model.strip() == '':
                    issues.append('model')
                if not product.capacity or product.capacity.strip() == '':
                    issues.append('capacity')
                if not product.type or product.type.strip() == '':
                    issues.append('type')
                if product.current_price <= 0:
                    issues.append('current_price')
                if not product.url or product.url.strip() == '':
                    issues.append('url')
                
                if issues:
                    missing_field_issue = {
                        'product_id': product.product_id,
                        'source': product.source,
                        'missing_fields': issues,
                        'type': 'missing_fields',
                        'timestamp': product.timestamp,
                        'severity': 'high' if len(issues) > 2 else 'medium'
                    }
                    missing_fields.append(missing_field_issue)
                    self._add_flagged_point(missing_field_issue)
            
            self._record_integrity_check('missing_fields', len(products), len(missing_fields))
            
        except Exception as e:
            self.logger.error(f"Error checking missing fields: {str(e)}")
        
        return missing_fields
    
    def check_temporal_consistency(self, price_records: List[PriceRecord]) -> List[Dict[str, Any]]:
        """
        Check for temporal inconsistencies in price data.
        
        Args:
            price_records: List of price records to check
            
        Returns:
            List of detected inconsistencies
        """
        inconsistencies = []
        
        try:
            # Group by product_id
            product_records = defaultdict(list)
            for record in price_records:
                product_records[record.product_id].append(record)
            
            # Check each product's price history
            for product_id, records in product_records.items():
                if len(records) < 2:
                    continue
                
                # Sort by timestamp
                sorted_records = sorted(records, key=lambda r: r.recorded_at)
                
                for i in range(1, len(sorted_records)):
                    prev_record = sorted_records[i-1]
                    curr_record = sorted_records[i]
                    
                    # Check for impossible price jumps in short time
                    time_diff = (curr_record.recorded_at - prev_record.recorded_at).total_seconds()
                    if time_diff < 3600:  # Less than 1 hour
                        price_ratio = float(curr_record.current_price) / float(prev_record.current_price)
                        
                        if price_ratio >= self.temporal_inconsistency_threshold or price_ratio <= 1.0 / self.temporal_inconsistency_threshold:
                            inconsistency = {
                                'product_id': product_id,
                                'prev_price': float(prev_record.current_price),
                                'curr_price': float(curr_record.current_price),
                                'time_diff_seconds': time_diff,
                                'price_ratio': price_ratio,
                                'type': 'temporal_inconsistency',
                                'timestamp': curr_record.recorded_at,
                                'severity': 'high'
                            }
                            inconsistencies.append(inconsistency)
                            self._add_flagged_point(inconsistency)
            
            self._record_integrity_check('temporal_consistency', len(price_records), len(inconsistencies))
            
        except Exception as e:
            self.logger.error(f"Error checking temporal consistency: {str(e)}")
        
        return inconsistencies
    
    def get_flagged_data_points(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get flagged data points."""
        if limit:
            return self.flagged_data_points[-limit:]
        return self.flagged_data_points.copy()
    
    def get_integrity_check_summary(self) -> Dict[str, Any]:
        """Get summary of integrity checks performed."""
        if not self.integrity_checks_performed:
            return {
                'total_checks': 0,
                'total_flagged_points': 0,
                'checks_by_type': {},
                'last_check': None
            }
        
        checks_by_type = defaultdict(int)
        for check in self.integrity_checks_performed:
            checks_by_type[check['check_type']] += 1
        
        return {
            'total_checks': len(self.integrity_checks_performed),
            'total_flagged_points': len(self.flagged_data_points),
            'checks_by_type': dict(checks_by_type),
            'last_check': max(self.integrity_checks_performed, 
                            key=lambda x: x['timestamp'])['timestamp']
        }
    
    def _add_flagged_point(self, point: Dict[str, Any]) -> None:
        """Add a flagged data point, maintaining size limit."""
        self.flagged_data_points.append(point)
        
        # Maintain size limit
        if len(self.flagged_data_points) > self.max_flagged_points:
            self.flagged_data_points = self.flagged_data_points[-self.max_flagged_points:]
    
    def _record_integrity_check(self, check_type: str, items_checked: int, issues_found: int) -> None:
        """Record an integrity check."""
        check_record = {
            'check_type': check_type,
            'items_checked': items_checked,
            'issues_found': issues_found,
            'timestamp': datetime.now()
        }
        self.integrity_checks_performed.append(check_record)
        
        # Log the check
        self.logger.info(f"Integrity check completed: {check_type}, "
                        f"checked={items_checked}, issues={issues_found}")


class SystemHealthMonitor:
    """Monitors system health and performance metrics."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize system health monitor."""
        self.config = config or {}
        self.logger = get_logger(__name__)
        self.metrics_history: deque = deque(maxlen=self.config.get('max_metrics_history', 1000))
        self.response_times: deque = deque(maxlen=100)  # Last 100 response times
        
    def collect_system_metrics(self) -> SystemHealthMetrics:
        """
        Collect current system health metrics.
        
        Returns:
            Current system metrics
        """
        metrics = SystemHealthMetrics()
        
        try:
            # Try to collect system metrics using psutil if available
            try:
                import psutil
                metrics.cpu_usage_percent = psutil.cpu_percent(interval=1)
                metrics.memory_usage_percent = psutil.virtual_memory().percent
                metrics.disk_usage_percent = psutil.disk_usage('/').percent
            except ImportError:
                self.logger.warning("psutil not available, using mock system metrics")
                # Use mock values if psutil is not available
                metrics.cpu_usage_percent = 25.0
                metrics.memory_usage_percent = 45.0
                metrics.disk_usage_percent = 60.0
            
            # Calculate average response time
            if self.response_times:
                metrics.average_response_time_ms = sum(self.response_times) / len(self.response_times)
            
            # Store metrics in history
            self.metrics_history.append(metrics)
            
            self.logger.debug(f"System metrics collected: CPU={metrics.cpu_usage_percent}%, "
                            f"Memory={metrics.memory_usage_percent}%, "
                            f"Disk={metrics.disk_usage_percent}%")
            
        except Exception as e:
            self.logger.error(f"Error collecting system metrics: {str(e)}")
        
        return metrics
    
    def record_response_time(self, response_time_ms: float) -> None:
        """Record a response time measurement."""
        self.response_times.append(response_time_ms)
    
    def get_metrics_history(self, hours: int = 24) -> List[SystemHealthMetrics]:
        """Get metrics history for the specified number of hours."""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        return [m for m in self.metrics_history if m.timestamp >= cutoff_time]
    
    def check_system_health(self) -> List[Alert]:
        """
        Check system health and generate alerts if needed.
        
        Returns:
            List of health-related alerts
        """
        alerts = []
        
        try:
            metrics = self.collect_system_metrics()
            
            # Check CPU usage
            if metrics.cpu_usage_percent > 90:
                alerts.append(Alert(
                    id=f"cpu_high_{int(time.time())}",
                    type=AlertType.SYSTEM_HEALTH,
                    severity=AlertSeverity.HIGH,
                    title="High CPU Usage",
                    message=f"CPU usage is {metrics.cpu_usage_percent:.1f}%",
                    details={'cpu_usage': metrics.cpu_usage_percent}
                ))
            
            # Check memory usage
            if metrics.memory_usage_percent > 85:
                alerts.append(Alert(
                    id=f"memory_high_{int(time.time())}",
                    type=AlertType.SYSTEM_HEALTH,
                    severity=AlertSeverity.HIGH,
                    title="High Memory Usage",
                    message=f"Memory usage is {metrics.memory_usage_percent:.1f}%",
                    details={'memory_usage': metrics.memory_usage_percent}
                ))
            
            # Check disk usage
            if metrics.disk_usage_percent > 90:
                alerts.append(Alert(
                    id=f"disk_high_{int(time.time())}",
                    type=AlertType.SYSTEM_HEALTH,
                    severity=AlertSeverity.CRITICAL,
                    title="High Disk Usage",
                    message=f"Disk usage is {metrics.disk_usage_percent:.1f}%",
                    details={'disk_usage': metrics.disk_usage_percent}
                ))
            
            # Check response times
            if metrics.average_response_time_ms > 5000:  # 5 seconds
                alerts.append(Alert(
                    id=f"response_slow_{int(time.time())}",
                    type=AlertType.PERFORMANCE,
                    severity=AlertSeverity.MEDIUM,
                    title="Slow Response Times",
                    message=f"Average response time is {metrics.average_response_time_ms:.0f}ms",
                    details={'avg_response_time_ms': metrics.average_response_time_ms}
                ))
            
        except Exception as e:
            self.logger.error(f"Error checking system health: {str(e)}")
            alerts.append(Alert(
                id=f"health_check_error_{int(time.time())}",
                type=AlertType.SYSTEM_HEALTH,
                severity=AlertSeverity.HIGH,
                title="Health Check Error",
                message=f"Failed to check system health: {str(e)}",
                details={'error': str(e)}
            ))
        
        return alerts


class MonitoringService:
    """
    Main monitoring and alerting service.
    
    Coordinates all monitoring activities and manages alerts.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None, 
                 notification_service: Optional[NotificationService] = None):
        """
        Initialize monitoring service.
        
        Args:
            config: Configuration dictionary
            notification_service: Service for sending notifications
        """
        self.config = config or {}
        self.logger = get_logger(__name__)
        self.notification_service = notification_service
        
        # Initialize sub-monitors
        self.data_integrity_monitor = DataIntegrityMonitor(self.config.get('data_integrity', {}))
        self.system_health_monitor = SystemHealthMonitor(self.config.get('system_health', {}))
        
        # Alert management
        self.active_alerts: Dict[str, Alert] = {}
        self.alert_history: deque = deque(maxlen=self.config.get('max_alert_history', 1000))
        self.alert_queue: queue.Queue = queue.Queue()
        
        # Monitoring state
        self.monitoring_enabled = True
        self.last_health_check = None
        self.crawl_failure_counts = defaultdict(int)
        
        # Start background monitoring thread
        self.monitoring_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
        self.monitoring_thread.start()
    
    def process_crawl_result(self, result: CrawlResult) -> List[Alert]:
        """
        Process crawl result and generate alerts for failures.
        
        Args:
            result: Crawl result to process
            
        Returns:
            List of generated alerts
        """
        alerts = []
        
        try:
            # Check for parsing failures
            if result.errors:
                # Count consecutive failures
                self.crawl_failure_counts[result.source] += 1
                
                # Generate alert based on failure count
                severity = AlertSeverity.MEDIUM
                if self.crawl_failure_counts[result.source] >= 3:
                    severity = AlertSeverity.HIGH
                elif self.crawl_failure_counts[result.source] >= 5:
                    severity = AlertSeverity.CRITICAL
                
                alert = Alert(
                    id=f"parsing_failure_{result.source}_{int(time.time())}",
                    type=AlertType.PARSING_FAILURE,
                    severity=severity,
                    title=f"Parsing Failures in {result.source}",
                    message=f"Crawler {result.source} encountered {len(result.errors)} parsing failures",
                    details={
                        'source': result.source,
                        'errors': result.errors,
                        'products_found': result.products_found,
                        'products_extracted': result.products_extracted,
                        'consecutive_failures': self.crawl_failure_counts[result.source]
                    },
                    source=result.source
                )
                alerts.append(alert)
                self._add_alert(alert)
                
                self.logger.warning(f"Parsing failures detected in {result.source}: {len(result.errors)} errors")
            
            else:
                # Reset failure count on success
                self.crawl_failure_counts[result.source] = 0
            
            # Check for low extraction rate
            if result.products_found > 0:
                extraction_rate = result.products_extracted / result.products_found
                if extraction_rate < 0.5:  # Less than 50% extraction rate
                    alert = Alert(
                        id=f"low_extraction_{result.source}_{int(time.time())}",
                        type=AlertType.PARSING_FAILURE,
                        severity=AlertSeverity.MEDIUM,
                        title=f"Low Extraction Rate in {result.source}",
                        message=f"Only {extraction_rate:.1%} of products were successfully extracted",
                        details={
                            'source': result.source,
                            'extraction_rate': extraction_rate,
                            'products_found': result.products_found,
                            'products_extracted': result.products_extracted
                        },
                        source=result.source
                    )
                    alerts.append(alert)
                    self._add_alert(alert)
            
        except Exception as e:
            self.logger.error(f"Error processing crawl result: {str(e)}")
        
        return alerts
    
    def check_data_integrity(self, products: List[StandardizedProduct]) -> List[Alert]:
        """
        Check data integrity and generate alerts.
        
        Args:
            products: List of products to check
            
        Returns:
            List of generated alerts
        """
        alerts = []
        
        try:
            # Check for price anomalies
            price_anomalies = self.data_integrity_monitor.check_price_anomalies(products)
            if price_anomalies:
                high_severity_anomalies = [a for a in price_anomalies if a.get('severity') == 'high']
                
                if high_severity_anomalies:
                    alert = Alert(
                        id=f"price_anomalies_{int(time.time())}",
                        type=AlertType.DATA_INTEGRITY,
                        severity=AlertSeverity.HIGH,
                        title="Price Anomalies Detected",
                        message=f"Found {len(high_severity_anomalies)} high-severity price anomalies",
                        details={
                            'total_anomalies': len(price_anomalies),
                            'high_severity_anomalies': len(high_severity_anomalies),
                            'anomalies': price_anomalies[:10]  # Include first 10 for details
                        }
                    )
                    alerts.append(alert)
                    self._add_alert(alert)
            
            # Check for duplicates
            duplicates = self.data_integrity_monitor.check_duplicate_data(products)
            if len(duplicates) > 10:  # Alert if many duplicates
                alert = Alert(
                    id=f"duplicate_data_{int(time.time())}",
                    type=AlertType.DATA_INTEGRITY,
                    severity=AlertSeverity.MEDIUM,
                    title="Duplicate Data Detected",
                    message=f"Found {len(duplicates)} duplicate data entries",
                    details={'duplicate_count': len(duplicates)}
                )
                alerts.append(alert)
                self._add_alert(alert)
            
            # Check for missing fields
            missing_fields = self.data_integrity_monitor.check_missing_required_fields(products)
            if missing_fields:
                high_severity_missing = [m for m in missing_fields if m.get('severity') == 'high']
                
                if high_severity_missing:
                    alert = Alert(
                        id=f"missing_fields_{int(time.time())}",
                        type=AlertType.DATA_INTEGRITY,
                        severity=AlertSeverity.HIGH,
                        title="Missing Required Fields",
                        message=f"Found {len(high_severity_missing)} products with critical missing fields",
                        details={
                            'total_missing': len(missing_fields),
                            'high_severity_missing': len(high_severity_missing)
                        }
                    )
                    alerts.append(alert)
                    self._add_alert(alert)
            
        except Exception as e:
            self.logger.error(f"Error checking data integrity: {str(e)}")
        
        return alerts
    
    def get_system_health(self) -> SystemHealthMetrics:
        """Get current system health metrics."""
        return self.system_health_monitor.collect_system_metrics()
    
    def get_active_alerts(self, severity: Optional[AlertSeverity] = None) -> List[Alert]:
        """
        Get active alerts, optionally filtered by severity.
        
        Args:
            severity: Optional severity filter
            
        Returns:
            List of active alerts
        """
        alerts = list(self.active_alerts.values())
        
        if severity:
            alerts = [a for a in alerts if a.severity == severity]
        
        return sorted(alerts, key=lambda a: a.timestamp, reverse=True)
    
    def get_alert_summary(self) -> Dict[str, Any]:
        """Get summary of alert status."""
        active_alerts = list(self.active_alerts.values())
        
        severity_counts = defaultdict(int)
        type_counts = defaultdict(int)
        
        for alert in active_alerts:
            severity_counts[alert.severity.value] += 1
            type_counts[alert.type.value] += 1
        
        return {
            'total_active_alerts': len(active_alerts),
            'by_severity': dict(severity_counts),
            'by_type': dict(type_counts),
            'last_alert': max(active_alerts, key=lambda a: a.timestamp).timestamp if active_alerts else None,
            'data_integrity_summary': self.data_integrity_monitor.get_integrity_check_summary()
        }
    
    def resolve_alert(self, alert_id: str) -> bool:
        """
        Resolve an active alert.
        
        Args:
            alert_id: ID of alert to resolve
            
        Returns:
            True if alert was resolved, False if not found
        """
        if alert_id in self.active_alerts:
            alert = self.active_alerts[alert_id]
            alert.resolved = True
            alert.resolved_at = datetime.now()
            
            # Move to history
            self.alert_history.append(alert)
            del self.active_alerts[alert_id]
            
            self.logger.info(f"Alert resolved: {alert_id}")
            return True
        
        return False
    
    def _add_alert(self, alert: Alert) -> None:
        """Add an alert to active alerts."""
        self.active_alerts[alert.id] = alert
        self.alert_queue.put(alert)
        
        # Send notification if service is available
        if self.notification_service:
            try:
                self._send_alert_notification(alert)
            except Exception as e:
                self.logger.error(f"Failed to send alert notification: {str(e)}")
    
    def _send_alert_notification(self, alert: Alert) -> None:
        """Send notification for an alert."""
        if not self.notification_service:
            return
        
        # Only send notifications for high and critical alerts
        if alert.severity in [AlertSeverity.HIGH, AlertSeverity.CRITICAL]:
            message = f"🚨 {alert.title}\n\n{alert.message}"
            
            if alert.details:
                message += f"\n\nDetails: {alert.details}"
            
            # Try to send via configured notification channels
            try:
                # This would use the notification service to send alerts
                # Implementation depends on the notification service interface
                self.logger.info(f"Alert notification sent: {alert.title}")
            except Exception as e:
                self.logger.error(f"Failed to send alert notification: {str(e)}")
    
    def _monitoring_loop(self) -> None:
        """Background monitoring loop."""
        while self.monitoring_enabled:
            try:
                # Perform periodic health checks
                if (not self.last_health_check or 
                    datetime.now() - self.last_health_check > timedelta(minutes=5)):
                    
                    health_alerts = self.system_health_monitor.check_system_health()
                    for alert in health_alerts:
                        self._add_alert(alert)
                    
                    self.last_health_check = datetime.now()
                
                # Clean up old resolved alerts from history
                cutoff_time = datetime.now() - timedelta(days=7)
                while (self.alert_history and 
                       self.alert_history[0].timestamp < cutoff_time):
                    self.alert_history.popleft()
                
                # Sleep before next check
                time.sleep(60)  # Check every minute
                
            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {str(e)}")
                time.sleep(60)
    
    def stop_monitoring(self) -> None:
        """Stop the monitoring service."""
        self.monitoring_enabled = False
        if self.monitoring_thread.is_alive():
            self.monitoring_thread.join(timeout=5)
        
        self.logger.info("Monitoring service stopped")
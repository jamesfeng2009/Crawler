"""
Health check endpoints and utilities for the monitoring system.
"""

import json
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from flask import Flask, jsonify, request
import threading

from memory_price_monitor.services.monitoring import MonitoringService, AlertSeverity
from memory_price_monitor.utils.logging import get_logger


class HealthCheckServer:
    """Simple HTTP server for health check endpoints."""
    
    def __init__(self, monitoring_service: MonitoringService, 
                 host: str = '127.0.0.1', port: int = 8080):
        """
        Initialize health check server.
        
        Args:
            monitoring_service: The monitoring service instance
            host: Host to bind to
            port: Port to bind to
        """
        self.monitoring_service = monitoring_service
        self.host = host
        self.port = port
        self.logger = get_logger(__name__)
        
        # Create Flask app
        self.app = Flask(__name__)
        self._setup_routes()
        
        # Server thread
        self.server_thread = None
        self.running = False
    
    def _setup_routes(self):
        """Setup HTTP routes for health checks."""
        
        @self.app.route('/health', methods=['GET'])
        def health_check():
            """Basic health check endpoint."""
            try:
                metrics = self.monitoring_service.get_system_health()
                alert_summary = self.monitoring_service.get_alert_summary()
                
                # Determine overall health status
                status = "healthy"
                if alert_summary['total_active_alerts'] > 0:
                    critical_alerts = alert_summary['by_severity'].get('critical', 0)
                    high_alerts = alert_summary['by_severity'].get('high', 0)
                    
                    if critical_alerts > 0:
                        status = "critical"
                    elif high_alerts > 0:
                        status = "degraded"
                    else:
                        status = "warning"
                
                return jsonify({
                    'status': status,
                    'timestamp': datetime.now().isoformat(),
                    'system_metrics': metrics.to_dict(),
                    'alert_summary': alert_summary
                })
                
            except Exception as e:
                self.logger.error(f"Health check failed: {str(e)}")
                return jsonify({
                    'status': 'error',
                    'timestamp': datetime.now().isoformat(),
                    'error': str(e)
                }), 500
        
        @self.app.route('/health/detailed', methods=['GET'])
        def detailed_health_check():
            """Detailed health check with more information."""
            try:
                metrics = self.monitoring_service.get_system_health()
                alert_summary = self.monitoring_service.get_alert_summary()
                active_alerts = self.monitoring_service.get_active_alerts()
                
                # Get metrics history
                hours = request.args.get('hours', 1, type=int)
                metrics_history = self.monitoring_service.system_health_monitor.get_metrics_history(hours)
                
                return jsonify({
                    'status': 'healthy' if alert_summary['total_active_alerts'] == 0 else 'degraded',
                    'timestamp': datetime.now().isoformat(),
                    'system_metrics': metrics.to_dict(),
                    'alert_summary': alert_summary,
                    'active_alerts': [alert.to_dict() for alert in active_alerts],
                    'metrics_history': [m.to_dict() for m in metrics_history],
                    'data_integrity': self.monitoring_service.data_integrity_monitor.get_integrity_check_summary()
                })
                
            except Exception as e:
                self.logger.error(f"Detailed health check failed: {str(e)}")
                return jsonify({
                    'status': 'error',
                    'timestamp': datetime.now().isoformat(),
                    'error': str(e)
                }), 500
        
        @self.app.route('/alerts', methods=['GET'])
        def get_alerts():
            """Get active alerts."""
            try:
                severity_filter = request.args.get('severity')
                severity = None
                
                if severity_filter:
                    try:
                        severity = AlertSeverity(severity_filter.lower())
                    except ValueError:
                        return jsonify({'error': 'Invalid severity level'}), 400
                
                alerts = self.monitoring_service.get_active_alerts(severity)
                
                return jsonify({
                    'alerts': [alert.to_dict() for alert in alerts],
                    'total': len(alerts),
                    'timestamp': datetime.now().isoformat()
                })
                
            except Exception as e:
                self.logger.error(f"Get alerts failed: {str(e)}")
                return jsonify({
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                }), 500
        
        @self.app.route('/alerts/<alert_id>/resolve', methods=['POST'])
        def resolve_alert(alert_id):
            """Resolve an alert."""
            try:
                success = self.monitoring_service.resolve_alert(alert_id)
                
                if success:
                    return jsonify({
                        'message': f'Alert {alert_id} resolved',
                        'timestamp': datetime.now().isoformat()
                    })
                else:
                    return jsonify({
                        'error': f'Alert {alert_id} not found',
                        'timestamp': datetime.now().isoformat()
                    }), 404
                    
            except Exception as e:
                self.logger.error(f"Resolve alert failed: {str(e)}")
                return jsonify({
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                }), 500
        
        @self.app.route('/metrics', methods=['GET'])
        def get_metrics():
            """Get system metrics."""
            try:
                hours = request.args.get('hours', 24, type=int)
                metrics_history = self.monitoring_service.system_health_monitor.get_metrics_history(hours)
                current_metrics = self.monitoring_service.get_system_health()
                
                return jsonify({
                    'current_metrics': current_metrics.to_dict(),
                    'history': [m.to_dict() for m in metrics_history],
                    'timestamp': datetime.now().isoformat()
                })
                
            except Exception as e:
                self.logger.error(f"Get metrics failed: {str(e)}")
                return jsonify({
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                }), 500
        
        @self.app.route('/data-integrity', methods=['GET'])
        def get_data_integrity():
            """Get data integrity information."""
            try:
                summary = self.monitoring_service.data_integrity_monitor.get_integrity_check_summary()
                flagged_points = self.monitoring_service.data_integrity_monitor.get_flagged_data_points(
                    limit=request.args.get('limit', 100, type=int)
                )
                
                return jsonify({
                    'summary': summary,
                    'flagged_points': flagged_points,
                    'timestamp': datetime.now().isoformat()
                })
                
            except Exception as e:
                self.logger.error(f"Get data integrity failed: {str(e)}")
                return jsonify({
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                }), 500
    
    def start(self):
        """Start the health check server."""
        if self.running:
            return
        
        self.running = True
        self.server_thread = threading.Thread(
            target=self._run_server,
            daemon=True
        )
        self.server_thread.start()
        self.logger.info(f"Health check server started on {self.host}:{self.port}")
    
    def stop(self):
        """Stop the health check server."""
        self.running = False
        if self.server_thread and self.server_thread.is_alive():
            self.server_thread.join(timeout=5)
        self.logger.info("Health check server stopped")
    
    def _run_server(self):
        """Run the Flask server."""
        try:
            self.app.run(
                host=self.host,
                port=self.port,
                debug=False,
                use_reloader=False,
                threaded=True
            )
        except Exception as e:
            self.logger.error(f"Health check server error: {str(e)}")


class SimpleHealthChecker:
    """Simple health checker without HTTP server for basic monitoring."""
    
    def __init__(self, monitoring_service: MonitoringService):
        """Initialize simple health checker."""
        self.monitoring_service = monitoring_service
        self.logger = get_logger(__name__)
    
    def check_health(self) -> Dict[str, Any]:
        """
        Perform a simple health check.
        
        Returns:
            Health status dictionary
        """
        try:
            metrics = self.monitoring_service.get_system_health()
            alert_summary = self.monitoring_service.get_alert_summary()
            
            # Determine overall status
            status = "healthy"
            issues = []
            
            # Check system metrics
            if metrics.cpu_usage_percent > 90:
                status = "degraded"
                issues.append(f"High CPU usage: {metrics.cpu_usage_percent:.1f}%")
            
            if metrics.memory_usage_percent > 85:
                status = "degraded"
                issues.append(f"High memory usage: {metrics.memory_usage_percent:.1f}%")
            
            if metrics.disk_usage_percent > 90:
                status = "critical"
                issues.append(f"High disk usage: {metrics.disk_usage_percent:.1f}%")
            
            # Check alerts
            critical_alerts = alert_summary['by_severity'].get('critical', 0)
            high_alerts = alert_summary['by_severity'].get('high', 0)
            
            if critical_alerts > 0:
                status = "critical"
                issues.append(f"{critical_alerts} critical alerts")
            elif high_alerts > 0 and status == "healthy":
                status = "degraded"
                issues.append(f"{high_alerts} high-priority alerts")
            
            return {
                'status': status,
                'timestamp': datetime.now().isoformat(),
                'issues': issues,
                'metrics': {
                    'cpu_usage': metrics.cpu_usage_percent,
                    'memory_usage': metrics.memory_usage_percent,
                    'disk_usage': metrics.disk_usage_percent,
                    'avg_response_time_ms': metrics.average_response_time_ms
                },
                'alerts': {
                    'total': alert_summary['total_active_alerts'],
                    'critical': critical_alerts,
                    'high': high_alerts
                }
            }
            
        except Exception as e:
            self.logger.error(f"Health check failed: {str(e)}")
            return {
                'status': 'error',
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }
    
    def print_health_status(self) -> None:
        """Print health status to console."""
        health = self.check_health()
        
        print(f"\n=== System Health Check - {health['timestamp']} ===")
        print(f"Status: {health['status'].upper()}")
        
        if health.get('issues'):
            print("\nIssues:")
            for issue in health['issues']:
                print(f"  - {issue}")
        
        if 'metrics' in health:
            metrics = health['metrics']
            print(f"\nSystem Metrics:")
            print(f"  CPU Usage: {metrics['cpu_usage']:.1f}%")
            print(f"  Memory Usage: {metrics['memory_usage']:.1f}%")
            print(f"  Disk Usage: {metrics['disk_usage']:.1f}%")
            print(f"  Avg Response Time: {metrics['avg_response_time_ms']:.0f}ms")
        
        if 'alerts' in health:
            alerts = health['alerts']
            print(f"\nAlerts:")
            print(f"  Total: {alerts['total']}")
            print(f"  Critical: {alerts['critical']}")
            print(f"  High: {alerts['high']}")
        
        print("=" * 50)
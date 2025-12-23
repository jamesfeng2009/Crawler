"""
Configuration management for the memory price monitor system.
"""

import os
import threading
import time
from typing import Dict, Any, List, Optional, Callable, Set
from dataclasses import dataclass, field, asdict
from pathlib import Path
import json
import logging
from datetime import datetime
import jsonschema
from jsonschema import validate, ValidationError


@dataclass
class DatabaseConfig:
    """Database configuration settings."""
    # Database type: "postgresql" or "sqlite"
    db_type: str = "sqlite"
    
    # PostgreSQL settings (when db_type="postgresql")
    host: str = "localhost"
    port: int = 5432
    database: str = "memory_price_monitor"
    username: str = "postgres"
    password: str = ""
    pool_size: int = 10
    max_overflow: int = 20
    
    # SQLite settings (when db_type="sqlite")
    sqlite_path: str = "data/memory_price_monitor.db"


@dataclass
class RedisConfig:
    """Redis configuration settings."""
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None


@dataclass
class CrawlerConfig:
    """Crawler configuration settings."""
    user_agents: List[str] = field(default_factory=lambda: [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
    ])
    request_timeout: int = 30
    retry_attempts: int = 3
    retry_delay: float = 1.0
    rate_limit_delay: float = 2.0
    concurrent_limit: int = 5


@dataclass
class NotificationConfig:
    """Notification configuration settings."""
    wechat_method: str = "serverchan"  # "serverchan" or "wxpy"
    serverchan_key: Optional[str] = None
    email_smtp_host: str = "smtp.gmail.com"
    email_smtp_port: int = 587
    email_username: Optional[str] = None
    email_password: Optional[str] = None
    email_recipients: List[str] = field(default_factory=list)
    retry_attempts: int = 3
    retry_delay: float = 5.0
    # User-specific preferences (isolated from system config)
    user_preferences: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SchedulerConfig:
    """Scheduler configuration settings."""
    daily_crawl_hour: int = 10
    daily_crawl_minute: int = 0
    weekly_report_day: int = 0  # Monday
    weekly_report_hour: int = 9
    timezone: str = "Asia/Shanghai"


@dataclass
class SystemConfig:
    """Main system configuration."""
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    redis: RedisConfig = field(default_factory=RedisConfig)
    crawler: CrawlerConfig = field(default_factory=CrawlerConfig)
    notification: NotificationConfig = field(default_factory=NotificationConfig)
    scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)
    log_level: str = "INFO"
    data_retention_days: int = 365


# Configuration schema for validation
CONFIG_SCHEMA = {
    "type": "object",
    "properties": {
        "database": {
            "type": "object",
            "properties": {
                "db_type": {"type": "string", "enum": ["postgresql", "sqlite"]},
                "sqlite_path": {"type": "string", "minLength": 1},
                "host": {"type": "string", "minLength": 1},
                "port": {"type": "integer", "minimum": 1, "maximum": 65535},
                "database": {"type": "string", "minLength": 1},
                "username": {"type": "string", "minLength": 1},
                "password": {"type": "string"},
                "pool_size": {"type": "integer", "minimum": 1, "maximum": 1000},
                "max_overflow": {"type": "integer", "minimum": 0, "maximum": 1000}
            },
            "required": ["db_type"],
            "additionalProperties": False
        },
        "redis": {
            "type": "object",
            "properties": {
                "host": {"type": "string", "minLength": 1},
                "port": {"type": "integer", "minimum": 1, "maximum": 65535},
                "db": {"type": "integer", "minimum": 0, "maximum": 15},
                "password": {"type": ["string", "null"]}
            },
            "required": ["host", "port"],
            "additionalProperties": False
        },
        "crawler": {
            "type": "object",
            "properties": {
                "user_agents": {
                    "type": "array",
                    "items": {"type": "string", "minLength": 10},
                    "minItems": 1
                },
                "request_timeout": {"type": "integer", "minimum": 1, "maximum": 300},
                "retry_attempts": {"type": "integer", "minimum": 1, "maximum": 10},
                "retry_delay": {"type": "number", "minimum": 0.1, "maximum": 60.0},
                "rate_limit_delay": {"type": "number", "minimum": 0.1, "maximum": 60.0},
                "concurrent_limit": {"type": "integer", "minimum": 1, "maximum": 50}
            },
            "additionalProperties": False
        },
        "notification": {
            "type": "object",
            "properties": {
                "wechat_method": {"type": "string", "enum": ["serverchan", "wxpy"]},
                "serverchan_key": {"type": ["string", "null"]},
                "email_smtp_host": {"type": "string", "minLength": 1},
                "email_smtp_port": {"type": "integer", "minimum": 1, "maximum": 65535},
                "email_username": {"type": ["string", "null"]},
                "email_password": {"type": ["string", "null"]},
                "email_recipients": {
                    "type": "array",
                    "items": {"type": "string", "format": "email"}
                },
                "retry_attempts": {"type": "integer", "minimum": 1, "maximum": 10},
                "retry_delay": {"type": "number", "minimum": 0.1, "maximum": 300.0},
                "user_preferences": {"type": "object"}
            },
            "additionalProperties": False
        },
        "scheduler": {
            "type": "object",
            "properties": {
                "daily_crawl_hour": {"type": "integer", "minimum": 0, "maximum": 23},
                "daily_crawl_minute": {"type": "integer", "minimum": 0, "maximum": 59},
                "weekly_report_day": {"type": "integer", "minimum": 0, "maximum": 6},
                "weekly_report_hour": {"type": "integer", "minimum": 0, "maximum": 23},
                "timezone": {"type": "string", "minLength": 1}
            },
            "additionalProperties": False
        },
        "log_level": {
            "type": "string",
            "enum": ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        },
        "data_retention_days": {"type": "integer", "minimum": 1, "maximum": 10000}
    },
    "additionalProperties": False
}


class ConfigurationError(Exception):
    """Configuration-related errors."""
    pass


class ConfigChangeListener:
    """Interface for configuration change listeners."""
    
    def on_config_changed(self, old_config: SystemConfig, new_config: SystemConfig) -> None:
        """Called when configuration changes."""
        pass


class ConfigManager:
    """Enhanced configuration manager with validation, change detection, and preference isolation."""
    
    def __init__(self, config_path: str = "config.json"):
        self.config_path = Path(config_path)
        self._config: Optional[SystemConfig] = None
        self._last_modified: Optional[float] = None
        self._lock = threading.RLock()
        self._change_listeners: List[ConfigChangeListener] = []
        self._user_preferences: Dict[str, Dict[str, Any]] = {}
        self._monitoring_thread: Optional[threading.Thread] = None
        self._stop_monitoring = threading.Event()
        
    def add_change_listener(self, listener: ConfigChangeListener) -> None:
        """Add a configuration change listener."""
        with self._lock:
            self._change_listeners.append(listener)
    
    def remove_change_listener(self, listener: ConfigChangeListener) -> None:
        """Remove a configuration change listener."""
        with self._lock:
            if listener in self._change_listeners:
                self._change_listeners.remove(listener)
    
    def start_monitoring(self, check_interval: float = 1.0) -> None:
        """Start monitoring configuration file for changes."""
        if self._monitoring_thread and self._monitoring_thread.is_alive():
            return
            
        self._stop_monitoring.clear()
        self._monitoring_thread = threading.Thread(
            target=self._monitor_config_changes,
            args=(check_interval,),
            daemon=True
        )
        self._monitoring_thread.start()
        logging.info("Configuration monitoring started")
    
    def stop_monitoring(self) -> None:
        """Stop monitoring configuration file for changes."""
        self._stop_monitoring.set()
        if self._monitoring_thread:
            self._monitoring_thread.join(timeout=5.0)
        logging.info("Configuration monitoring stopped")
    
    def _monitor_config_changes(self, check_interval: float) -> None:
        """Monitor configuration file for changes in a separate thread."""
        while not self._stop_monitoring.wait(check_interval):
            try:
                if self.reload_if_changed():
                    logging.info("Configuration automatically reloaded due to file changes")
            except Exception as e:
                logging.error(f"Error during automatic config reload: {e}")
    
    def validate_config(self, config_data: Dict[str, Any]) -> None:
        """Validate configuration data against schema."""
        try:
            validate(instance=config_data, schema=CONFIG_SCHEMA)
        except ValidationError as e:
            raise ConfigurationError(f"Configuration validation failed: {e.message}")
    
    def load_config(self) -> SystemConfig:
        """Load configuration from file or environment variables."""
        with self._lock:
            # Check if config file exists and has been modified
            if self.config_path.exists():
                current_modified = self.config_path.stat().st_mtime
                if self._config is None or current_modified != self._last_modified:
                    self._load_from_file()
                    self._last_modified = current_modified
            else:
                # Load from environment variables if no config file
                self._load_from_env()
                
            return self._config or SystemConfig()
    
    def _load_from_file(self) -> None:
        """Load configuration from JSON file with validation."""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
            
            # Validate configuration
            self.validate_config(config_data)
            
            old_config = self._config
            self._config = self._dict_to_config(config_data)
            
            # 优先使用环境变量覆盖配置文件中的敏感信息
            self._override_with_env_vars()
            
            # Apply user preferences
            self._apply_user_preferences()
            
            # Notify listeners of configuration change
            if old_config is not None:
                self._notify_config_changed(old_config, self._config)
            
            logging.info(f"Configuration loaded and validated from {self.config_path}")
            
        except ValidationError as e:
            logging.error(f"Configuration validation failed: {e}")
            raise ConfigurationError(f"Invalid configuration: {e}")
        except Exception as e:
            logging.error(f"Failed to load config from file: {e}")
            if self._config is None:
                self._config = SystemConfig()
    
    def _override_with_env_vars(self) -> None:
        """Override configuration with environment variables."""
        # 尝试从 .env 文件加载环境变量
        env_file = Path('.env')
        if env_file.exists():
            try:
                with open(env_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#') and '=' in line:
                            key, value = line.split('=', 1)
                            os.environ[key.strip()] = value.strip()
                logging.info("Loaded environment variables from .env file")
            except Exception as e:
                logging.warning(f"Failed to load .env file: {e}")
        
        # 使用环境变量覆盖配置
        if os.getenv("EMAIL_USERNAME"):
            self._config.notification.email_username = os.getenv("EMAIL_USERNAME")
        
        if os.getenv("EMAIL_PASSWORD"):
            self._config.notification.email_password = os.getenv("EMAIL_PASSWORD")
        
        # 处理收件人列表
        email_recipients = os.getenv("EMAIL_RECIPIENTS")
        if email_recipients:
            # 支持逗号分隔的多个收件人
            self._config.notification.email_recipients = [
                email.strip() for email in email_recipients.split(',') if email.strip()
            ]
        
        # 其他环境变量
        if os.getenv("SERVERCHAN_KEY"):
            self._config.notification.serverchan_key = os.getenv("SERVERCHAN_KEY")
        
        if os.getenv("DB_HOST"):
            self._config.database.host = os.getenv("DB_HOST")
        
        if os.getenv("DB_PASSWORD"):
            self._config.database.password = os.getenv("DB_PASSWORD")
    
    def _load_from_env(self) -> None:
        """Load configuration from environment variables."""
        config = SystemConfig()
        
        # 尝试从 .env 文件加载环境变量
        env_file = Path('.env')
        if env_file.exists():
            try:
                with open(env_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#') and '=' in line:
                            key, value = line.split('=', 1)
                            os.environ[key.strip()] = value.strip()
                logging.info("Loaded environment variables from .env file")
            except Exception as e:
                logging.warning(f"Failed to load .env file: {e}")
        
        # Database config
        config.database.host = os.getenv("DB_HOST", config.database.host)
        config.database.port = int(os.getenv("DB_PORT", str(config.database.port)))
        config.database.database = os.getenv("DB_NAME", config.database.database)
        config.database.username = os.getenv("DB_USER", config.database.username)
        config.database.password = os.getenv("DB_PASSWORD", config.database.password)
        
        # Redis config
        config.redis.host = os.getenv("REDIS_HOST", config.redis.host)
        config.redis.port = int(os.getenv("REDIS_PORT", str(config.redis.port)))
        config.redis.password = os.getenv("REDIS_PASSWORD")
        
        # Notification config - 优先使用环境变量
        config.notification.serverchan_key = os.getenv("SERVERCHAN_KEY")
        config.notification.email_username = os.getenv("EMAIL_USERNAME")
        config.notification.email_password = os.getenv("EMAIL_PASSWORD")
        
        # 处理收件人列表
        email_recipients = os.getenv("EMAIL_RECIPIENTS")
        if email_recipients:
            # 支持逗号分隔的多个收件人
            config.notification.email_recipients = [
                email.strip() for email in email_recipients.split(',') if email.strip()
            ]
        
        old_config = self._config
        self._config = config
        
        # Apply user preferences
        self._apply_user_preferences()
        
        # Notify listeners if this is a reload
        if old_config is not None:
            self._notify_config_changed(old_config, self._config)
        
        logging.info("Configuration loaded from environment variables")
    
    def _dict_to_config(self, data: Dict[str, Any]) -> SystemConfig:
        """Convert dictionary to SystemConfig object."""
        config = SystemConfig()
        
        if "database" in data:
            db_data = data["database"]
            config.database = DatabaseConfig(**db_data)
            
        if "redis" in data:
            redis_data = data["redis"]
            config.redis = RedisConfig(**redis_data)
            
        if "crawler" in data:
            crawler_data = data["crawler"]
            config.crawler = CrawlerConfig(**crawler_data)
            
        if "notification" in data:
            notif_data = data["notification"]
            # Extract user_preferences separately to avoid passing it to dataclass constructor
            user_prefs = notif_data.pop("user_preferences", {})
            config.notification = NotificationConfig(**notif_data)
            config.notification.user_preferences = user_prefs
            
        if "scheduler" in data:
            sched_data = data["scheduler"]
            config.scheduler = SchedulerConfig(**sched_data)
            
        config.log_level = data.get("log_level", config.log_level)
        config.data_retention_days = data.get("data_retention_days", config.data_retention_days)
        
        return config
    
    def _apply_user_preferences(self) -> None:
        """Apply user-specific preferences to notification config."""
        if self._config and self._user_preferences:
            for user_id, preferences in self._user_preferences.items():
                # Store preferences in the notification config
                self._config.notification.user_preferences[user_id] = preferences
    
    def _notify_config_changed(self, old_config: SystemConfig, new_config: SystemConfig) -> None:
        """Notify all listeners of configuration changes."""
        for listener in self._change_listeners:
            try:
                listener.on_config_changed(old_config, new_config)
            except Exception as e:
                logging.error(f"Error notifying config change listener: {e}")
    
    def reload_if_changed(self) -> bool:
        """Check if config file has changed and reload if necessary."""
        with self._lock:
            if not self.config_path.exists():
                return False
                
            current_modified = self.config_path.stat().st_mtime
            if current_modified != self._last_modified:
                self.load_config()
                return True
            return False
    
    def set_user_preference(self, user_id: str, key: str, value: Any) -> None:
        """Set a user-specific preference (isolated from system config)."""
        with self._lock:
            if user_id not in self._user_preferences:
                self._user_preferences[user_id] = {}
            
            old_value = self._user_preferences[user_id].get(key)
            self._user_preferences[user_id][key] = value
            
            # Apply to current config if loaded
            if self._config:
                if user_id not in self._config.notification.user_preferences:
                    self._config.notification.user_preferences[user_id] = {}
                self._config.notification.user_preferences[user_id][key] = value
            
            logging.info(f"User preference updated: {user_id}.{key} = {value}")
    
    def get_user_preference(self, user_id: str, key: str, default: Any = None) -> Any:
        """Get a user-specific preference."""
        with self._lock:
            return self._user_preferences.get(user_id, {}).get(key, default)
    
    def get_user_preferences(self, user_id: str) -> Dict[str, Any]:
        """Get all preferences for a specific user."""
        with self._lock:
            return self._user_preferences.get(user_id, {}).copy()
    
    def remove_user_preferences(self, user_id: str) -> None:
        """Remove all preferences for a specific user."""
        with self._lock:
            if user_id in self._user_preferences:
                del self._user_preferences[user_id]
            
            if self._config and user_id in self._config.notification.user_preferences:
                del self._config.notification.user_preferences[user_id]
            
            logging.info(f"User preferences removed for: {user_id}")
    
    def export_config(self) -> Dict[str, Any]:
        """Export current configuration as dictionary."""
        with self._lock:
            if not self._config:
                return {}
            
            return {
                "database": asdict(self._config.database),
                "redis": asdict(self._config.redis),
                "crawler": asdict(self._config.crawler),
                "notification": asdict(self._config.notification),
                "scheduler": asdict(self._config.scheduler),
                "log_level": self._config.log_level,
                "data_retention_days": self._config.data_retention_days
            }
    
    def save_config(self, config_path: Optional[str] = None) -> None:
        """Save current configuration to file."""
        with self._lock:
            if not self._config:
                raise ConfigurationError("No configuration loaded to save")
            
            save_path = Path(config_path) if config_path else self.config_path
            config_dict = self.export_config()
            
            # Validate before saving
            self.validate_config(config_dict)
            
            with open(save_path, 'w', encoding='utf-8') as f:
                json.dump(config_dict, f, indent=2, ensure_ascii=False)
            
            logging.info(f"Configuration saved to {save_path}")
    
    def __enter__(self):
        """Context manager entry."""
        self.start_monitoring()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop_monitoring()


# Global config manager instance
config_manager = ConfigManager()


def get_config() -> SystemConfig:
    """Get the current system configuration."""
    return config_manager.load_config()


def reload_config() -> SystemConfig:
    """Force reload configuration and return updated config."""
    config_manager._config = None
    config_manager._last_modified = None
    return config_manager.load_config()
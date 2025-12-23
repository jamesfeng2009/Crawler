"""
Logging configuration and utilities.
统一管理所有业务代码的日志配置，包括爬虫、服务器运行等日志
支持7天日志保留策略和自动清理
"""

import logging
import logging.handlers
import sys
import os
import time
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
import threading
import schedule

try:
    import structlog
    STRUCTLOG_AVAILABLE = True
except ImportError:
    STRUCTLOG_AVAILABLE = False
    structlog = None


def setup_logging(
    log_level: str = "INFO",
    log_file: Optional[str] = None,
    max_bytes: int = 50 * 1024 * 1024,  # 50MB
    backup_count: int = 3,
    retention_days: int = 7  # 7天日志保留
) -> None:
    """
    Set up structured logging for the application.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional log file path
        max_bytes: Maximum log file size before rotation
        backup_count: Number of backup log files to keep
        retention_days: Number of days to retain log files
    """
    # Configure structlog if available
    if STRUCTLOG_AVAILABLE:
        structlog.configure(
            processors=[
                structlog.stdlib.filter_by_level,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.UnicodeDecoder(),
                structlog.processors.JSONRenderer()
            ],
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )
    
    # Configure standard logging
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))
    
    # Clear existing handlers
    root_logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)
    
    # File handler (if specified)
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 使用时间轮转处理器，每天轮转一次，保留指定天数
        file_handler = logging.handlers.TimedRotatingFileHandler(
            log_file,
            when='midnight',
            interval=1,
            backupCount=retention_days,
            encoding='utf-8'
        )
        file_handler.suffix = "%Y-%m-%d"
        
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)
        
        # 启动日志清理调度器
        _start_log_cleanup_scheduler(log_path.parent, retention_days)


def get_logger(name: str):
    """
    Get a structured logger instance.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        Standard logger (structlog disabled due to compatibility issues)
    """
    # Always use standard logging for compatibility
    return logging.getLogger(name)


def get_business_logger(business_name: str, log_level: str = "INFO") -> logging.Logger:
    """
    获取业务日志记录器的便捷函数
    
    Args:
        business_name: 业务名称 (如 'crawler_zol', 'daily_monitor')
        log_level: 日志级别
        
    Returns:
        配置好的日志记录器
    """
    # 业务日志文件映射
    business_logs = {
        # 主要服务
        "daily_monitor": "logs/daily_monitor.log",
        "email_service": "logs/email_service.log",
        "scheduler": "logs/scheduler.log",
        
        # 爬虫相关
        "crawler_zol": "logs/crawler_zol.log",
        "crawler_jd": "logs/crawler_jd.log", 
        "crawler_playwright": "logs/crawler_playwright.log",
        "crawler_general": "logs/crawler.log",
        
        # 数据相关
        "database": "logs/database.log",
        "repository": "logs/repository.log",
        "price_comparison": "logs/price_comparison.log",
        
        # 系统相关
        "system": "logs/system.log",
        "error": "logs/error.log",
        "performance": "logs/performance.log",
        
        # 监控相关
        "monitoring": "logs/monitoring.log",
        "health_check": "logs/health_check.log",
        "state_manager": "logs/state_manager.log"
    }
    
    if business_name not in business_logs:
        # 如果不在预定义列表中，使用通用日志
        log_file = f"logs/{business_name}.log"
    else:
        log_file = business_logs[business_name]
    
    logger = logging.getLogger(f"business.{business_name}")
    
    # 如果已经配置过，直接返回
    if logger.handlers:
        return logger
    
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # 文件处理器 - 使用时间轮转
    project_root = Path(__file__).parent.parent.parent
    log_path = project_root / log_file
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    file_handler = logging.handlers.TimedRotatingFileHandler(
        log_path,
        when='midnight',
        interval=1,
        backupCount=7,  # 保留7天
        encoding='utf-8'
    )
    
    # 设置文件名后缀
    file_handler.suffix = "%Y-%m-%d"
    
    # 详细的日志格式
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    
    # 控制台处理器 (仅ERROR级别以上)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.ERROR)
    console_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    return logger


def _start_log_cleanup_scheduler(logs_dir: Path, retention_days: int):
    """启动日志清理调度器"""
    def cleanup_job():
        try:
            cleanup_old_logs(logs_dir, retention_days)
        except Exception as e:
            print(f"日志清理失败: {e}")
    
    # 每天凌晨2点执行清理
    schedule.every().day.at("02:00").do(cleanup_job)
    
    # 启动调度线程
    def run_scheduler():
        while True:
            schedule.run_pending()
            time.sleep(60)  # 每分钟检查一次
    
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()


def cleanup_old_logs(logs_dir: Path = None, retention_days: int = 7) -> int:
    """
    清理超过保留期的日志文件
    
    Args:
        logs_dir: 日志目录路径
        retention_days: 保留天数
        
    Returns:
        清理的文件数量
    """
    if logs_dir is None:
        project_root = Path(__file__).parent.parent.parent
        logs_dir = project_root / "logs"
    
    if not logs_dir.exists():
        return 0
    
    cleaned_count = 0
    cutoff_date = datetime.now() - timedelta(days=retention_days)
    
    try:
        for log_file in logs_dir.glob("*.log*"):
            if log_file.is_file():
                # 检查文件修改时间
                file_mtime = datetime.fromtimestamp(log_file.stat().st_mtime)
                
                if file_mtime < cutoff_date:
                    log_file.unlink()
                    cleaned_count += 1
                    print(f"清理过期日志文件: {log_file.name}")
        
        if cleaned_count > 0:
            print(f"日志清理完成，清理了 {cleaned_count} 个文件")
        
    except Exception as e:
        print(f"日志清理过程中出错: {e}")
    
    return cleaned_count


def get_log_statistics(logs_dir: Path = None) -> Dict[str, Any]:
    """获取日志统计信息"""
    if logs_dir is None:
        project_root = Path(__file__).parent.parent.parent
        logs_dir = project_root / "logs"
    
    stats = {
        "total_files": 0,
        "total_size_mb": 0,
        "files_by_business": {},
        "oldest_log": None,
        "newest_log": None
    }
    
    if not logs_dir.exists():
        return stats
    
    oldest_time = None
    newest_time = None
    
    for log_file in logs_dir.glob("*.log*"):
        if log_file.is_file():
            stats["total_files"] += 1
            
            file_size = log_file.stat().st_size
            stats["total_size_mb"] += file_size / 1024 / 1024
            
            file_mtime = datetime.fromtimestamp(log_file.stat().st_mtime)
            
            if oldest_time is None or file_mtime < oldest_time:
                oldest_time = file_mtime
                stats["oldest_log"] = log_file.name
            
            if newest_time is None or file_mtime > newest_time:
                newest_time = file_mtime
                stats["newest_log"] = log_file.name
            
            # 按业务分类统计
            business_name = log_file.stem.replace('.log', '')
            if business_name not in stats["files_by_business"]:
                stats["files_by_business"][business_name] = {
                    "count": 0,
                    "size_mb": 0
                }
            
            stats["files_by_business"][business_name]["count"] += 1
            stats["files_by_business"][business_name]["size_mb"] += file_size / 1024 / 1024
    
    stats["total_size_mb"] = round(stats["total_size_mb"], 2)
    
    return stats


# 业务日志装饰器
def log_business_operation(business_name: str, operation_name: str = None):
    """
    业务操作日志装饰器
    
    Args:
        business_name: 业务名称
        operation_name: 操作名称
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            logger = get_business_logger(business_name)
            op_name = operation_name or func.__name__
            
            logger.info(f"开始执行 {op_name}")
            start_time = time.time()
            
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                logger.info(f"完成执行 {op_name}，耗时: {duration:.2f}秒")
                return result
                
            except Exception as e:
                duration = time.time() - start_time
                logger.error(f"执行 {op_name} 失败，耗时: {duration:.2f}秒，错误: {e}")
                raise
        
        return wrapper
    return decorator
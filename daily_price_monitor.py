#!/usr/bin/env python3
"""
每日内存条价格监控主程序
- 每天上午10点自动爬取ZOL内存条价格
- 对比今日与昨日价格变化
- 生成价格变化报告
"""

import sys
import signal
from pathlib import Path
from datetime import datetime, date
from typing import List
import logging

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from config import get_config
from memory_price_monitor.utils.logging import setup_logging
from memory_price_monitor.data.sqlite_database import SQLiteDatabaseManager
from memory_price_monitor.data.repository import PriceRepository
from memory_price_monitor.services.scheduler import TaskScheduler, ResourceLimits
from memory_price_monitor.services.price_comparison import PriceComparisonService, DailyComparisonReport
from memory_price_monitor.crawlers import CrawlerRegistry
from memory_price_monitor.services.state_manager import StateManager


class DailyPriceMonitor:
    """每日价格监控主类"""
    
    def __init__(self):
        """初始化监控系统"""
        # 加载配置
        self.config = get_config()
        
        # 设置日志
        setup_logging(
            log_level=self.config.log_level,
            log_file="logs/daily_monitor.log",
            retention_days=7  # 7天日志保留
        )
        logging.getLogger().setLevel(getattr(logging, self.config.log_level.upper(), logging.INFO))
        self.logger = logging.getLogger(__name__)
        
        # 初始化数据库
        self.db_manager = SQLiteDatabaseManager(self.config.database.sqlite_path)
        self.db_manager.initialize()
        
        # 初始化仓库
        self.repository = PriceRepository(self.db_manager)
        
        # 初始化价格对比服务
        self.price_comparison = PriceComparisonService(self.repository)
        
        # 初始化邮件服务
        from memory_price_monitor.services.email_service import EmailService
        self.email_service = EmailService(self.config.notification)
        
        # 初始化爬虫注册表
        self.crawler_registry = CrawlerRegistry()
        
        # 初始化状态管理器
        self.state_manager = StateManager()
        
        # 初始化任务调度器
        resource_limits = ResourceLimits(
            max_concurrent_tasks=self.config.crawler.concurrent_limit,
            max_memory_usage_percent=80.0,
            max_cpu_usage_percent=70.0
        )
        
        self.scheduler = TaskScheduler(
            crawler_registry=self.crawler_registry,
            repository=self.repository,
            resource_limits=resource_limits,
            state_manager=self.state_manager
        )
        
        # 设置信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self.logger.info("每日价格监控系统初始化完成")
    
    def start_monitoring(self):
        """启动监控系统"""
        try:
            self.logger.info("启动每日价格监控系统...")
            
            # 启动调度器
            self.scheduler.start()
            
            # 设置每日爬取任务 - 上午10点
            self.scheduler.schedule_daily_crawl(
                hour=self.config.scheduler.daily_crawl_hour,
                minute=getattr(self.config.scheduler, 'daily_crawl_minute', 0)
            )
            
            # 设置每周报告任务
            self.scheduler.schedule_weekly_report(
                day_of_week=self.config.scheduler.weekly_report_day,
                hour=self.config.scheduler.weekly_report_hour
            )
            
            self.logger.info(f"定时任务已设置:")
            self.logger.info(f"  - 每日爬取: {self.config.scheduler.daily_crawl_hour:02d}:00")
            self.logger.info(f"  - 每周报告: 周{self.config.scheduler.weekly_report_day} {self.config.scheduler.weekly_report_hour:02d}:00")
            
            # 显示系统状态
            self._show_system_status()
            
            # 保持运行
            self._keep_running()
            
        except Exception as e:
            self.logger.error(f"启动监控系统失败: {e}")
            raise
    
    def run_immediate_crawl(self):
        """立即执行一次爬取任务"""
        try:
            self.logger.info("开始立即执行爬取任务...")
            
            # 启动调度器（如果未启动）
            if not self.scheduler._running:
                self.scheduler.start()
            
            # 执行爬取任务
            task_ids = self.scheduler.execute_crawl_task(['zol_playwright'])
            
            self.logger.info(f"爬取任务已提交，任务ID: {task_ids}")
            
            # 等待任务完成
            import time
            max_wait = 300  # 最多等待5分钟
            wait_time = 0
            
            while wait_time < max_wait:
                status = self.scheduler.get_scheduler_status()
                active_tasks = status.get('active_tasks', 0)
                
                if active_tasks == 0:
                    self.logger.info("爬取任务已完成")
                    break
                
                self.logger.info(f"等待任务完成... (活跃任务: {active_tasks})")
                time.sleep(10)
                wait_time += 10
            
            # 生成价格对比报告
            report = self.generate_price_comparison_report()
            
            # 发送邮件报告
            if report:
                self.send_email_report(report)
            
        except Exception as e:
            self.logger.error(f"立即爬取失败: {e}")
            raise
    
    def generate_price_comparison_report(self, target_date: date = None):
        """生成价格对比报告"""
        try:
            if target_date is None:
                target_date = date.today()
            
            self.logger.info(f"生成价格对比报告: {target_date}")
            
            # 生成对比报告
            report = self.price_comparison.compare_daily_prices(
                target_date=target_date,
                source_filter='zol_playwright'
            )
            
            # 格式化报告
            report_text = self.price_comparison.format_report_text(report)
            
            # 输出报告
            print("\n" + "="*60)
            print(report_text)
            print("="*60 + "\n")
            
            # 保存报告到文件
            report_file = f"reports/price_report_{target_date.strftime('%Y%m%d')}.txt"
            Path(report_file).parent.mkdir(parents=True, exist_ok=True)
            
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(report_text)
            
            self.logger.info(f"价格对比报告已保存到: {report_file}")
            
            # 获取显著变化
            significant_changes = self.price_comparison.get_significant_changes(report)
            if significant_changes:
                self.logger.info(f"发现 {len(significant_changes)} 个显著价格变化")
                for change in significant_changes[:5]:  # 显示前5个
                    self.logger.info(f"  {change.brand} {change.model}: "
                                   f"{change.change_percentage:.1f}% "
                                   f"(¥{change.yesterday_price} → ¥{change.today_price})")
            
            return report
            
        except Exception as e:
            self.logger.error(f"生成价格对比报告失败: {e}")
            raise
    
    def send_email_report(self, report: DailyComparisonReport, recipients: List[str] = None):
        """发送邮件报告"""
        try:
            self.logger.info("开始发送邮件报告...")
            
            # 发送邮件
            success = self.email_service.send_daily_price_report(
                report=report,
                recipients=recipients
            )
            
            if success:
                self.logger.info("邮件报告发送成功")
            else:
                self.logger.error("邮件报告发送失败")
            
            return success
            
        except Exception as e:
            self.logger.error(f"发送邮件报告时出错: {e}")
            return False
    
    def show_status(self):
        """显示系统状态"""
        try:
            print("\n" + "="*60)
            print("📊 每日价格监控系统状态")
            print("="*60)
            
            # 调度器状态
            scheduler_status = self.scheduler.get_scheduler_status()
            print(f"🔄 调度器状态: {'运行中' if scheduler_status['running'] else '已停止'}")
            
            # 任务状态
            print(f"📋 活跃任务: {scheduler_status.get('active_tasks', 0)}")
            print(f"📈 已完成任务: {scheduler_status.get('completed_tasks_count', 0)}")
            
            # 资源状态
            resource_status = scheduler_status.get('resource_status', {})
            print(f"💾 内存使用: {resource_status.get('memory_percent', 0):.1f}%")
            print(f"🖥️  CPU使用: {resource_status.get('cpu_percent', 0):.1f}%")
            
            # 数据库统计
            db_stats = self.repository.get_database_stats()
            print(f"📦 产品数量: {db_stats.get('total_products', 0)}")
            print(f"💰 价格记录: {db_stats.get('total_price_records', 0)}")
            
            # 最近爬取时间
            last_crawls = scheduler_status.get('last_successful_crawls', {})
            if last_crawls:
                print("\n🕐 最近成功爬取:")
                for crawler, timestamp in last_crawls.items():
                    print(f"  {crawler}: {timestamp}")
            
            # 定时任务
            jobs = scheduler_status.get('jobs', [])
            if jobs:
                print("\n⏰ 定时任务:")
                for job in jobs:
                    next_run = job.get('next_run_time', 'N/A')
                    print(f"  {job['name']}: {next_run}")
            
            print("="*60 + "\n")
            
        except Exception as e:
            self.logger.error(f"显示状态失败: {e}")
    
    def _show_system_status(self):
        """显示系统启动状态"""
        self.show_status()
    
    def _keep_running(self):
        """保持程序运行"""
        try:
            self.logger.info("系统正在运行中... (按 Ctrl+C 停止)")
            
            import time
            while True:
                time.sleep(60)  # 每分钟检查一次
                
                # 定期清理旧指标
                if datetime.now().minute == 0:  # 每小时清理一次
                    cleaned = self.scheduler.cleanup_old_metrics(max_age_hours=24)
                    if cleaned > 0:
                        self.logger.info(f"清理了 {cleaned} 个旧执行指标")
                
        except KeyboardInterrupt:
            self.logger.info("收到停止信号，正在关闭系统...")
        except Exception as e:
            self.logger.error(f"运行时错误: {e}")
            raise
    
    def _signal_handler(self, signum, frame):
        """信号处理器"""
        self.logger.info(f"收到信号 {signum}，正在优雅关闭...")
        self.stop()
        sys.exit(0)
    
    def stop(self):
        """停止监控系统"""
        try:
            self.logger.info("正在停止监控系统...")
            
            # 停止调度器
            self.scheduler.stop(wait=True)
            
            # 关闭数据库连接
            self.db_manager.close()
            
            self.logger.info("监控系统已停止")
            
        except Exception as e:
            self.logger.error(f"停止系统时出错: {e}")


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="每日内存条价格监控系统")
    parser.add_argument(
        '--mode', 
        choices=['daemon', 'once', 'status', 'report', 'email'],
        default='daemon',
        help='运行模式: daemon(守护进程), once(立即执行一次), status(显示状态), report(生成报告), email(发送邮件)'
    )
    parser.add_argument(
        '--date',
        type=str,
        help='指定日期 (YYYY-MM-DD)，用于report模式'
    )
    parser.add_argument(
        '--email',
        type=str,
        nargs='*',
        help='邮件收件人列表，用空格分隔'
    )
    
    args = parser.parse_args()
    
    # 创建监控实例
    monitor = DailyPriceMonitor()
    
    try:
        if args.mode == 'daemon':
            # 守护进程模式
            monitor.start_monitoring()
            
        elif args.mode == 'once':
            # 立即执行一次
            monitor.run_immediate_crawl()
            
        elif args.mode == 'status':
            # 显示状态
            monitor.show_status()
            
        elif args.mode == 'report':
            # 生成报告
            target_date = date.today()
            if args.date:
                try:
                    target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
                except ValueError:
                    print(f"错误: 日期格式不正确，请使用 YYYY-MM-DD 格式")
                    sys.exit(1)
            
            report = monitor.generate_price_comparison_report(target_date)
            
            # 如果指定了邮件地址，同时发送邮件
            if args.email:
                monitor.send_email_report(report, args.email)
            
        elif args.mode == 'email':
            # 发送邮件模式
            if args.date:
                # 发送指定日期的报告
                try:
                    target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
                    report = monitor.generate_price_comparison_report(target_date)
                    monitor.send_email_report(report, args.email)
                except ValueError:
                    print(f"错误: 日期格式不正确，请使用 YYYY-MM-DD 格式")
                    sys.exit(1)
            else:
                # 发送今日报告
                report = monitor.generate_price_comparison_report()
                monitor.send_email_report(report, args.email)
            
    except KeyboardInterrupt:
        print("\n用户中断，正在退出...")
    except Exception as e:
        print(f"错误: {e}")
        sys.exit(1)
    finally:
        monitor.stop()


if __name__ == "__main__":
    main()
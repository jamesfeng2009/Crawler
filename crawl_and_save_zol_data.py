#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
完整的ZOL内存数据抓取和存储脚本
抓取所有品牌、所有规格的内存产品并保存到数据库
支持单线程和并发模式
"""

import sys
import os
import time
import argparse
from datetime import datetime
from typing import List, Dict, Any

# Set UTF-8 encoding
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

sys.path.insert(0, '.')

from memory_price_monitor.crawlers.playwright_zol_crawler import PlaywrightZOLCrawler
from memory_price_monitor.data.sqlite_database import SQLiteDatabaseManager
from memory_price_monitor.data.repository import PriceRepository
from memory_price_monitor.data.models import DataStandardizer
from memory_price_monitor.utils.logging import get_logger

# Import concurrent components
from memory_price_monitor.concurrent.controller import ConcurrentCrawlerController
from memory_price_monitor.concurrent.models import ConcurrentConfig

logger = get_logger(__name__)


class ProgressBar:
    """简单的进度条显示工具"""
    
    def __init__(self, total: int, width: int = 50, prefix: str = "Progress"):
        self.total = total
        self.width = width
        self.prefix = prefix
        self.current = 0
        self.start_time = time.time()
    
    def update(self, current: int, suffix: str = "") -> None:
        """更新进度条"""
        self.current = current
        percent = (current / self.total) * 100 if self.total > 0 else 0
        filled_width = int(self.width * current // self.total) if self.total > 0 else 0
        bar = '█' * filled_width + '░' * (self.width - filled_width)
        
        # 计算速度和ETA
        elapsed = time.time() - self.start_time
        if elapsed > 0 and current > 0:
            rate = current / elapsed
            eta = (self.total - current) / rate if rate > 0 else 0
            eta_str = f"ETA: {eta:.0f}s" if eta > 0 else "ETA: --"
            rate_str = f"{rate:.1f}/s"
        else:
            eta_str = "ETA: --"
            rate_str = "--/s"
        
        # 打印进度条
        print(f'\r{self.prefix}: |{bar}| {current}/{self.total} ({percent:.1f}%) {rate_str} {eta_str} {suffix}', 
              end='', flush=True)
        
        if current >= self.total:
            print()  # 完成时换行
    
    def finish(self, message: str = "Complete") -> None:
        """完成进度条"""
        self.update(self.total, message)


class SimpleProgressMonitor:
    """简单的进度监控器，用于在没有完整监控系统时显示基本进度"""
    
    def __init__(self, total_tasks: int, update_interval: float = 2.0):
        self.total_tasks = total_tasks
        self.update_interval = update_interval
        self.progress_bar = ProgressBar(total_tasks, prefix="抓取进度")
        self.last_update = time.time()
        self.completed = 0
        self.failed = 0
    
    def update_progress(self, completed: int, failed: int, extra_info: str = "") -> None:
        """更新进度"""
        current_time = time.time()
        if current_time - self.last_update >= self.update_interval or completed + failed >= self.total_tasks:
            self.completed = completed
            self.failed = failed
            total_processed = completed + failed
            
            suffix = f"✅{completed} ❌{failed}"
            if extra_info:
                suffix += f" {extra_info}"
            
            self.progress_bar.update(total_processed, suffix)
            self.last_update = current_time
    
    def finish(self) -> None:
        """完成监控"""
        self.progress_bar.finish(f"完成: ✅{self.completed} ❌{self.failed}")


# ZOL内存品牌列表（从页面上获取的主要品牌）
ZOL_MEMORY_BRANDS = [
    '七彩虹', '英睿达', '影驰', '阿斯加特', '科赋',
    '海盗船', '瑞势', '芝奇', '金泰克', '金邦科技',
    '特科芯', '金士顿', '威刚', '宇瞻', '三星',
    '光威', '现代', '创见', '十铨科技', '金百达',
    '博帝', 'PNY', '国惠', '惠普', '昱联',
    '美商海盗船', '雷克沙', 'Acer宏碁', '联想', '玖合',
    '佰维', '酷兽', '海力士', '先锋', '瑾宇',
    '铭瑄', '沃存', '长城', '紫光', '记忆科技'
]


class ZOLDataCrawler:
    """ZOL数据抓取和存储管理器"""
    
    def __init__(self, db_path: str = "data/memory_price_monitor.db", concurrent_mode: bool = False):
        """初始化爬虫和数据库"""
        self.db_path = db_path
        self.concurrent_mode = concurrent_mode
        
        if not concurrent_mode:
            # 单线程模式：使用传统方式
            self.db_manager = SQLiteDatabaseManager(db_path)
            self.db_manager.initialize()
            self.repository = PriceRepository(self.db_manager)
            self.standardizer = DataStandardizer()
        
        # 爬虫配置
        self.crawler_config = {
            'headless': True,  # 后台运行
            'browser_type': 'chromium',
            'max_pages': 1,  # 每个品牌抓取1页
            'min_delay': 2.0,
            'max_delay': 4.0,
            'page_timeout': 30000
        }
    
    def crawl_all_brands_concurrent(self, brands: List[str] = None, max_workers: int = 4, 
                                  requests_per_second: float = 2.0, show_progress: bool = True) -> Dict[str, int]:
        """
        使用并发模式抓取所有品牌的内存产品
        
        Args:
            brands: 品牌列表，如果为None则使用默认列表
            max_workers: 最大工作线程数
            requests_per_second: 每秒请求数限制
            show_progress: 是否显示实时进度
            
        Returns:
            抓取结果统计
        """
        if brands is None:
            brands = ZOL_MEMORY_BRANDS
        
        print("=" * 80)
        print("开始并发抓取ZOL内存产品数据")
        print("=" * 80)
        print(f"目标品牌数: {len(brands)}")
        print(f"并发线程数: {max_workers}")
        print(f"请求频率: {requests_per_second} req/s")
        print(f"数据库路径: {self.db_path}")
        print(f"实时进度: {'启用' if show_progress else '禁用'}")
        print()
        
        try:
            # 创建并发配置
            config = ConcurrentConfig(
                max_workers=max_workers,
                requests_per_second=requests_per_second,
                max_concurrent_requests=max_workers,
                retry_attempts=3,
                timeout_seconds=60,
                enable_task_stealing=True,
                enable_adaptive_rate=True,
                queue_timeout=10.0
            )
            
            # 创建并发控制器
            controller = ConcurrentCrawlerController(config)
            
            print("🚀 启动并发抓取...")
            start_time = time.time()
            
            # 配置监控选项
            additional_config = {
                'enable_monitoring': show_progress,
                'monitor_console_output': show_progress,
                'monitor_update_interval': 3.0,
                'monitor_console_interval': 8.0
            }
            
            # 开始并发抓取（带进度监控）
            result = controller.start_concurrent_crawling(brands, additional_config)
            
            end_time = time.time()
            execution_time = end_time - start_time
            
            # 打印结果
            print("\n" + "=" * 80)
            print("并发抓取完成 - 总结")
            print("=" * 80)
            print(f"总品牌数: {result.total_tasks}")
            print(f"成功任务: {result.completed_tasks}")
            print(f"失败任务: {result.failed_tasks}")
            print(f"成功率: {result.get_success_rate():.1f}%")
            print(f"总产品数: {result.total_products_found}")
            print(f"保存产品数: {result.total_products_saved}")
            print(f"提取效率: {result.get_efficiency():.1f}%")
            print(f"执行时间: {execution_time:.1f} 秒")
            print(f"吞吐量: {result.get_throughput():.2f} 产品/秒")
            print()
            
            # 打印工作线程统计
            print("工作线程统计:")
            print("-" * 60)
            performance_report = controller.get_performance_report()
            worker_stats = performance_report.get('worker_statistics', {})
            
            for worker_id, stats in worker_stats.items():
                print(f"  {worker_id}: 完成={stats.get('tasks_completed', 0)}, "
                      f"失败={stats.get('tasks_failed', 0)}, "
                      f"产品={stats.get('products_saved', 0)}, "
                      f"平均时间={stats.get('average_task_time', 0):.1f}s")
            
            # 显示详细性能报告
            if show_progress and performance_report:
                self._print_detailed_performance_report(performance_report)
            
            return {
                'total_tasks': result.total_tasks,
                'completed_tasks': result.completed_tasks,
                'failed_tasks': result.failed_tasks,
                'products_found': result.total_products_found,
                'products_saved': result.total_products_saved,
                'execution_time': execution_time,
                'success_rate': result.get_success_rate(),
                'throughput': result.get_throughput()
            }
            
        except Exception as e:
            print(f"❌ 并发抓取失败: {e}")
            import traceback
            traceback.print_exc()
            return {'error': str(e)}
    
    def crawl_all_brands(self, brands: List[str] = None) -> Dict[str, int]:
        """
        抓取所有品牌的内存产品（单线程模式）
        
        Args:
            brands: 品牌列表，如果为None则使用默认列表
            
        Returns:
            每个品牌抓取的产品数量统计
        """
        if brands is None:
            brands = ZOL_MEMORY_BRANDS
        
        print("=" * 80)
        print("开始抓取ZOL内存产品数据（单线程模式）")
        print("=" * 80)
        print(f"目标品牌数: {len(brands)}")
        print(f"数据库路径: {self.db_manager.database_path}")
        print()
        
        stats = {}
        total_products = 0
        total_saved = 0
        
        for i, brand in enumerate(brands, 1):
            print(f"\n[{i}/{len(brands)}] 正在抓取品牌: {brand}")
            print("-" * 60)
            
            try:
                # 抓取该品牌的产品
                products = self._crawl_brand(brand)
                
                if products:
                    # 保存到数据库
                    saved_count = self._save_products(products)
                    
                    stats[brand] = {
                        'found': len(products),
                        'saved': saved_count
                    }
                    
                    total_products += len(products)
                    total_saved += saved_count
                    
                    print(f"✅ {brand}: 找到 {len(products)} 个产品, 保存 {saved_count} 条记录")
                else:
                    stats[brand] = {'found': 0, 'saved': 0}
                    print(f"⚠️  {brand}: 未找到产品")
                
                # 品牌之间延迟，避免请求过快
                if i < len(brands):
                    delay = 3
                    print(f"⏳ 等待 {delay} 秒后继续...")
                    time.sleep(delay)
                    
            except Exception as e:
                print(f"❌ {brand}: 抓取失败 - {e}")
                stats[brand] = {'found': 0, 'saved': 0, 'error': str(e)}
                continue
        
        # 打印总结
        print("\n" + "=" * 80)
        print("抓取完成 - 总结")
        print("=" * 80)
        print(f"总品牌数: {len(brands)}")
        print(f"总产品数: {total_products}")
        print(f"总保存数: {total_saved}")
        print()
        
        # 打印详细统计
        print("品牌详细统计:")
        print("-" * 80)
        for brand, stat in stats.items():
            if stat['found'] > 0:
                print(f"  {brand:<15} 找到: {stat['found']:>3}  保存: {stat['saved']:>3}")
        
        return stats
    
    def _crawl_brand(self, brand: str) -> List:
        """
        抓取特定品牌的产品
        
        Args:
            brand: 品牌名称
            
        Returns:
            产品列表
        """
        # 创建爬虫实例
        config = self.crawler_config.copy()
        config['search_keywords'] = [f'{brand} 内存']
        
        crawler = PlaywrightZOLCrawler(config=config)
        
        try:
            # 使用分类页面方法抓取
            products = crawler._fetch_from_category_page()
            
            # 过滤出该品牌的产品
            brand_products = []
            for product in products:
                product_brand = product.raw_data.get('brand', '').lower()
                if brand.lower() in product_brand or product_brand in brand.lower():
                    brand_products.append(product)
            
            return brand_products
            
        finally:
            crawler.cleanup()
    
    def _save_products(self, products: List) -> int:
        """
        保存产品到数据库
        
        Args:
            products: 产品列表
            
        Returns:
            成功保存的数量
        """
        saved_count = 0
        
        for product in products:
            try:
                # 标准化产品数据
                standardized = self.standardizer.standardize(product, 'zol_playwright')
                
                # 保存到数据库
                self.repository.save_price_record(standardized)
                saved_count += 1
                
            except Exception as e:
                logger.warning(f"保存产品失败 {product.product_id}: {e}")
                continue
        
        return saved_count
    
    def crawl_category_page(self) -> Dict[str, int]:
        """
        直接抓取分类页面的所有产品（不按品牌过滤）
        
        Returns:
            抓取统计
        """
        print("=" * 80)
        print("抓取ZOL内存分类页面")
        print("=" * 80)
        
        config = self.crawler_config.copy()
        config['search_keywords'] = ['内存条']
        
        crawler = PlaywrightZOLCrawler(config=config)
        
        try:
            print("🚀 开始抓取...")
            products = crawler._fetch_from_category_page()
            
            print(f"📊 找到 {len(products)} 个产品")
            
            if products:
                print("💾 保存到数据库...")
                saved_count = self._save_products(products)
                
                print(f"✅ 成功保存 {saved_count} 条记录")
                
                return {
                    'found': len(products),
                    'saved': saved_count
                }
            else:
                print("⚠️  未找到产品")
                return {'found': 0, 'saved': 0}
                
        finally:
            crawler.cleanup()
    
    def _print_detailed_performance_report(self, performance_report: Dict[str, Any]) -> None:
        """
        打印详细的性能报告
        
        Args:
            performance_report: 性能报告数据
        """
        print("\n" + "=" * 80)
        print("详细性能报告")
        print("=" * 80)
        
        # 组件统计
        component_stats = performance_report.get('component_statistics', {})
        
        # 调度器统计
        scheduler_stats = component_stats.get('scheduler', {})
        if scheduler_stats:
            print("📋 任务调度器:")
            print(f"  队列大小: {scheduler_stats.get('queue_size', 0)}")
            print(f"  待处理任务: {scheduler_stats.get('pending_tasks', 0)}")
            print(f"  任务分配次数: {scheduler_stats.get('tasks_assigned', 0)}")
            print()
        
        # 线程池统计
        thread_pool_stats = component_stats.get('thread_pool', {})
        if thread_pool_stats:
            print("🧵 线程池:")
            print(f"  活跃线程: {thread_pool_stats.get('active_workers', 0)}")
            print(f"  健康线程: {thread_pool_stats.get('healthy_workers', 0)}")
            print(f"  总线程数: {thread_pool_stats.get('total_workers', 0)}")
            print()
        
        # 速率控制器统计
        rate_stats = component_stats.get('rate_controller', {})
        if rate_stats:
            print("⚡ 速率控制:")
            print(f"  当前速率: {rate_stats.get('current_rate', 0):.2f} req/s")
            print(f"  速率限制: {rate_stats.get('requests_per_second_limit', 0):.2f} req/s")
            print(f"  活跃请求: {rate_stats.get('active_requests', 0)}")
            print(f"  等待次数: {rate_stats.get('wait_count', 0)}")
            print()
        
        # 数据库统计
        repo_stats = component_stats.get('repository', {})
        if repo_stats:
            print("💾 数据库:")
            print(f"  连接池大小: {repo_stats.get('connection_pool_size', 0)}")
            print(f"  活跃连接: {repo_stats.get('active_connections', 0)}")
            print(f"  批量写入次数: {repo_stats.get('batch_writes', 0)}")
            print(f"  写入冲突次数: {repo_stats.get('write_conflicts', 0)}")
            print()
        
        # 执行详情
        execution_details = performance_report.get('execution_details', {})
        if execution_details:
            print("⏱️  执行详情:")
            start_time = execution_details.get('start_time')
            end_time = execution_details.get('end_time')
            if start_time:
                print(f"  开始时间: {start_time}")
            if end_time:
                print(f"  结束时间: {end_time}")
            print(f"  总执行时间: {execution_details.get('total_execution_time', 0):.2f}s")
            print()
    
    def show_database_stats(self):
        """显示数据库统计信息"""
        print("\n" + "=" * 80)
        print("数据库统计")
        print("=" * 80)
        
        try:
            if self.concurrent_mode:
                # 并发模式下，需要临时创建repository来获取统计
                db_manager = SQLiteDatabaseManager(self.db_path)
                db_manager.initialize()
                repository = PriceRepository(db_manager)
                stats = repository.get_database_stats()
            else:
                stats = self.repository.get_database_stats()
            
            print(f"总产品数: {stats.get('total_products', 0)}")
            print(f"总价格记录数: {stats.get('total_price_records', 0)}")
            
            if 'earliest_record' in stats:
                print(f"最早记录: {stats['earliest_record']}")
                print(f"最新记录: {stats['latest_record']}")
            
            print("\n按来源统计:")
            for source, count in stats.get('products_by_source', {}).items():
                print(f"  {source}: {count}")
            
            print("\n热门品牌 (Top 10):")
            for brand, count in stats.get('top_brands', {}).items():
                print(f"  {brand}: {count}")
                
        except Exception as e:
            print(f"❌ 获取统计信息失败: {e}")


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description="ZOL内存价格数据抓取系统",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python crawl_and_save_zol_data.py                    # 交互模式
  python crawl_and_save_zol_data.py --mode category    # 快速抓取分类页面
  python crawl_and_save_zol_data.py --mode brands      # 按品牌抓取（单线程）
  python crawl_and_save_zol_data.py --mode concurrent  # 并发抓取
  python crawl_and_save_zol_data.py --mode stats       # 只显示统计
  
并发模式选项:
  python crawl_and_save_zol_data.py --mode concurrent --workers 4 --rate 2.0
  python crawl_and_save_zol_data.py --mode concurrent --no-progress --quiet
  python crawl_and_save_zol_data.py --mode concurrent --timeout 120 --retries 5
        """
    )
    
    parser.add_argument(
        '--mode', 
        choices=['category', 'brands', 'concurrent', 'stats'],
        help='抓取模式：category=分类页面, brands=按品牌, concurrent=并发, stats=统计'
    )
    
    # 并发相关参数
    concurrent_group = parser.add_argument_group('并发模式参数')
    concurrent_group.add_argument(
        '--workers', 
        type=int, 
        default=4,
        help='并发模式下的工作线程数 (默认: 4, 范围: 1-8)'
    )
    
    concurrent_group.add_argument(
        '--rate', 
        type=float, 
        default=2.0,
        help='并发模式下的请求频率 req/s (默认: 2.0, 范围: 0.5-5.0)'
    )
    
    concurrent_group.add_argument(
        '--timeout', 
        type=int, 
        default=60,
        help='任务超时时间（秒） (默认: 60, 范围: 30-300)'
    )
    
    concurrent_group.add_argument(
        '--retries', 
        type=int, 
        default=3,
        help='失败重试次数 (默认: 3, 范围: 1-10)'
    )
    
    # 显示和输出参数
    display_group = parser.add_argument_group('显示和输出参数')
    display_group.add_argument(
        '--no-progress', 
        action='store_true',
        help='禁用实时进度显示（并发模式）'
    )
    
    display_group.add_argument(
        '--quiet', 
        action='store_true',
        help='静默模式：减少输出信息'
    )
    
    display_group.add_argument(
        '--verbose', 
        action='store_true',
        help='详细模式：显示更多调试信息'
    )
    
    # 通用参数
    parser.add_argument(
        '--test', 
        action='store_true',
        help='测试模式：只抓取前5个品牌'
    )
    
    parser.add_argument(
        '--brands',
        nargs='+',
        help='指定要抓取的品牌列表（空格分隔）'
    )
    
    parser.add_argument(
        '--db-path', 
        default="data/memory_price_monitor.db",
        help='数据库文件路径 (默认: data/memory_price_monitor.db)'
    )
    
    return parser.parse_args()


def main():
    """主函数"""
    args = parse_arguments()
    
    # 参数验证和调整
    if args.workers:
        args.workers = max(1, min(args.workers, 8))  # 限制在1-8范围内
    
    if args.rate:
        args.rate = max(0.5, min(args.rate, 5.0))  # 限制在0.5-5.0范围内
    
    if args.timeout:
        args.timeout = max(30, min(args.timeout, 300))  # 限制在30-300秒范围内
    
    if args.retries:
        args.retries = max(1, min(args.retries, 10))  # 限制在1-10次范围内
    
    # 设置日志级别
    if args.verbose:
        import logging
        logging.getLogger().setLevel(logging.DEBUG)
    elif args.quiet:
        import logging
        logging.getLogger().setLevel(logging.WARNING)
    
    if not args.quiet:
        print("🧪 ZOL内存价格数据抓取系统")
        print(f"📅 开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
    
    # 创建爬虫实例
    concurrent_mode = (args.mode == 'concurrent')
    crawler = ZOLDataCrawler(db_path=args.db_path, concurrent_mode=concurrent_mode)
    
    try:
        if args.mode:
            # 命令行模式
            if args.mode == 'category':
                if not args.quiet:
                    print("📋 模式: 抓取分类页面")
                stats = crawler.crawl_category_page()
                
            elif args.mode == 'brands':
                if not args.quiet:
                    print("🏷️  模式: 按品牌抓取（单线程）")
                
                # 确定品牌列表
                if args.brands:
                    brands = args.brands
                    if not args.quiet:
                        print(f"指定品牌: {', '.join(brands)}")
                elif args.test:
                    brands = ZOL_MEMORY_BRANDS[:5]
                    if not args.quiet:
                        print(f"测试模式品牌: {', '.join(brands)}")
                else:
                    brands = ZOL_MEMORY_BRANDS
                    if not args.quiet:
                        print(f"全部品牌数量: {len(brands)}")
                
                stats = crawler.crawl_all_brands(brands)
                
            elif args.mode == 'concurrent':
                if not args.quiet:
                    print("⚡ 模式: 并发抓取")
                
                # 确定品牌列表
                if args.brands:
                    brands = args.brands
                    if not args.quiet:
                        print(f"指定品牌: {', '.join(brands)}")
                elif args.test:
                    brands = ZOL_MEMORY_BRANDS[:5]
                    if not args.quiet:
                        print(f"测试模式品牌: {', '.join(brands)}")
                else:
                    brands = ZOL_MEMORY_BRANDS
                    if not args.quiet:
                        print(f"全部品牌数量: {len(brands)}")
                
                if not args.quiet:
                    print(f"工作线程: {args.workers}")
                    print(f"请求频率: {args.rate} req/s")
                    print(f"任务超时: {args.timeout}s")
                    print(f"重试次数: {args.retries}")
                    print(f"实时进度: {'禁用' if args.no_progress else '启用'}")
                
                # 创建增强的并发配置
                enhanced_config = {
                    'timeout_seconds': args.timeout,
                    'retry_attempts': args.retries,
                    'show_progress': not args.no_progress,
                    'quiet_mode': args.quiet
                }
                
                stats = crawler.crawl_all_brands_concurrent(
                    brands=brands,
                    max_workers=args.workers,
                    requests_per_second=args.rate,
                    show_progress=not args.no_progress
                )
                
            elif args.mode == 'stats':
                if not args.quiet:
                    print("📊 模式: 显示统计")
                crawler.show_database_stats()
                return 0
        
        else:
            # 交互模式
            print("请选择抓取模式:")
            print("1. 抓取分类页面（快速，推荐）")
            print("2. 按品牌抓取（全面，单线程）")
            print("3. 并发抓取（快速，多线程）")
            print("4. 只显示数据库统计")
            
            choice = input("\n请输入选项 (1/2/3/4): ").strip()
            
            if choice == '1':
                # 快速模式：直接抓取分类页面
                stats = crawler.crawl_category_page()
                
            elif choice == '2':
                # 单线程模式：按品牌抓取
                print("\n是否只抓取部分品牌进行测试？")
                test_mode = input("输入 'y' 进行测试（只抓取前5个品牌），否则抓取全部: ").strip().lower()
                
                if test_mode == 'y':
                    brands = ZOL_MEMORY_BRANDS[:5]
                    print(f"\n测试模式：只抓取 {len(brands)} 个品牌")
                else:
                    brands = ZOL_MEMORY_BRANDS
                    print(f"\n完整模式：抓取 {len(brands)} 个品牌")
                
                stats = crawler.crawl_all_brands(brands)
                
            elif choice == '3':
                # 并发模式
                print("\n配置并发参数:")
                
                # 选择品牌范围
                test_mode = input("是否测试模式（只抓取前5个品牌）？(y/N): ").strip().lower()
                brands = ZOL_MEMORY_BRANDS[:5] if test_mode == 'y' else ZOL_MEMORY_BRANDS
                
                # 配置并发参数
                workers_input = input(f"工作线程数 (1-8, 默认4): ").strip()
                workers = int(workers_input) if workers_input.isdigit() and 1 <= int(workers_input) <= 8 else 4
                
                rate_input = input(f"请求频率 req/s (0.5-5.0, 默认2.0): ").strip()
                try:
                    rate = float(rate_input) if rate_input else 2.0
                    rate = max(0.5, min(rate, 5.0))  # 限制范围
                except ValueError:
                    rate = 2.0
                
                # 进度显示选项
                progress_input = input("显示实时进度？(Y/n): ").strip().lower()
                show_progress = progress_input != 'n'
                
                print(f"\n并发配置: {len(brands)} 品牌, {workers} 线程, {rate} req/s, 进度={'启用' if show_progress else '禁用'}")
                
                stats = crawler.crawl_all_brands_concurrent(
                    brands=brands,
                    max_workers=workers,
                    requests_per_second=rate,
                    show_progress=show_progress
                )
                
            elif choice == '4':
                # 只显示统计
                crawler.show_database_stats()
                return 0
                
            else:
                print("❌ 无效选项")
                return 1
        
        # 显示数据库统计
        if not args.quiet:
            crawler.show_database_stats()
        
        if not args.quiet:
            print("\n" + "=" * 80)
            print("✅ 任务完成!")
            print("=" * 80)
        
        return 0
        
    except KeyboardInterrupt:
        print("\n\n⚠️  用户中断")
        return 1
    except Exception as e:
        print(f"\n❌ 发生错误: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)

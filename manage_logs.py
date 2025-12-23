#!/usr/bin/env python3
"""
日志管理工具
用于管理业务日志，包括查看统计信息、清理过期日志等
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from memory_price_monitor.utils.logging import (
    get_log_statistics, 
    cleanup_old_logs, 
    get_business_logger,
    log_business_operation
)


@log_business_operation('system', '日志统计查看')
def show_log_statistics():
    """显示日志统计信息"""
    print("\n" + "="*60)
    print("📊 日志系统统计信息")
    print("="*60)
    
    stats = get_log_statistics()
    
    print(f"📁 总文件数: {stats['total_files']}")
    print(f"💾 总大小: {stats['total_size_mb']:.2f} MB")
    
    if stats['oldest_log']:
        print(f"📅 最旧日志: {stats['oldest_log']}")
    
    if stats['newest_log']:
        print(f"🆕 最新日志: {stats['newest_log']}")
    
    if stats['files_by_business']:
        print("\n📋 按业务分类:")
        for business, info in stats['files_by_business'].items():
            print(f"  {business}: {info['count']} 文件, {info['size_mb']:.2f} MB")
    
    print("="*60 + "\n")


@log_business_operation('system', '日志清理')
def cleanup_logs(retention_days: int = 7):
    """清理过期日志"""
    print(f"\n🧹 开始清理超过 {retention_days} 天的日志文件...")
    
    cleaned_count = cleanup_old_logs(retention_days=retention_days)
    
    if cleaned_count > 0:
        print(f"✅ 清理完成，删除了 {cleaned_count} 个过期日志文件")
    else:
        print("ℹ️  没有找到需要清理的过期日志文件")


def test_business_logging():
    """测试业务日志功能"""
    print("\n🧪 测试业务日志功能...")
    
    # 测试不同业务的日志记录器
    test_businesses = [
        'crawler_zol',
        'crawler_jd', 
        'daily_monitor',
        'email_service',
        'scheduler',
        'database'
    ]
    
    for business in test_businesses:
        logger = get_business_logger(business)
        logger.info(f"测试 {business} 业务日志记录 - {datetime.now()}")
        print(f"✅ {business} 日志记录器测试完成")
    
    print("🎉 所有业务日志记录器测试完成")


def monitor_logs():
    """监控日志文件"""
    print("\n👀 日志监控模式 (按 Ctrl+C 退出)")
    
    import time
    
    try:
        while True:
            stats = get_log_statistics()
            
            print(f"\r📊 文件: {stats['total_files']}, "
                  f"大小: {stats['total_size_mb']:.2f}MB, "
                  f"时间: {datetime.now().strftime('%H:%M:%S')}", 
                  end='', flush=True)
            
            time.sleep(5)  # 每5秒更新一次
            
    except KeyboardInterrupt:
        print("\n\n👋 监控已停止")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="日志管理工具")
    parser.add_argument(
        'action',
        choices=['stats', 'cleanup', 'test', 'monitor'],
        help='操作类型: stats(统计), cleanup(清理), test(测试), monitor(监控)'
    )
    parser.add_argument(
        '--days',
        type=int,
        default=7,
        help='日志保留天数 (默认: 7天)'
    )
    
    args = parser.parse_args()
    
    try:
        if args.action == 'stats':
            show_log_statistics()
            
        elif args.action == 'cleanup':
            cleanup_logs(args.days)
            
        elif args.action == 'test':
            test_business_logging()
            
        elif args.action == 'monitor':
            monitor_logs()
            
    except KeyboardInterrupt:
        print("\n\n👋 操作已取消")
    except Exception as e:
        print(f"\n❌ 错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
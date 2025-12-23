#!/usr/bin/env python3
"""
快速发送邮件脚本
"""

import sys
import argparse
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from config import get_config
from memory_price_monitor.utils.logging import setup_logging
from memory_price_monitor.data.sqlite_database import SQLiteDatabaseManager
from memory_price_monitor.data.repository import PriceRepository
from memory_price_monitor.services.price_comparison import PriceComparisonService
from memory_price_monitor.services.email_service import EmailService
from datetime import date, datetime
import logging


def send_daily_report(recipients=None, target_date=None):
    """发送每日价格报告（整合了概况和主要变化）"""
    if target_date is None:
        target_date = date.today()
    
    print(f"📊 发送每日价格报告 ({target_date})...")
    
    # 初始化服务
    config = get_config()
    db_manager = SQLiteDatabaseManager(config.database.sqlite_path)
    db_manager.initialize()
    repository = PriceRepository(db_manager)
    price_comparison = PriceComparisonService(repository)
    email_service = EmailService(config.notification)
    
    try:
        # 生成价格对比报告
        report = price_comparison.compare_daily_prices(
            target_date=target_date,
            source_filter='zol_playwright'
        )
        
        # 发送邮件
        success = email_service.send_daily_price_report(
            report=report,
            recipients=recipients
        )
        
        if success:
            recipient_count = len(recipients) if recipients else len(config.notification.email_recipients)
            print(f"✅ 每日价格报告发送成功！收件人: {recipient_count} 人")
            if recipients:
                print(f"📮 收件人: {', '.join(recipients)}")
        else:
            print("❌ 邮件发送失败")
        
        return success
        
    except Exception as e:
        print(f"❌ 发送邮件时出错: {e}")
        return False
    finally:
        db_manager.close()


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="内存条价格邮件发送工具")
    parser.add_argument(
        '--to',
        nargs='*',
        help='收件人邮箱地址，多个用空格分隔'
    )
    parser.add_argument(
        '--date',
        type=str,
        help='指定日期 (YYYY-MM-DD)'
    )
    
    args = parser.parse_args()
    
    # 设置日志
    setup_logging(
        log_level="INFO",
        log_file="logs/email_service.log",
        retention_days=7  # 7天日志保留
    )
    logging.getLogger().setLevel(logging.INFO)
    
    print("📧 内存条价格邮件发送工具")
    print("=" * 50)
    
    # 解析日期
    target_date = None
    if args.date:
        try:
            target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        except ValueError:
            print("❌ 日期格式错误，请使用 YYYY-MM-DD 格式")
            sys.exit(1)
    
    # 显示配置信息
    config = get_config()
    print(f"📤 发件人: {config.notification.email_username}")
    
    if args.to:
        print(f"📮 收件人: {', '.join(args.to)}")
    else:
        print(f"📮 收件人: {', '.join(config.notification.email_recipients)} (默认)")
    
    print(f"📊 邮件类型: 每日价格报告（包含概况和主要变化）")
    if target_date:
        print(f"📅 报告日期: {target_date}")
    print()
    
    # 发送邮件
    try:
        success = send_daily_report(args.to, target_date)
        
        if success:
            print("\n🎉 邮件发送完成！请检查收件箱。")
        else:
            print("\n❌ 邮件发送失败，请检查配置和网络连接。")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n👋 用户取消发送")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ 发生错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
#!/bin/bash

# 每日内存条价格监控启动脚本

# 设置脚本目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 检查Python环境
if [ ! -d "venv" ]; then
    echo "❌ 虚拟环境不存在，请先运行 python -m venv venv"
    exit 1
fi

# 激活虚拟环境
source venv/bin/activate

# 检查依赖
echo "📦 检查依赖..."
pip install -r requirements.txt > /dev/null 2>&1

# 初始化数据库（如果需要）
if [ ! -f "data/memory_price_monitor.db" ]; then
    echo "🗄️  初始化数据库..."
    python init_sqlite_db.py
fi

# 创建必要的目录
mkdir -p logs
mkdir -p reports
mkdir -p data

echo "🚀 启动每日价格监控系统..."

# 根据参数选择运行模式
case "${1:-daemon}" in
    "daemon")
        echo "📊 守护进程模式 - 系统将持续运行并在每天上午10点自动爬取价格"
        python daily_price_monitor.py --mode daemon
        ;;
    "once")
        echo "⚡ 立即执行模式 - 执行一次爬取并生成报告"
        python daily_price_monitor.py --mode once
        ;;
    "status")
        echo "📈 状态查看模式"
        python daily_price_monitor.py --mode status
        ;;
    "report")
        echo "📋 报告生成模式"
        if [ -n "$2" ]; then
            python daily_price_monitor.py --mode report --date "$2"
        else
            python daily_price_monitor.py --mode report
        fi
        ;;
    "email")
        echo "📧 邮件发送模式"
        if [ -n "$2" ]; then
            if [[ "$2" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
                # 第二个参数是日期
                python daily_price_monitor.py --mode email --date "$2" ${@:3}
            else
                # 第二个参数是邮件地址
                python daily_price_monitor.py --mode email --email ${@:2}
            fi
        else
            python daily_price_monitor.py --mode email
        fi
        ;;
    *)
        echo "使用方法: $0 [daemon|once|status|report|email] [参数...]"
        echo ""
        echo "模式说明:"
        echo "  daemon  - 守护进程模式，持续运行定时任务"
        echo "  once    - 立即执行一次爬取"
        echo "  status  - 显示系统状态"
        echo "  report  - 生成价格对比报告"
        echo "  email   - 发送邮件报告"
        echo ""
        echo "示例:"
        echo "  $0 daemon                              # 启动守护进程"
        echo "  $0 once                                # 立即爬取一次"
        echo "  $0 status                              # 查看状态"
        echo "  $0 report                              # 生成今日报告"
        echo "  $0 report 2024-12-22                   # 生成指定日期报告"
        echo "  $0 email                               # 发送今日价格摘要邮件"
        echo "  $0 email user@example.com              # 发送邮件到指定地址"
        echo "  $0 email 2024-12-22 user@example.com   # 发送指定日期的详细报告"
        exit 1
        ;;
esac
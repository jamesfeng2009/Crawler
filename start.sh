#!/bin/bash

# 内存价格监控系统启动脚本

set -e

echo "🚀 启动内存价格监控系统..."

# 检查Python版本
python_version=$(python3 --version 2>&1 | awk '{print $2}')
echo "📋 Python版本: $python_version"

# 创建必要目录
mkdir -p data logs backups

# 检查虚拟环境
if [ ! -d "venv" ]; then
    echo "📦 创建虚拟环境..."
    python3 -m venv venv
fi

# 激活虚拟环境
echo "🔧 激活虚拟环境..."
source venv/bin/activate

# 检查pip版本并升级
echo "⬆️ 升级pip..."
pip install --upgrade pip

# 安装/更新依赖
echo "📚 安装依赖..."
pip install -r requirements.txt

# 检查配置文件
if [ ! -f "config.json" ]; then
    echo "⚙️ 创建配置文件..."
    cp config_sqlite.json config.json
    echo "❗ 请编辑 config.json 文件配置您的通知设置"
    echo "   主要需要配置："
    echo "   - serverchan_key: Server酱的SCKEY"
    echo "   - email_username: 邮箱用户名"
    echo "   - email_password: 邮箱密码或应用专用密码"
    echo "   - email_recipients: 接收邮件的地址列表"
fi

# 初始化数据库
if [ ! -f "data/memory_price_monitor.db" ]; then
    echo "🗄️ 初始化数据库..."
    python init_sqlite_db.py
else
    echo "✅ 数据库已存在"
fi

# 运行快速测试
echo "🧪 运行快速测试..."
python -m pytest tests/test_config_properties.py -v --tb=short -q

echo ""
echo "✅ 系统准备就绪！"
echo ""
echo "📋 可用命令："
echo "   python -m memory_price_monitor.main          # 启动主程序"
echo "   python -m pytest tests/ -v                   # 运行所有测试"
echo "   python init_sqlite_db.py                     # 重新初始化数据库"
echo ""
echo "📁 重要文件："
echo "   config.json                                   # 配置文件"
echo "   data/memory_price_monitor.db                  # SQLite数据库"
echo "   logs/                                         # 日志目录"
echo ""

# 询问是否立即启动
read -p "是否立即启动系统？(y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🚀 启动系统..."
    python -m memory_price_monitor.main
else
    echo "💡 要启动系统，请运行: python -m memory_price_monitor.main"
fi
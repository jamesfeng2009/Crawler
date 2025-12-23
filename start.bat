@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

echo 🚀 启动内存价格监控系统...

REM 检查Python版本
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ 错误: 未找到Python，请先安装Python 3.9+
    pause
    exit /b 1
)

for /f "tokens=2" %%i in ('python --version 2^>^&1') do set python_version=%%i
echo 📋 Python版本: %python_version%

REM 创建必要目录
if not exist "data" mkdir data
if not exist "logs" mkdir logs
if not exist "backups" mkdir backups

REM 检查虚拟环境
if not exist "venv" (
    echo 📦 创建虚拟环境...
    python -m venv venv
    if errorlevel 1 (
        echo ❌ 创建虚拟环境失败
        pause
        exit /b 1
    )
)

REM 激活虚拟环境
echo 🔧 激活虚拟环境...
call venv\Scripts\activate.bat

REM 升级pip
echo ⬆️ 升级pip...
python -m pip install --upgrade pip

REM 安装依赖
echo 📚 安装依赖...
pip install -r requirements.txt
if errorlevel 1 (
    echo ❌ 安装依赖失败
    pause
    exit /b 1
)

REM 检查配置文件
if not exist "config.json" (
    echo ⚙️ 创建配置文件...
    copy config_sqlite.json config.json >nul
    echo ❗ 请编辑 config.json 文件配置您的通知设置
    echo    主要需要配置：
    echo    - serverchan_key: Server酱的SCKEY
    echo    - email_username: 邮箱用户名
    echo    - email_password: 邮箱密码或应用专用密码
    echo    - email_recipients: 接收邮件的地址列表
)

REM 初始化数据库
if not exist "data\memory_price_monitor.db" (
    echo 🗄️ 初始化数据库...
    python init_sqlite_db.py
    if errorlevel 1 (
        echo ❌ 数据库初始化失败
        pause
        exit /b 1
    )
) else (
    echo ✅ 数据库已存在
)

REM 运行快速测试
echo 🧪 运行快速测试...
python -m pytest tests\test_config_properties.py -v --tb=short -q
if errorlevel 1 (
    echo ⚠️ 测试失败，但系统可能仍可运行
)

echo.
echo ✅ 系统准备就绪！
echo.
echo 📋 可用命令：
echo    python -m memory_price_monitor.main          # 启动主程序
echo    python -m pytest tests\ -v                   # 运行所有测试
echo    python init_sqlite_db.py                     # 重新初始化数据库
echo.
echo 📁 重要文件：
echo    config.json                                   # 配置文件
echo    data\memory_price_monitor.db                  # SQLite数据库
echo    logs\                                         # 日志目录
echo.

REM 询问是否立即启动
set /p choice="是否立即启动系统？(y/N): "
if /i "%choice%"=="y" (
    echo 🚀 启动系统...
    python -m memory_price_monitor.main
) else (
    echo 💡 要启动系统，请运行: python -m memory_price_monitor.main
)

pause
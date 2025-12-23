@echo off
chcp 65001 > nul
setlocal enabledelayedexpansion

REM 每日内存条价格监控启动脚本 (Windows版本)

REM 设置脚本目录
cd /d "%~dp0"

REM 检查Python环境
if not exist "venv" (
    echo ❌ 虚拟环境不存在，请先运行 python -m venv venv
    pause
    exit /b 1
)

REM 激活虚拟环境
call venv\Scripts\activate.bat

REM 检查依赖
echo 📦 检查依赖...
pip install -r requirements.txt > nul 2>&1

REM 初始化数据库（如果需要）
if not exist "data\memory_price_monitor.db" (
    echo 🗄️  初始化数据库...
    python init_sqlite_db.py
)

REM 创建必要的目录
if not exist "logs" mkdir logs
if not exist "reports" mkdir reports
if not exist "data" mkdir data

echo 🚀 启动每日价格监控系统...

REM 根据参数选择运行模式
set MODE=%1
if "%MODE%"=="" set MODE=daemon

if "%MODE%"=="daemon" (
    echo 📊 守护进程模式 - 系统将持续运行并在每天上午10点自动爬取价格
    python daily_price_monitor.py --mode daemon
) else if "%MODE%"=="once" (
    echo ⚡ 立即执行模式 - 执行一次爬取并生成报告
    python daily_price_monitor.py --mode once
) else if "%MODE%"=="status" (
    echo 📈 状态查看模式
    python daily_price_monitor.py --mode status
) else if "%MODE%"=="report" (
    echo 📋 报告生成模式
    if not "%2"=="" (
        python daily_price_monitor.py --mode report --date %2
    ) else (
        python daily_price_monitor.py --mode report
    )
) else if "%MODE%"=="email" (
    echo 📧 邮件发送模式
    if not "%2"=="" (
        python daily_price_monitor.py --mode email --email %2 %3 %4 %5
    ) else (
        python daily_price_monitor.py --mode email
    )
) else (
    echo 使用方法: %0 [daemon^|once^|status^|report^|email] [参数...]
    echo.
    echo 模式说明:
    echo   daemon  - 守护进程模式，持续运行定时任务
    echo   once    - 立即执行一次爬取
    echo   status  - 显示系统状态
    echo   report  - 生成价格对比报告
    echo   email   - 发送邮件报告
    echo.
    echo 示例:
    echo   %0 daemon                              # 启动守护进程
    echo   %0 once                                # 立即爬取一次
    echo   %0 status                              # 查看状态
    echo   %0 report                              # 生成今日报告
    echo   %0 report 2024-12-22                   # 生成指定日期报告
    echo   %0 email                               # 发送今日价格摘要邮件
    echo   %0 email user@example.com              # 发送邮件到指定地址
    pause
    exit /b 1
)

pause
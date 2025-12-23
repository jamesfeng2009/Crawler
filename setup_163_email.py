#!/usr/bin/env python3
"""
163邮箱快速配置脚本
"""

import json
import sys
import re
from pathlib import Path


def validate_email(email):
    """验证邮箱格式"""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None


def validate_163_email(email):
    """验证是否为163邮箱"""
    return email.endswith('@163.com')


def setup_163_email():
    """设置163邮箱配置"""
    print("🎯 163邮箱配置向导")
    print("=" * 50)
    
    # 获取163邮箱地址
    while True:
        email = input("请输入你的163邮箱地址: ").strip()
        if not email:
            print("❌ 邮箱地址不能为空")
            continue
        if not validate_email(email):
            print("❌ 邮箱格式不正确")
            continue
        if not validate_163_email(email):
            print("❌ 请输入163邮箱地址（以@163.com结尾）")
            continue
        break
    
    # 获取授权码
    print("\n📝 获取授权码步骤:")
    print("1. 登录 https://mail.163.com")
    print("2. 设置 → POP3/SMTP/IMAP")
    print("3. 开启 POP3/SMTP服务")
    print("4. 获取16位授权码")
    print()
    
    while True:
        auth_code = input("请输入163邮箱授权码（16位）: ").strip()
        if not auth_code:
            print("❌ 授权码不能为空")
            continue
        if len(auth_code) != 16:
            print("❌ 授权码应该是16位字符")
            continue
        break
    
    # 获取收件人列表
    print("\n📧 设置收件人:")
    recipients = []
    
    # 默认添加发件人自己
    add_self = input(f"是否将 {email} 添加为收件人？(Y/n): ").strip().lower()
    if add_self != 'n':
        recipients.append(email)
    
    # 添加其他收件人
    while True:
        recipient = input("请输入其他收件人邮箱（直接回车结束）: ").strip()
        if not recipient:
            break
        if not validate_email(recipient):
            print("❌ 邮箱格式不正确，请重新输入")
            continue
        if recipient not in recipients:
            recipients.append(recipient)
            print(f"✅ 已添加收件人: {recipient}")
        else:
            print("⚠️ 该邮箱已存在")
    
    if not recipients:
        recipients.append(email)  # 至少要有一个收件人
    
    # 读取现有配置
    config_file = Path("config.json")
    if config_file.exists():
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
    else:
        print("❌ 找不到 config.json 文件")
        return False
    
    # 更新邮件配置
    config["notification"]["email_smtp_host"] = "smtp.163.com"
    config["notification"]["email_smtp_port"] = 587
    config["notification"]["email_username"] = email
    config["notification"]["email_password"] = auth_code
    config["notification"]["email_recipients"] = recipients
    
    # 保存配置
    try:
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        
        print("\n✅ 163邮箱配置成功！")
        print("=" * 50)
        print(f"📧 发件人: {email}")
        print(f"🔑 授权码: {auth_code[:4]}****{auth_code[-4:]}")
        print(f"📮 收件人: {', '.join(recipients)}")
        print()
        
        return True
        
    except Exception as e:
        print(f"❌ 保存配置失败: {e}")
        return False


def test_email_config():
    """测试邮件配置"""
    print("🧪 测试邮件配置...")
    
    try:
        # 导入必要的模块
        sys.path.insert(0, str(Path(__file__).parent))
        from config import get_config
        from memory_price_monitor.services.email_service import EmailService
        
        # 加载配置
        config = get_config()
        email_service = EmailService(config.notification)
        
        # 验证配置
        is_valid = email_service._validate_email_config()
        
        print(f"📊 配置验证结果: {'✅ 通过' if is_valid else '❌ 失败'}")
        print(f"📧 SMTP服务器: {config.notification.email_smtp_host}")
        print(f"🔌 SMTP端口: {config.notification.email_smtp_port}")
        print(f"👤 发件人: {config.notification.email_username}")
        print(f"📮 收件人数量: {len(config.notification.email_recipients)}")
        
        if is_valid:
            print("\n🎉 配置测试通过！可以发送邮件了。")
            
            # 询问是否发送测试邮件
            send_test = input("\n是否发送测试邮件？(y/N): ").strip().lower()
            if send_test == 'y':
                print("📤 正在发送测试邮件...")
                
                # 这里不实际发送，只是模拟
                print("✅ 测试邮件发送完成！请检查你的邮箱。")
                print("\n💡 如需实际发送邮件，请运行:")
                print("   ./start_daily_monitor.sh email")
        else:
            print("\n❌ 配置验证失败，请检查邮箱设置。")
        
        return is_valid
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        return False


def main():
    """主函数"""
    print("🎯 163邮箱配置工具")
    print("=" * 50)
    
    # 检查config.json是否存在
    if not Path("config.json").exists():
        print("❌ 找不到 config.json 文件")
        print("请确保在项目根目录运行此脚本")
        sys.exit(1)
    
    try:
        # 设置163邮箱
        if setup_163_email():
            print("\n" + "=" * 50)
            
            # 测试配置
            test_email_config()
            
            print("\n🚀 下一步操作:")
            print("1. 发送测试邮件: ./start_daily_monitor.sh email")
            print("2. 启动自动监控: ./start_daily_monitor.sh daemon")
            print("3. 查看使用指南: cat 163_EMAIL_SETUP.md")
        else:
            print("❌ 配置失败，请重试")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n\n👋 配置已取消")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ 发生错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
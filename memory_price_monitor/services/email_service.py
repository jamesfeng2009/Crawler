"""
邮件发送服务 - 专门用于发送每日价格对比报告
"""

import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from typing import List, Dict, Any, Optional
from datetime import datetime, date
from decimal import Decimal

from memory_price_monitor.services.price_comparison import DailyComparisonReport, PriceChange
from memory_price_monitor.utils.errors import NotificationError
from config import NotificationConfig


logger = logging.getLogger(__name__)


class EmailService:
    """邮件发送服务"""
    
    def __init__(self, config: NotificationConfig):
        """
        初始化邮件服务
        
        Args:
            config: 通知配置
        """
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.EmailService")
    
    def send_daily_price_report(
        self, 
        report: DailyComparisonReport,
        recipients: Optional[List[str]] = None,
        include_charts: bool = False
    ) -> bool:
        """
        发送每日价格对比报告邮件（整合了概况和主要变化）
        
        Args:
            report: 每日价格对比报告
            recipients: 收件人列表，如果为None则使用配置中的默认收件人
            include_charts: 是否包含图表附件
            
        Returns:
            True if successful, False otherwise
        """
        if recipients is None:
            recipients = self.config.email_recipients
        
        if not recipients:
            self.logger.error("没有配置邮件收件人")
            return False
        
        if not self._validate_email_config():
            return False
        
        try:
            # 生成邮件内容
            subject = self._generate_subject(report)
            text_content = self._generate_text_content(report)
            html_content = self._generate_html_content(report)
            
            # 创建邮件
            msg = MIMEMultipart('related')
            msg['From'] = self.config.email_username
            msg['To'] = ', '.join(recipients)
            msg['Subject'] = subject
            
            # 添加文本和HTML内容
            msg_alternative = MIMEMultipart('alternative')
            msg.attach(msg_alternative)
            
            text_part = MIMEText(text_content, 'plain', 'utf-8')
            html_part = MIMEText(html_content, 'html', 'utf-8')
            
            msg_alternative.attach(text_part)
            msg_alternative.attach(html_part)
            
            # 发送邮件
            with smtplib.SMTP(self.config.email_smtp_host, self.config.email_smtp_port) as server:
                server.starttls()
                server.login(self.config.email_username, self.config.email_password)
                server.send_message(msg)
            
            self.logger.info(f"每日价格报告邮件发送成功，收件人: {len(recipients)} 人")
            return True
            
        except Exception as e:
            self.logger.error(f"发送每日价格报告邮件失败: {e}")
            return False
    
    def _validate_email_config(self) -> bool:
        """验证邮件配置"""
        if not self.config.email_username or not self.config.email_password:
            self.logger.error("邮件用户名或密码未配置")
            return False
        
        if not self.config.email_smtp_host or not self.config.email_smtp_port:
            self.logger.error("SMTP服务器配置不完整")
            return False
        
        return True
    
    def _generate_subject(self, report: DailyComparisonReport) -> str:
        """生成邮件主题"""
        date_str = report.comparison_date.strftime('%Y年%m月%d日')
        
        # 根据价格变化情况生成不同的主题
        if report.price_increases > report.price_decreases:
            trend_indicator = "📈 整体上涨"
        elif report.price_decreases > report.price_increases:
            trend_indicator = "📉 整体下跌"
        else:
            trend_indicator = "📊 价格稳定"
        
        return f"内存条价格日报 {date_str} - {trend_indicator}"
    
    def _generate_text_content(self, report: DailyComparisonReport) -> str:
        """生成纯文本邮件内容"""
        lines = [
            "📊 内存条价格日报",
            f"📅 对比日期: {report.comparison_date.strftime('%Y年%m月%d日')} vs {report.yesterday_date.strftime('%Y年%m月%d日')}",
            "",
            "📈 总体统计:",
            f"  • 总产品数: {report.total_products}",
            f"  • 价格变化: {report.products_with_changes}",
            f"  • 涨价产品: {report.price_increases} 🔴",
            f"  • 降价产品: {report.price_decreases} 🟢",
            f"  • 价格稳定: {report.stable_prices} ⚪",
            f"  • 新增产品: {report.new_products} 🆕",
        ]
        
        if report.avg_price_change:
            lines.append(f"  • 平均价格变化: ¥{report.avg_price_change:.2f}")
        
        lines.append("")
        
        # 最大涨跌幅
        if report.max_increase:
            change = report.max_increase
            lines.extend([
                "📈 最大涨幅:",
                f"  {change.brand} {change.model}",
                f"  ¥{change.yesterday_price} → ¥{change.today_price}",
                f"  涨幅: ¥{change.price_change:.2f} ({change.change_percentage:.1f}%)",
                ""
            ])
        
        if report.max_decrease:
            change = report.max_decrease
            lines.extend([
                "📉 最大跌幅:",
                f"  {change.brand} {change.model}",
                f"  ¥{change.yesterday_price} → ¥{change.today_price}",
                f"  跌幅: ¥{change.price_change:.2f} ({change.change_percentage:.1f}%)",
                ""
            ])
        
        lines.extend([
            "",
            "📈 今日概况:",
            f"  • 今日监控产品: {len([c for c in report.changes if c.today_price])} 个",
            f"  • 涨价产品: {report.price_increases} 🔴",
            f"  • 降价产品: {report.price_decreases} 🟢", 
            f"  • 价格稳定: {report.stable_prices} ⚪",
            f"  • 新增产品: {report.new_products} 🆕",
        ])
        
        if report.avg_price_change:
            lines.append(f"  • 平均价格变化: ¥{report.avg_price_change:.2f}")
        
        lines.append("")
        
        # 主要价格变化（降低阈值）
        significant_changes = [c for c in report.changes 
                             if c.change_percentage and abs(c.change_percentage) >= 1.0]
        
        if significant_changes:
            lines.extend([
                "🔥 主要价格变化 (>1%):",
            ])
            
            # 按变化幅度排序
            significant_changes.sort(key=lambda x: abs(x.change_percentage or 0), reverse=True)
            
            for i, change in enumerate(significant_changes[:15]):  # 显示前15个
                trend_emoji = "🔴" if change.trend == "up" else "🟢"
                lines.append(f"  {i+1}. {trend_emoji} {change.brand} {change.model[:50]}...")
                lines.append(f"     ¥{change.yesterday_price} → ¥{change.today_price} ({change.change_percentage:+.1f}%)")
                if change.url:
                    lines.append(f"     链接: {change.url}")
                lines.append("")
        else:
            lines.extend([
                "🔥 主要价格变化:",
                "  暂无显著价格变化 (>1%)",
                ""
            ])
        
        # 完整产品价格列表
        lines.extend([
            "📋 完整产品价格列表:",
            "=" * 80,
        ])
        
        # 按品牌分组显示
        brand_groups = {}
        for change in report.changes:
            brand = change.brand
            if brand not in brand_groups:
                brand_groups[brand] = []
            brand_groups[brand].append(change)
        
        for brand, changes in sorted(brand_groups.items()):
            lines.append(f"\n🏷️ {brand}:")
            lines.append("-" * 40)
            
            # 按今日价格排序
            changes.sort(key=lambda x: x.today_price or x.yesterday_price or 0)
            
            for change in changes:
                # 确定趋势符号
                if change.trend == "up":
                    trend_symbol = "📈"
                    change_text = f"(+{change.change_percentage:.1f}%)" if change.change_percentage else "(新增)"
                elif change.trend == "down":
                    trend_symbol = "📉"
                    change_text = f"({change.change_percentage:.1f}%)" if change.change_percentage else "(下跌)"
                elif change.trend == "stable":
                    trend_symbol = "➖"
                    change_text = "(稳定)"
                elif change.trend == "new":
                    trend_symbol = "🆕"
                    change_text = "(新增)"
                elif change.trend == "missing":
                    trend_symbol = "❌"
                    change_text = "(缺失)"
                else:
                    trend_symbol = "❓"
                    change_text = "(未知)"
                
                # 处理价格显示
                yesterday_price_text = f"¥{change.yesterday_price:.0f}" if change.yesterday_price else "-"
                today_price_text = f"¥{change.today_price:.0f}" if change.today_price else "-"
                
                # 截断过长的型号名称
                model_display = change.model[:60] + "..." if len(change.model) > 60 else change.model
                
                lines.append(f"  {trend_symbol} {model_display}")
                lines.append(f"     容量: {change.capacity} | 类型: {change.type}")
                lines.append(f"     价格: {yesterday_price_text} → {today_price_text} {change_text}")
                if change.url:
                    lines.append(f"     链接: {change.url}")
                lines.append("")
        
        # 品牌汇总
        brand_summary = self._calculate_brand_summary(report.changes)
        if brand_summary:
            lines.extend([
                "🏷️ 品牌价格变化汇总:",
            ])
            
            for brand, summary in sorted(brand_summary.items()):
                avg_change = summary['avg_change_percentage']
                trend_emoji = "🔴" if avg_change > 0 else "🟢" if avg_change < 0 else "⚪"
                lines.append(f"  {trend_emoji} {brand}: 平均变化 {avg_change:.1f}% "
                           f"(涨{summary['increases']}/跌{summary['decreases']}/稳{summary['stable']})")
            
            lines.append("")
        
        lines.extend([
            f"⏰ 报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "💡 提示: 查看HTML版本获得更好的阅读体验"
        ])
        
        return "\n".join(lines)
    
    def _generate_html_content(self, report: DailyComparisonReport) -> str:
        """生成HTML邮件内容"""
        # 计算整体趋势
        if report.price_increases > report.price_decreases:
            trend_color = "#e74c3c"
            trend_text = "整体上涨"
            trend_icon = "📈"
        elif report.price_decreases > report.price_increases:
            trend_color = "#27ae60"
            trend_text = "整体下跌"
            trend_icon = "📉"
        else:
            trend_color = "#95a5a6"
            trend_text = "价格稳定"
            trend_icon = "📊"
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>内存条价格日报</title>
            <style>
                body {{ 
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
                    margin: 0; 
                    padding: 20px; 
                    background-color: #f5f5f5;
                }}
                .container {{ 
                    max-width: 800px; 
                    margin: 0 auto; 
                    background-color: white; 
                    border-radius: 10px; 
                    box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                    overflow: hidden;
                }}
                .header {{ 
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white; 
                    padding: 30px; 
                    text-align: center;
                }}
                .header h1 {{ margin: 0; font-size: 28px; }}
                .header p {{ margin: 10px 0 0 0; opacity: 0.9; }}
                .content {{ padding: 30px; }}
                .summary {{ 
                    background-color: #f8f9fa; 
                    padding: 20px; 
                    border-radius: 8px; 
                    margin-bottom: 30px;
                }}
                .summary h2 {{ 
                    margin-top: 0; 
                    color: #333; 
                    border-bottom: 2px solid {trend_color};
                    padding-bottom: 10px;
                }}
                .stats-grid {{ 
                    display: grid; 
                    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); 
                    gap: 15px; 
                    margin-top: 20px;
                }}
                .stat-item {{ 
                    background: white; 
                    padding: 15px; 
                    border-radius: 6px; 
                    border-left: 4px solid #667eea;
                    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
                }}
                .stat-number {{ font-size: 24px; font-weight: bold; color: #333; }}
                .stat-label {{ color: #666; font-size: 14px; margin-top: 5px; }}
                .changes-section {{ margin: 30px 0; }}
                .changes-section h2 {{ color: #333; border-bottom: 2px solid #667eea; padding-bottom: 10px; }}
                .change-item {{ 
                    background: white; 
                    border: 1px solid #e0e0e0; 
                    border-radius: 6px; 
                    padding: 15px; 
                    margin: 10px 0;
                    transition: box-shadow 0.2s;
                }}
                .change-item:hover {{ box-shadow: 0 2px 8px rgba(0,0,0,0.1); }}
                .change-header {{ 
                    display: flex; 
                    justify-content: space-between; 
                    align-items: center; 
                    margin-bottom: 10px;
                }}
                .product-name {{ font-weight: bold; color: #333; }}
                .price-change {{ font-size: 18px; font-weight: bold; }}
                .price-up {{ color: #e74c3c; }}
                .price-down {{ color: #27ae60; }}
                .price-stable {{ color: #95a5a6; }}
                .price-details {{ color: #666; font-size: 14px; }}
                .brand-summary {{ margin: 30px 0; }}
                .brand-item {{ 
                    display: flex; 
                    justify-content: space-between; 
                    align-items: center; 
                    padding: 10px 15px; 
                    margin: 5px 0; 
                    background: #f8f9fa; 
                    border-radius: 6px;
                }}
                .footer {{ 
                    background-color: #f8f9fa; 
                    padding: 20px; 
                    text-align: center; 
                    color: #666; 
                    font-size: 14px;
                }}
                .trend-indicator {{ 
                    display: inline-block; 
                    background-color: {trend_color}; 
                    color: white; 
                    padding: 5px 15px; 
                    border-radius: 20px; 
                    font-size: 14px; 
                    margin-top: 10px;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>📊 内存条价格日报</h1>
                    <p>📅 {report.comparison_date.strftime('%Y年%m月%d日')} vs {report.yesterday_date.strftime('%Y年%m月%d日')}</p>
                    <div class="trend-indicator">{trend_icon} {trend_text}</div>
                </div>
                
                <div class="content">
                    <div class="summary">
                        <h2>📈 总体统计</h2>
                        <div class="stats-grid">
                            <div class="stat-item">
                                <div class="stat-number">{report.total_products}</div>
                                <div class="stat-label">总产品数</div>
                            </div>
                            <div class="stat-item">
                                <div class="stat-number">{report.products_with_changes}</div>
                                <div class="stat-label">价格变化</div>
                            </div>
                            <div class="stat-item">
                                <div class="stat-number">{report.price_increases}</div>
                                <div class="stat-label">涨价产品 🔴</div>
                            </div>
                            <div class="stat-item">
                                <div class="stat-number">{report.price_decreases}</div>
                                <div class="stat-label">降价产品 🟢</div>
                            </div>
                            <div class="stat-item">
                                <div class="stat-number">{report.stable_prices}</div>
                                <div class="stat-label">价格稳定 ⚪</div>
                            </div>
                            <div class="stat-item">
                                <div class="stat-number">{report.new_products}</div>
                                <div class="stat-label">新增产品 🆕</div>
                            </div>
                        </div>
        """
        
        if report.avg_price_change:
            avg_change_color = "#e74c3c" if report.avg_price_change > 0 else "#27ae60"
            html += f"""
                        <div style="text-align: center; margin-top: 20px; padding: 15px; background: white; border-radius: 6px;">
                            <span style="color: {avg_change_color}; font-size: 18px; font-weight: bold;">
                                平均价格变化: ¥{report.avg_price_change:.2f}
                            </span>
                        </div>
            """
        
        html += """
                    </div>
        """
        
        # 最大涨跌幅
        if report.max_increase or report.max_decrease:
            html += """
                    <div class="changes-section">
                        <h2>🏆 极值变化</h2>
            """
            
            if report.max_increase:
                change = report.max_increase
                html += f"""
                        <div class="change-item">
                            <div class="change-header">
                                <div class="product-name">📈 最大涨幅: {change.brand} {change.model}</div>
                                <div class="price-change price-up">+{change.change_percentage:.1f}%</div>
                            </div>
                            <div class="price-details">
                                ¥{change.yesterday_price} → ¥{change.today_price} 
                                (涨幅: ¥{change.price_change:.2f})
                            </div>
                        </div>
                """
            
            if report.max_decrease:
                change = report.max_decrease
                html += f"""
                        <div class="change-item">
                            <div class="change-header">
                                <div class="product-name">📉 最大跌幅: {change.brand} {change.model}</div>
                                <div class="price-change price-down">{change.change_percentage:.1f}%</div>
                            </div>
                            <div class="price-details">
                                ¥{change.yesterday_price} → ¥{change.today_price} 
                                (跌幅: ¥{abs(change.price_change):.2f})
                            </div>
                        </div>
                """
            
            html += "</div>"
        
        # 概况统计（新增）
        html += f"""
                    <div class="changes-section">
                        <h2>📈 今日概况</h2>
                        <div style="background: #f8f9fa; padding: 15px; border-radius: 6px; margin-bottom: 20px;">
                            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 15px;">
                                <div style="text-align: center;">
                                    <div style="font-size: 24px; font-weight: bold; color: #333;">{len([c for c in report.changes if c.today_price])}</div>
                                    <div style="color: #666; font-size: 14px;">今日监控产品</div>
                                </div>
                                <div style="text-align: center;">
                                    <div style="font-size: 24px; font-weight: bold; color: #e74c3c;">{report.price_increases}</div>
                                    <div style="color: #666; font-size: 14px;">涨价产品</div>
                                </div>
                                <div style="text-align: center;">
                                    <div style="font-size: 24px; font-weight: bold; color: #27ae60;">{report.price_decreases}</div>
                                    <div style="color: #666; font-size: 14px;">降价产品</div>
                                </div>
                                <div style="text-align: center;">
                                    <div style="font-size: 24px; font-weight: bold; color: #95a5a6;">{report.stable_prices}</div>
                                    <div style="color: #666; font-size: 14px;">价格稳定</div>
                                </div>
                            </div>
                        </div>
                    </div>
        """

        # 主要价格变化（降低阈值，显示更多变化）
        significant_changes = [c for c in report.changes 
                             if c.change_percentage and abs(c.change_percentage) >= 1.0]
        
        if significant_changes:
            significant_changes.sort(key=lambda x: abs(x.change_percentage or 0), reverse=True)
            
            html += """
                    <div class="changes-section">
                        <h2>🔥 主要价格变化 (>1%)</h2>
                        <div style="overflow-x: auto;">
                            <table style="width: 100%; border-collapse: collapse; margin-top: 15px;">
                                <thead>
                                    <tr style="background-color: #f8f9fa;">
                                        <th style="border: 1px solid #ddd; padding: 12px; text-align: left;">品牌</th>
                                        <th style="border: 1px solid #ddd; padding: 12px; text-align: left;">型号</th>
                                        <th style="border: 1px solid #ddd; padding: 12px; text-align: center;">容量</th>
                                        <th style="border: 1px solid #ddd; padding: 12px; text-align: right;">昨日价格</th>
                                        <th style="border: 1px solid #ddd; padding: 12px; text-align: right;">今日价格</th>
                                        <th style="border: 1px solid #ddd; padding: 12px; text-align: center;">变化</th>
                                        <th style="border: 1px solid #ddd; padding: 12px; text-align: center;">链接</th>
                                    </tr>
                                </thead>
                                <tbody>
            """
            
            for change in significant_changes[:15]:  # 显示前15个主要变化
                price_class = "price-up" if change.trend == "up" else "price-down"
                trend_emoji = "🔴" if change.trend == "up" else "🟢"
                model_display = change.model[:50] + "..." if len(change.model) > 50 else change.model
                
                html += f"""
                                    <tr>
                                        <td style="border: 1px solid #ddd; padding: 8px; font-weight: bold;">{change.brand}</td>
                                        <td style="border: 1px solid #ddd; padding: 8px;" title="{change.model}">{model_display}</td>
                                        <td style="border: 1px solid #ddd; padding: 8px; text-align: center;">{change.capacity}</td>
                                        <td style="border: 1px solid #ddd; padding: 8px; text-align: right;">¥{change.yesterday_price:.0f}</td>
                                        <td style="border: 1px solid #ddd; padding: 8px; text-align: right; font-weight: bold;">¥{change.today_price:.0f}</td>
                                        <td style="border: 1px solid #ddd; padding: 8px; text-align: center;" class="{price_class}">
                                            {trend_emoji} {change.change_percentage:+.1f}%
                                        </td>
                                        <td style="border: 1px solid #ddd; padding: 8px; text-align: center;">
                                            {f'<a href="{change.url}" target="_blank" style="color: #007bff; text-decoration: none;">🔗 查看</a>' if change.url else '-'}
                                        </td>
                                    </tr>
                """
            
            html += """
                                </tbody>
                            </table>
                        </div>
                    </div>
            """
        else:
            html += """
                    <div class="changes-section">
                        <h2>🔥 主要价格变化</h2>
                        <div style="background: #f8f9fa; padding: 20px; border-radius: 6px; text-align: center; color: #666;">
                            📊 今日暂无显著价格变化 (>1%)
                        </div>
                    </div>
            """
        
        # 完整产品价格列表
        html += """
                <div class="changes-section">
                    <h2>📋 完整产品价格列表</h2>
                    <div style="overflow-x: auto;">
                        <table style="width: 100%; border-collapse: collapse; margin-top: 15px;">
                            <thead>
                                <tr style="background-color: #f8f9fa;">
                                    <th style="border: 1px solid #ddd; padding: 12px; text-align: left;">品牌</th>
                                    <th style="border: 1px solid #ddd; padding: 12px; text-align: left;">型号</th>
                                    <th style="border: 1px solid #ddd; padding: 12px; text-align: center;">容量</th>
                                    <th style="border: 1px solid #ddd; padding: 12px; text-align: center;">类型</th>
                                    <th style="border: 1px solid #ddd; padding: 12px; text-align: right;">昨日价格</th>
                                    <th style="border: 1px solid #ddd; padding: 12px; text-align: right;">今日价格</th>
                                    <th style="border: 1px solid #ddd; padding: 12px; text-align: center;">变化</th>
                                    <th style="border: 1px solid #ddd; padding: 12px; text-align: center;">链接</th>
                                </tr>
                            </thead>
                            <tbody>
        """
        
        # 按品牌和价格排序所有产品
        all_changes = sorted(report.changes, key=lambda x: (x.brand, x.today_price or x.yesterday_price or 0))
        
        for change in all_changes:
            # 确定价格变化样式
            if change.trend == "up":
                price_class = "price-up"
                trend_symbol = "📈"
                change_text = f"+{change.change_percentage:.1f}%" if change.change_percentage else "新增"
            elif change.trend == "down":
                price_class = "price-down"
                trend_symbol = "📉"
                change_text = f"{change.change_percentage:.1f}%" if change.change_percentage else "下跌"
            elif change.trend == "stable":
                price_class = "price-stable"
                trend_symbol = "➖"
                change_text = "稳定"
            elif change.trend == "new":
                price_class = "price-up"
                trend_symbol = "🆕"
                change_text = "新增"
            elif change.trend == "missing":
                price_class = "price-down"
                trend_symbol = "❌"
                change_text = "缺失"
            else:
                price_class = "price-stable"
                trend_symbol = "❓"
                change_text = "未知"
            
            # 处理价格显示
            yesterday_price_text = f"¥{change.yesterday_price:.0f}" if change.yesterday_price else "-"
            today_price_text = f"¥{change.today_price:.0f}" if change.today_price else "-"
            
            # 截断过长的型号名称
            model_display = change.model[:50] + "..." if len(change.model) > 50 else change.model
            
            html += f"""
                                <tr>
                                    <td style="border: 1px solid #ddd; padding: 8px; font-weight: bold;">{change.brand}</td>
                                    <td style="border: 1px solid #ddd; padding: 8px;" title="{change.model}">{model_display}</td>
                                    <td style="border: 1px solid #ddd; padding: 8px; text-align: center;">{change.capacity}</td>
                                    <td style="border: 1px solid #ddd; padding: 8px; text-align: center;">{change.type}</td>
                                    <td style="border: 1px solid #ddd; padding: 8px; text-align: right;">{yesterday_price_text}</td>
                                    <td style="border: 1px solid #ddd; padding: 8px; text-align: right; font-weight: bold;">{today_price_text}</td>
                                    <td style="border: 1px solid #ddd; padding: 8px; text-align: center;" class="{price_class}">
                                        {trend_symbol} {change_text}
                                    </td>
                                    <td style="border: 1px solid #ddd; padding: 8px; text-align: center;">
                                        {f'<a href="{change.url}" target="_blank" style="color: #007bff; text-decoration: none;">🔗 查看</a>' if change.url else '-'}
                                    </td>
                                </tr>
            """
        
        html += """
                            </tbody>
                        </table>
                    </div>
                    <p style="margin-top: 15px; color: #666; font-size: 14px;">
                        💡 提示: 表格显示所有监控的内存条产品及其价格变化情况
                    </p>
                </div>
        """
        
        # 品牌汇总
        brand_summary = self._calculate_brand_summary(report.changes)
        if brand_summary:
            html += """
                    <div class="brand-summary">
                        <h2>🏷️ 品牌价格变化汇总</h2>
            """
            
            for brand, summary in sorted(brand_summary.items()):
                avg_change = summary['avg_change_percentage']
                trend_emoji = "🔴" if avg_change > 0 else "🟢" if avg_change < 0 else "⚪"
                change_class = "price-up" if avg_change > 0 else "price-down" if avg_change < 0 else "price-stable"
                
                html += f"""
                        <div class="brand-item">
                            <div>
                                <strong>{trend_emoji} {brand}</strong>
                                <span style="color: #666; margin-left: 10px;">
                                    涨{summary['increases']} / 跌{summary['decreases']} / 稳{summary['stable']}
                                </span>
                            </div>
                            <div class="price-change {change_class}">
                                平均 {avg_change:+.1f}%
                            </div>
                        </div>
                """
            
            html += "</div>"
        
        html += f"""
                </div>
                
                <div class="footer">
                    <p>⏰ 报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                    <p>💡 此报告由内存条价格监控系统自动生成</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        return html
    
    def _calculate_brand_summary(self, changes: List[PriceChange]) -> Dict[str, Dict[str, Any]]:
        """计算品牌汇总信息"""
        brand_summary = {}
        
        for change in changes:
            brand = change.brand
            if brand not in brand_summary:
                brand_summary[brand] = {
                    'increases': 0,
                    'decreases': 0,
                    'stable': 0,
                    'total_change': 0.0,
                    'count': 0
                }
            
            summary = brand_summary[brand]
            
            if change.trend == "up":
                summary['increases'] += 1
            elif change.trend == "down":
                summary['decreases'] += 1
            elif change.trend == "stable":
                summary['stable'] += 1
            
            if change.change_percentage is not None:
                summary['total_change'] += change.change_percentage
                summary['count'] += 1
        
        # 计算平均变化
        for brand, summary in brand_summary.items():
            if summary['count'] > 0:
                summary['avg_change_percentage'] = summary['total_change'] / summary['count']
            else:
                summary['avg_change_percentage'] = 0.0
        
        return brand_summary
"""
Notification service for sending weekly reports via WeChat and email.
"""

import logging
import smtplib
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from typing import List, Dict, Any, Optional, Tuple
import requests
from datetime import datetime, date

from memory_price_monitor.data.models import WeeklyReport, FormattedReport
from memory_price_monitor.utils.errors import NotificationError
from config import NotificationConfig


logger = logging.getLogger(__name__)


class NotificationService:
    """Service for sending notifications via multiple channels."""
    
    def __init__(self, config: NotificationConfig):
        """
        Initialize notification service.
        
        Args:
            config: Notification configuration
        """
        self.config = config
        self._wxpy_bot = None
        
    def send_wechat_serverchan(self, report: WeeklyReport) -> bool:
        """
        Send WeChat notification using Server酱 API.
        
        Args:
            report: Weekly report to send
            
        Returns:
            True if successful, False otherwise
        """
        if not self.config.serverchan_key:
            logger.error("Server酱 key not configured")
            return False
            
        try:
            formatted_report = self.format_report(report)
            
            # Server酱 API endpoint
            url = f"https://sctapi.ftqq.com/{self.config.serverchan_key}.send"
            
            # Prepare data for Server酱
            data = {
                "title": formatted_report.subject,
                "desp": formatted_report.text_content
            }
            
            response = requests.post(url, data=data, timeout=30)
            response.raise_for_status()
            
            result = response.json()
            if result.get("code") == 0:
                logger.info("WeChat notification sent successfully via Server酱")
                return True
            else:
                logger.error(f"Server酱 API error: {result}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to send WeChat notification via Server酱: {e}")
            return False
    
    def send_wechat_wxpy(self, report: WeeklyReport) -> bool:
        """
        Send WeChat notification using wxpy/itchat for direct messaging.
        
        Args:
            report: Weekly report to send
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Import wxpy only when needed to avoid dependency issues
            import wxpy
            
            if self._wxpy_bot is None:
                self._wxpy_bot = wxpy.Bot(cache_path=True)
            
            formatted_report = self.format_report(report)
            
            # Send to file transfer helper (文件传输助手)
            file_helper = self._wxpy_bot.file_helper
            file_helper.send(formatted_report.text_content)
            
            # Send chart images if available
            for attachment in formatted_report.attachments:
                if attachment.get('content_type', '').startswith('image/'):
                    # Save image temporarily and send
                    temp_path = f"/tmp/{attachment['filename']}"
                    with open(temp_path, 'wb') as f:
                        f.write(attachment['content'])
                    file_helper.send_image(temp_path)
            
            logger.info("WeChat notification sent successfully via wxpy")
            return True
            
        except ImportError:
            logger.error("wxpy not installed. Install with: pip install wxpy")
            return False
        except Exception as e:
            logger.error(f"Failed to send WeChat notification via wxpy: {e}")
            return False
    
    def send_email(self, recipients: List[str], report: WeeklyReport) -> bool:
        """
        Send email notification.
        
        Args:
            recipients: List of email addresses
            report: Weekly report to send
            
        Returns:
            True if successful, False otherwise
        """
        if not recipients:
            recipients = self.config.email_recipients
            
        if not recipients:
            logger.error("No email recipients configured")
            return False
            
        if not self.config.email_username or not self.config.email_password:
            logger.error("Email credentials not configured")
            return False
            
        try:
            formatted_report = self.format_report(report)
            
            # Create message
            msg = MIMEMultipart('related')
            msg['From'] = self.config.email_username
            msg['To'] = ', '.join(recipients)
            msg['Subject'] = formatted_report.subject
            
            # Add text and HTML content
            msg_alternative = MIMEMultipart('alternative')
            msg.attach(msg_alternative)
            
            text_part = MIMEText(formatted_report.text_content, 'plain', 'utf-8')
            html_part = MIMEText(formatted_report.html_content, 'html', 'utf-8')
            
            msg_alternative.attach(text_part)
            msg_alternative.attach(html_part)
            
            # Add image attachments
            for i, attachment in enumerate(formatted_report.attachments):
                if attachment.get('content_type', '').startswith('image/'):
                    # Specify PNG subtype explicitly to avoid MIME guessing issues
                    img = MIMEImage(attachment['content'], _subtype='png')
                    img.add_header('Content-ID', f'<image{i}>')
                    img.add_header('Content-Disposition', 
                                 f'attachment; filename="{attachment["filename"]}"')
                    msg.attach(img)
            
            # Send email
            with smtplib.SMTP(self.config.email_smtp_host, self.config.email_smtp_port) as server:
                server.starttls()
                server.login(self.config.email_username, self.config.email_password)
                server.send_message(msg)
            
            logger.info(f"Email notification sent successfully to {len(recipients)} recipients")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email notification: {e}")
            return False
    
    def format_report(self, report: WeeklyReport) -> FormattedReport:
        """
        Format weekly report for notifications.
        
        Args:
            report: Weekly report to format
            
        Returns:
            Formatted report with text and HTML content
        """
        # Generate subject
        subject = f"内存价格周报 - {report.report_date.strftime('%Y年%m月%d日')}"
        
        # Generate text content
        text_content = self._generate_text_content(report)
        
        # Generate HTML content
        html_content = self._generate_html_content(report)
        
        # Prepare attachments
        attachments = []
        for chart_name, chart_data in report.charts.items():
            attachments.append({
                'filename': f"{chart_name}.png",
                'content': chart_data,
                'content_type': 'image/png'
            })
        
        return FormattedReport(
            subject=subject,
            text_content=text_content,
            html_content=html_content,
            attachments=attachments
        )
    
    def _generate_text_content(self, report: WeeklyReport) -> str:
        """Generate plain text content for the report."""
        lines = [
            f"📊 内存价格周报",
            f"📅 报告日期: {report.report_date.strftime('%Y年%m月%d日')}",
            f"📈 数据周期: {report.week_start.strftime('%m月%d日')} - {report.week_end.strftime('%m月%d日')}",
            "",
            "📋 本周概况:",
        ]
        
        # Add summary statistics
        stats = report.summary_stats
        if 'total_products' in stats:
            lines.append(f"• 监控产品数量: {stats['total_products']}")
        if 'average_price_change' in stats:
            change = stats['average_price_change']
            direction = "上涨" if change > 0 else "下跌" if change < 0 else "持平"
            lines.append(f"• 平均价格变化: {direction} {abs(change):.1f}%")
        
        # Add significant changes
        if report.comparison.significant_changes:
            lines.extend([
                "",
                "🔥 显著价格变化:",
            ])
            
            for trend in report.comparison.significant_changes[:5]:  # Top 5
                change_symbol = "📈" if trend.percentage_change > 0 else "📉"
                lines.append(
                    f"{change_symbol} {trend.brand} {trend.model} "
                    f"({trend.capacity}): {trend.percentage_change:+.1f}%"
                )
        
        lines.extend([
            "",
            "📊 详细图表请查看附件",
            "",
            f"⏰ 报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        ])
        
        return "\n".join(lines)
    
    def _generate_html_content(self, report: WeeklyReport) -> str:
        """Generate HTML content for the report."""
        html = f"""
        <html>
        <head>
            <meta charset="utf-8">
            <title>内存价格周报</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background-color: #f0f8ff; padding: 15px; border-radius: 5px; }}
                .summary {{ margin: 20px 0; }}
                .changes {{ margin: 20px 0; }}
                .trend-up {{ color: #e74c3c; }}
                .trend-down {{ color: #27ae60; }}
                .chart-container {{ margin: 20px 0; text-align: center; }}
                table {{ border-collapse: collapse; width: 100%; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>📊 内存价格周报</h1>
                <p><strong>📅 报告日期:</strong> {report.report_date.strftime('%Y年%m月%d日')}</p>
                <p><strong>📈 数据周期:</strong> {report.week_start.strftime('%m月%d日')} - {report.week_end.strftime('%m月%d日')}</p>
            </div>
            
            <div class="summary">
                <h2>📋 本周概况</h2>
                <ul>
        """
        
        # Add summary statistics
        stats = report.summary_stats
        if 'total_products' in stats:
            html += f"<li>监控产品数量: <strong>{stats['total_products']}</strong></li>"
        if 'average_price_change' in stats:
            change = stats['average_price_change']
            direction = "上涨" if change > 0 else "下跌" if change < 0 else "持平"
            color_class = "trend-up" if change > 0 else "trend-down" if change < 0 else ""
            html += f'<li>平均价格变化: <span class="{color_class}"><strong>{direction} {abs(change):.1f}%</strong></span></li>'
        
        html += """
                </ul>
            </div>
        """
        
        # Add significant changes
        if report.comparison.significant_changes:
            html += """
            <div class="changes">
                <h2>🔥 显著价格变化</h2>
                <table>
                    <tr>
                        <th>品牌</th>
                        <th>型号</th>
                        <th>容量</th>
                        <th>价格变化</th>
                    </tr>
            """
            
            for trend in report.comparison.significant_changes[:10]:  # Top 10
                change_symbol = "📈" if trend.percentage_change > 0 else "📉"
                color_class = "trend-up" if trend.percentage_change > 0 else "trend-down"
                html += f"""
                    <tr>
                        <td>{trend.brand}</td>
                        <td>{trend.model}</td>
                        <td>{trend.capacity}</td>
                        <td class="{color_class}">{change_symbol} {trend.percentage_change:+.1f}%</td>
                    </tr>
                """
            
            html += """
                </table>
            </div>
            """
        
        # Add chart references
        if report.charts:
            html += """
            <div class="chart-container">
                <h2>📊 价格趋势图表</h2>
                <p>详细图表请查看邮件附件</p>
            """
            
            for i, chart_name in enumerate(report.charts.keys()):
                html += f'<p>图表 {i+1}: {chart_name}</p>'
            
            html += "</div>"
        
        html += f"""
            <div style="margin-top: 30px; font-size: 12px; color: #666;">
                <p>⏰ 报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
        </body>
        </html>
        """
        
        return html
    
    def get_wechat_method(self) -> str:
        """
        Get configured WeChat sending method.
        
        Returns:
            WeChat method: "serverchan" or "wxpy"
        """
        return self.config.wechat_method
    
    def send_notification_with_retry(self, report: WeeklyReport, 
                                   channels: Optional[List[str]] = None) -> Dict[str, bool]:
        """
        Send notification with retry logic and method fallback.
        
        Args:
            report: Weekly report to send
            channels: List of channels to use ("wechat", "email"). If None, use all configured.
            
        Returns:
            Dictionary with channel success status
        """
        if channels is None:
            channels = ["wechat", "email"]
        
        results = {}
        
        for channel in channels:
            success = False
            last_error = None
            
            for attempt in range(self.config.retry_attempts):
                try:
                    if channel == "wechat":
                        success = self._send_wechat_with_fallback(report)
                    elif channel == "email":
                        success = self.send_email([], report)
                    
                    if success:
                        break
                        
                except Exception as e:
                    last_error = e
                    logger.warning(f"Notification attempt {attempt + 1} failed for {channel}: {e}")
                
                if attempt < self.config.retry_attempts - 1:
                    time.sleep(self.config.retry_delay * (2 ** attempt))  # Exponential backoff
            
            results[channel] = success
            
            if not success:
                logger.error(f"All retry attempts failed for {channel}. Last error: {last_error}")
        
        return results
    
    def _send_wechat_with_fallback(self, report: WeeklyReport) -> bool:
        """Send WeChat notification with method fallback."""
        primary_method = self.get_wechat_method()
        
        # Try primary method first
        if primary_method == "serverchan":
            if self.send_wechat_serverchan(report):
                return True
            # Fallback to wxpy
            logger.warning("Server酱 failed, trying wxpy fallback")
            return self.send_wechat_wxpy(report)
        else:
            if self.send_wechat_wxpy(report):
                return True
            # Fallback to serverchan
            logger.warning("wxpy failed, trying Server酱 fallback")
            return self.send_wechat_serverchan(report)
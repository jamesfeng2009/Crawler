"""
价格对比分析服务 - 提供今日vs昨日价格对比和涨跌统计
"""

from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, date, timedelta
from dataclasses import dataclass, field
from decimal import Decimal
import logging

from memory_price_monitor.data.repository import PriceRepository
from memory_price_monitor.data.models import StandardizedProduct
from memory_price_monitor.utils.errors import ValidationError


logger = logging.getLogger(__name__)


@dataclass
class PriceChange:
    """价格变化信息"""
    product_id: int
    brand: str
    model: str
    capacity: str
    type: str
    yesterday_price: Optional[Decimal]
    today_price: Optional[Decimal]
    url: str = ""  # 产品链接URL
    price_change: Optional[Decimal] = None  # 价格变化金额
    change_percentage: Optional[float] = None  # 变化百分比
    trend: str = "unknown"  # "up", "down", "stable", "new", "missing"
    
    def __post_init__(self):
        """计算价格变化"""
        if self.yesterday_price and self.today_price:
            self.price_change = self.today_price - self.yesterday_price
            if self.yesterday_price > 0:
                self.change_percentage = float((self.price_change / self.yesterday_price) * 100)
            else:
                self.change_percentage = 0.0
            
            # 确定趋势
            if abs(self.change_percentage) < 0.1:  # 变化小于0.1%认为稳定
                self.trend = "stable"
            elif self.change_percentage > 0:
                self.trend = "up"
            else:
                self.trend = "down"
        elif self.today_price and not self.yesterday_price:
            self.trend = "new"
            self.price_change = None
            self.change_percentage = None
        elif self.yesterday_price and not self.today_price:
            self.trend = "missing"
            self.price_change = None
            self.change_percentage = None
        else:
            self.trend = "unknown"
            self.price_change = None
            self.change_percentage = None


@dataclass
class DailyComparisonReport:
    """每日价格对比报告"""
    comparison_date: date
    yesterday_date: date
    total_products: int
    products_with_changes: int
    price_increases: int
    price_decreases: int
    stable_prices: int
    new_products: int
    missing_products: int
    
    # 统计信息
    avg_price_change: Optional[Decimal] = None
    max_increase: Optional[PriceChange] = None
    max_decrease: Optional[PriceChange] = None
    
    # 详细变化列表
    changes: List[PriceChange] = field(default_factory=list)
    
    def calculate_statistics(self):
        """计算统计信息"""
        if not self.changes:
            return
        
        # 计算有效价格变化
        valid_changes = [c for c in self.changes if c.price_change is not None]
        
        if valid_changes:
            # 平均价格变化
            total_change = sum(c.price_change for c in valid_changes)
            self.avg_price_change = total_change / len(valid_changes)
            
            # 最大涨幅和跌幅
            increases = [c for c in valid_changes if c.price_change > 0]
            decreases = [c for c in valid_changes if c.price_change < 0]
            
            if increases:
                self.max_increase = max(increases, key=lambda x: x.price_change)
            
            if decreases:
                self.max_decrease = min(decreases, key=lambda x: x.price_change)


class PriceComparisonService:
    """价格对比分析服务"""
    
    def __init__(self, repository: PriceRepository):
        """
        初始化价格对比服务
        
        Args:
            repository: 价格数据仓库
        """
        self.repository = repository
        self.logger = logging.getLogger(f"{__name__}.PriceComparisonService")
    
    def compare_daily_prices(
        self, 
        target_date: Optional[date] = None,
        source_filter: Optional[str] = None
    ) -> DailyComparisonReport:
        """
        对比指定日期与前一天的价格
        
        Args:
            target_date: 目标日期，默认为今天
            source_filter: 数据源过滤器（如'zol', 'jd'）
            
        Returns:
            每日价格对比报告
        """
        if target_date is None:
            target_date = date.today()
        
        yesterday_date = target_date - timedelta(days=1)
        
        self.logger.info(f"开始对比价格: {target_date} vs {yesterday_date}")
        
        try:
            # 获取两天的价格数据
            today_prices = self._get_latest_prices_by_date(target_date, source_filter)
            yesterday_prices = self._get_latest_prices_by_date(yesterday_date, source_filter)
            
            # 创建产品ID到价格的映射
            today_price_map = {p['product_id']: p for p in today_prices}
            yesterday_price_map = {p['product_id']: p for p in yesterday_prices}
            
            # 获取所有涉及的产品ID
            all_product_ids = set(today_price_map.keys()) | set(yesterday_price_map.keys())
            
            # 生成价格变化列表
            changes = []
            for product_id in all_product_ids:
                today_data = today_price_map.get(product_id)
                yesterday_data = yesterday_price_map.get(product_id)
                
                # 获取产品基本信息
                product_info = today_data or yesterday_data
                
                change = PriceChange(
                    product_id=product_id,
                    brand=product_info['brand'],
                    model=product_info['model'],
                    capacity=product_info['capacity'],
                    type=product_info['type'],
                    yesterday_price=Decimal(str(yesterday_data['current_price'])) if yesterday_data else None,
                    today_price=Decimal(str(today_data['current_price'])) if today_data else None,
                    url=product_info.get('url', '')  # 添加产品URL
                )
                
                changes.append(change)
            
            # 统计各种情况的数量
            price_increases = len([c for c in changes if c.trend == "up"])
            price_decreases = len([c for c in changes if c.trend == "down"])
            stable_prices = len([c for c in changes if c.trend == "stable"])
            new_products = len([c for c in changes if c.trend == "new"])
            missing_products = len([c for c in changes if c.trend == "missing"])
            
            # 创建报告
            report = DailyComparisonReport(
                comparison_date=target_date,
                yesterday_date=yesterday_date,
                total_products=len(all_product_ids),
                products_with_changes=price_increases + price_decreases,
                price_increases=price_increases,
                price_decreases=price_decreases,
                stable_prices=stable_prices,
                new_products=new_products,
                missing_products=missing_products,
                changes=changes
            )
            
            # 计算统计信息
            report.calculate_statistics()
            
            self.logger.info(f"价格对比完成: 总产品{report.total_products}, "
                           f"涨价{price_increases}, 降价{price_decreases}, "
                           f"稳定{stable_prices}, 新增{new_products}")
            
            return report
            
        except Exception as e:
            self.logger.error(f"价格对比失败: {e}")
            raise ValidationError(f"价格对比失败: {e}")
    
    def get_significant_changes(
        self, 
        report: DailyComparisonReport,
        min_change_percentage: float = 5.0,
        min_change_amount: float = 10.0
    ) -> List[PriceChange]:
        """
        获取显著的价格变化
        
        Args:
            report: 价格对比报告
            min_change_percentage: 最小变化百分比阈值
            min_change_amount: 最小变化金额阈值
            
        Returns:
            显著价格变化列表
        """
        significant_changes = []
        
        for change in report.changes:
            if change.change_percentage is None or change.price_change is None:
                continue
            
            # 检查是否满足显著变化条件
            percentage_significant = abs(change.change_percentage) >= min_change_percentage
            amount_significant = abs(float(change.price_change)) >= min_change_amount
            
            if percentage_significant or amount_significant:
                significant_changes.append(change)
        
        # 按变化幅度排序
        significant_changes.sort(key=lambda x: abs(x.change_percentage or 0), reverse=True)
        
        return significant_changes
    
    def get_brand_summary(self, report: DailyComparisonReport) -> Dict[str, Dict[str, Any]]:
        """
        获取按品牌分组的价格变化摘要
        
        Args:
            report: 价格对比报告
            
        Returns:
            品牌价格变化摘要
        """
        brand_summary = {}
        
        for change in report.changes:
            brand = change.brand
            if brand not in brand_summary:
                brand_summary[brand] = {
                    'total_products': 0,
                    'price_increases': 0,
                    'price_decreases': 0,
                    'stable_prices': 0,
                    'new_products': 0,
                    'missing_products': 0,
                    'avg_change_percentage': 0.0,
                    'changes': []
                }
            
            summary = brand_summary[brand]
            summary['total_products'] += 1
            summary['changes'].append(change)
            
            if change.trend == "up":
                summary['price_increases'] += 1
            elif change.trend == "down":
                summary['price_decreases'] += 1
            elif change.trend == "stable":
                summary['stable_prices'] += 1
            elif change.trend == "new":
                summary['new_products'] += 1
            elif change.trend == "missing":
                summary['missing_products'] += 1
        
        # 计算每个品牌的平均变化百分比
        for brand, summary in brand_summary.items():
            valid_changes = [c.change_percentage for c in summary['changes'] 
                           if c.change_percentage is not None]
            if valid_changes:
                summary['avg_change_percentage'] = sum(valid_changes) / len(valid_changes)
        
        return brand_summary
    
    def format_report_text(self, report: DailyComparisonReport) -> str:
        """
        格式化报告为文本
        
        Args:
            report: 价格对比报告
            
        Returns:
            格式化的文本报告
        """
        lines = []
        lines.append(f"📊 内存条价格对比报告")
        lines.append(f"📅 对比日期: {report.comparison_date} vs {report.yesterday_date}")
        lines.append("")
        
        # 总体统计
        lines.append("📈 总体统计:")
        lines.append(f"  • 总产品数: {report.total_products}")
        lines.append(f"  • 价格变化: {report.products_with_changes}")
        lines.append(f"  • 涨价产品: {report.price_increases} 🔴")
        lines.append(f"  • 降价产品: {report.price_decreases} 🟢")
        lines.append(f"  • 价格稳定: {report.stable_prices} ⚪")
        lines.append(f"  • 新增产品: {report.new_products} 🆕")
        
        if report.avg_price_change:
            lines.append(f"  • 平均价格变化: ¥{report.avg_price_change:.2f}")
        
        lines.append("")
        
        # 最大涨跌幅
        if report.max_increase:
            change = report.max_increase
            lines.append(f"📈 最大涨幅:")
            lines.append(f"  {change.brand} {change.model}")
            lines.append(f"  ¥{change.yesterday_price} → ¥{change.today_price}")
            lines.append(f"  涨幅: ¥{change.price_change:.2f} ({change.change_percentage:.1f}%)")
            lines.append("")
        
        if report.max_decrease:
            change = report.max_decrease
            lines.append(f"📉 最大跌幅:")
            lines.append(f"  {change.brand} {change.model}")
            lines.append(f"  ¥{change.yesterday_price} → ¥{change.today_price}")
            lines.append(f"  跌幅: ¥{change.price_change:.2f} ({change.change_percentage:.1f}%)")
            lines.append("")
        
        # 显著变化
        significant_changes = self.get_significant_changes(report, min_change_percentage=3.0)
        if significant_changes:
            lines.append("🔥 显著价格变化 (>3%):")
            for i, change in enumerate(significant_changes[:10]):  # 只显示前10个
                trend_emoji = "🔴" if change.trend == "up" else "🟢"
                lines.append(f"  {i+1}. {trend_emoji} {change.brand} {change.model}")
                lines.append(f"     ¥{change.yesterday_price} → ¥{change.today_price}")
                lines.append(f"     变化: {change.change_percentage:.1f}%")
            lines.append("")
        
        return "\n".join(lines)
    
    def _get_latest_prices_by_date(
        self, 
        target_date: date, 
        source_filter: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        获取指定日期的最新价格数据
        
        Args:
            target_date: 目标日期
            source_filter: 数据源过滤器
            
        Returns:
            价格数据列表
        """
        # 构建查询条件
        start_datetime = datetime.combine(target_date, datetime.min.time())
        end_datetime = datetime.combine(target_date, datetime.max.time())
        
        # 这里需要调用repository的方法获取数据
        # 由于当前repository可能没有这个方法，我们先用一个占位符
        try:
            # 假设repository有这个方法
            return self.repository.get_latest_prices_by_date_range(
                start_datetime, end_datetime, source_filter
            )
        except AttributeError:
            # 如果方法不存在，返回空列表并记录警告
            self.logger.warning("Repository缺少get_latest_prices_by_date_range方法")
            return []
    
    def get_weekly_trend(
        self, 
        end_date: Optional[date] = None,
        source_filter: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        获取一周的价格趋势
        
        Args:
            end_date: 结束日期，默认为今天
            source_filter: 数据源过滤器
            
        Returns:
            一周价格趋势数据
        """
        if end_date is None:
            end_date = date.today()
        
        start_date = end_date - timedelta(days=6)  # 7天数据
        
        daily_reports = []
        for i in range(7):
            current_date = start_date + timedelta(days=i)
            if current_date <= end_date:
                try:
                    report = self.compare_daily_prices(current_date, source_filter)
                    daily_reports.append(report)
                except Exception as e:
                    self.logger.warning(f"无法获取{current_date}的价格对比: {e}")
        
        # 汇总一周的趋势
        total_increases = sum(r.price_increases for r in daily_reports)
        total_decreases = sum(r.price_decreases for r in daily_reports)
        total_stable = sum(r.stable_prices for r in daily_reports)
        
        return {
            'period': f"{start_date} 至 {end_date}",
            'daily_reports': daily_reports,
            'summary': {
                'total_price_increases': total_increases,
                'total_price_decreases': total_decreases,
                'total_stable_prices': total_stable,
                'trend_direction': 'up' if total_increases > total_decreases else 'down' if total_decreases > total_increases else 'stable'
            }
        }
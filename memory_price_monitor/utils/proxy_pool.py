#!/usr/bin/env python3
"""
代理池管理模块
支持HTTP/HTTPS/SOCKS代理的轮换和健康检查
"""

import random
import time
import asyncio
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from memory_price_monitor.utils.logging import get_business_logger


logger = get_business_logger('proxy_pool')


@dataclass
class ProxyInfo:
    """代理信息"""
    host: str
    port: int
    username: Optional[str] = None
    password: Optional[str] = None
    protocol: str = 'http'  # http, https, socks4, socks5
    country: Optional[str] = None
    city: Optional[str] = None
    
    # 健康状态
    is_active: bool = True
    last_used: Optional[datetime] = None
    last_checked: Optional[datetime] = None
    success_count: int = 0
    failure_count: int = 0
    avg_response_time: float = 0.0
    
    # 使用统计
    total_requests: int = 0
    failed_requests: int = 0
    
    def __post_init__(self):
        """初始化后处理"""
        if self.last_checked is None:
            self.last_checked = datetime.now()
    
    @property
    def proxy_url(self) -> str:
        """获取代理URL"""
        if self.username and self.password:
            return f"{self.protocol}://{self.username}:{self.password}@{self.host}:{self.port}"
        else:
            return f"{self.protocol}://{self.host}:{self.port}"
    
    @property
    def success_rate(self) -> float:
        """成功率"""
        if self.total_requests == 0:
            return 0.0
        return (self.total_requests - self.failed_requests) / self.total_requests
    
    @property
    def is_healthy(self) -> bool:
        """是否健康"""
        return (
            self.is_active and 
            self.success_rate >= 0.7 and  # 成功率 >= 70%
            self.failure_count < 5  # 连续失败次数 < 5
        )


class ProxyPool:
    """代理池管理器"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化代理池
        
        Args:
            config: 配置字典
        """
        self.config = config or {}
        self.proxies: List[ProxyInfo] = []
        self.current_index = 0
        
        # 配置参数
        self.health_check_interval = self.config.get('health_check_interval', 300)  # 5分钟
        self.health_check_timeout = self.config.get('health_check_timeout', 10)
        self.health_check_url = self.config.get('health_check_url', 'http://httpbin.org/ip')
        self.max_failure_count = self.config.get('max_failure_count', 5)
        self.min_success_rate = self.config.get('min_success_rate', 0.7)
        
        # 自动健康检查
        self.auto_health_check = self.config.get('auto_health_check', True)
        self.last_health_check = datetime.now()
        
        logger.info("代理池管理器初始化完成")
    
    def add_proxy(self, host: str, port: int, username: str = None, password: str = None, 
                  protocol: str = 'http', country: str = None, city: str = None) -> None:
        """
        添加代理
        
        Args:
            host: 代理主机
            port: 代理端口
            username: 用户名
            password: 密码
            protocol: 协议类型
            country: 国家
            city: 城市
        """
        proxy = ProxyInfo(
            host=host,
            port=port,
            username=username,
            password=password,
            protocol=protocol,
            country=country,
            city=city
        )
        
        self.proxies.append(proxy)
        logger.info(f"添加代理: {proxy.proxy_url}")
    
    def load_proxies_from_config(self, proxy_list: List[Dict[str, Any]]) -> None:
        """
        从配置加载代理列表
        
        Args:
            proxy_list: 代理配置列表
        """
        for proxy_config in proxy_list:
            self.add_proxy(**proxy_config)
        
        logger.info(f"从配置加载了 {len(proxy_list)} 个代理")
    
    def get_next_proxy(self) -> Optional[ProxyInfo]:
        """
        获取下一个可用代理（轮换）
        
        Returns:
            代理信息或None
        """
        if not self.proxies:
            logger.warning("代理池为空")
            return None
        
        # 自动健康检查
        if self.auto_health_check:
            self._auto_health_check()
        
        # 获取健康的代理
        healthy_proxies = [p for p in self.proxies if p.is_healthy]
        
        if not healthy_proxies:
            logger.warning("没有健康的代理可用")
            # 如果没有健康代理，尝试重新激活一些代理
            self._reactivate_proxies()
            healthy_proxies = [p for p in self.proxies if p.is_healthy]
            
            if not healthy_proxies:
                return None
        
        # 轮换选择
        proxy = healthy_proxies[self.current_index % len(healthy_proxies)]
        self.current_index = (self.current_index + 1) % len(healthy_proxies)
        
        proxy.last_used = datetime.now()
        logger.debug(f"选择代理: {proxy.host}:{proxy.port}")
        
        return proxy
    
    def get_random_proxy(self) -> Optional[ProxyInfo]:
        """
        随机获取一个可用代理
        
        Returns:
            代理信息或None
        """
        if not self.proxies:
            return None
        
        # 自动健康检查
        if self.auto_health_check:
            self._auto_health_check()
        
        healthy_proxies = [p for p in self.proxies if p.is_healthy]
        
        if not healthy_proxies:
            logger.warning("没有健康的代理可用")
            return None
        
        proxy = random.choice(healthy_proxies)
        proxy.last_used = datetime.now()
        
        return proxy
    
    def get_proxy_by_country(self, country: str) -> Optional[ProxyInfo]:
        """
        根据国家获取代理
        
        Args:
            country: 国家代码
            
        Returns:
            代理信息或None
        """
        country_proxies = [
            p for p in self.proxies 
            if p.is_healthy and p.country and p.country.lower() == country.lower()
        ]
        
        if not country_proxies:
            logger.warning(f"没有来自 {country} 的健康代理")
            return None
        
        return random.choice(country_proxies)
    
    def mark_proxy_success(self, proxy: ProxyInfo, response_time: float = 0.0) -> None:
        """
        标记代理请求成功
        
        Args:
            proxy: 代理信息
            response_time: 响应时间
        """
        proxy.success_count += 1
        proxy.total_requests += 1
        proxy.failure_count = 0  # 重置连续失败计数
        
        # 更新平均响应时间
        if proxy.avg_response_time == 0:
            proxy.avg_response_time = response_time
        else:
            proxy.avg_response_time = (proxy.avg_response_time + response_time) / 2
        
        logger.debug(f"代理成功: {proxy.host}:{proxy.port}, 响应时间: {response_time:.2f}s")
    
    def mark_proxy_failure(self, proxy: ProxyInfo, error: str = "") -> None:
        """
        标记代理请求失败
        
        Args:
            proxy: 代理信息
            error: 错误信息
        """
        proxy.failure_count += 1
        proxy.total_requests += 1
        proxy.failed_requests += 1
        
        # 如果连续失败次数过多，暂时禁用
        if proxy.failure_count >= self.max_failure_count:
            proxy.is_active = False
            logger.warning(f"代理已禁用: {proxy.host}:{proxy.port}, 连续失败 {proxy.failure_count} 次")
        
        logger.debug(f"代理失败: {proxy.host}:{proxy.port}, 错误: {error}")
    
    def check_proxy_health(self, proxy: ProxyInfo) -> bool:
        """
        检查单个代理的健康状态
        
        Args:
            proxy: 代理信息
            
        Returns:
            是否健康
        """
        try:
            proxies = {
                'http': proxy.proxy_url,
                'https': proxy.proxy_url
            }
            
            start_time = time.time()
            response = requests.get(
                self.health_check_url,
                proxies=proxies,
                timeout=self.health_check_timeout,
                verify=False
            )
            response_time = time.time() - start_time
            
            if response.status_code == 200:
                self.mark_proxy_success(proxy, response_time)
                proxy.last_checked = datetime.now()
                return True
            else:
                self.mark_proxy_failure(proxy, f"HTTP {response.status_code}")
                return False
                
        except Exception as e:
            self.mark_proxy_failure(proxy, str(e))
            return False
    
    def check_all_proxies_health(self) -> Dict[str, int]:
        """
        检查所有代理的健康状态
        
        Returns:
            健康检查统计
        """
        if not self.proxies:
            return {'total': 0, 'healthy': 0, 'unhealthy': 0}
        
        logger.info(f"开始检查 {len(self.proxies)} 个代理的健康状态")
        
        healthy_count = 0
        unhealthy_count = 0
        
        # 使用线程池并发检查
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_proxy = {
                executor.submit(self.check_proxy_health, proxy): proxy 
                for proxy in self.proxies
            }
            
            for future in as_completed(future_to_proxy):
                proxy = future_to_proxy[future]
                try:
                    is_healthy = future.result()
                    if is_healthy:
                        healthy_count += 1
                    else:
                        unhealthy_count += 1
                except Exception as e:
                    logger.error(f"检查代理 {proxy.host}:{proxy.port} 时出错: {e}")
                    unhealthy_count += 1
        
        self.last_health_check = datetime.now()
        
        stats = {
            'total': len(self.proxies),
            'healthy': healthy_count,
            'unhealthy': unhealthy_count
        }
        
        logger.info(f"健康检查完成: {stats}")
        return stats
    
    def _auto_health_check(self) -> None:
        """自动健康检查"""
        now = datetime.now()
        if (now - self.last_health_check).total_seconds() >= self.health_check_interval:
            logger.info("执行自动健康检查")
            self.check_all_proxies_health()
    
    def _reactivate_proxies(self) -> None:
        """重新激活一些代理"""
        inactive_proxies = [p for p in self.proxies if not p.is_active]
        
        # 重新激活失败次数较少的代理
        for proxy in inactive_proxies:
            if proxy.failure_count < self.max_failure_count * 2:
                proxy.is_active = True
                proxy.failure_count = 0
                logger.info(f"重新激活代理: {proxy.host}:{proxy.port}")
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        获取代理池统计信息
        
        Returns:
            统计信息字典
        """
        if not self.proxies:
            return {
                'total_proxies': 0,
                'healthy_proxies': 0,
                'active_proxies': 0,
                'average_success_rate': 0.0,
                'average_response_time': 0.0
            }
        
        healthy_proxies = [p for p in self.proxies if p.is_healthy]
        active_proxies = [p for p in self.proxies if p.is_active]
        
        # 计算平均成功率
        total_requests = sum(p.total_requests for p in self.proxies)
        total_failed = sum(p.failed_requests for p in self.proxies)
        avg_success_rate = 0.0
        if total_requests > 0:
            avg_success_rate = (total_requests - total_failed) / total_requests
        
        # 计算平均响应时间
        response_times = [p.avg_response_time for p in self.proxies if p.avg_response_time > 0]
        avg_response_time = sum(response_times) / len(response_times) if response_times else 0.0
        
        return {
            'total_proxies': len(self.proxies),
            'healthy_proxies': len(healthy_proxies),
            'active_proxies': len(active_proxies),
            'average_success_rate': avg_success_rate,
            'average_response_time': avg_response_time,
            'last_health_check': self.last_health_check.isoformat() if self.last_health_check else None
        }
    
    def remove_unhealthy_proxies(self, min_success_rate: float = None) -> int:
        """
        移除不健康的代理
        
        Args:
            min_success_rate: 最小成功率阈值
            
        Returns:
            移除的代理数量
        """
        if min_success_rate is None:
            min_success_rate = self.min_success_rate
        
        before_count = len(self.proxies)
        
        self.proxies = [
            p for p in self.proxies 
            if p.success_rate >= min_success_rate and p.failure_count < self.max_failure_count
        ]
        
        removed_count = before_count - len(self.proxies)
        
        if removed_count > 0:
            logger.info(f"移除了 {removed_count} 个不健康的代理")
        
        return removed_count


# 全局代理池实例
_proxy_pool = None

def get_proxy_pool(config: Optional[Dict[str, Any]] = None) -> ProxyPool:
    """
    获取全局代理池实例
    
    Args:
        config: 配置字典
        
    Returns:
        代理池实例
    """
    global _proxy_pool
    if _proxy_pool is None:
        _proxy_pool = ProxyPool(config)
    return _proxy_pool


def init_proxy_pool_from_config(proxy_config: Dict[str, Any]) -> ProxyPool:
    """
    从配置初始化代理池
    
    Args:
        proxy_config: 代理配置
        
    Returns:
        代理池实例
    """
    pool = get_proxy_pool(proxy_config.get('settings', {}))
    
    if 'proxies' in proxy_config:
        pool.load_proxies_from_config(proxy_config['proxies'])
    
    return pool
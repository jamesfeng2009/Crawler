#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enhanced Playwright-based crawler with advanced human behavior simulation,
optimized request timing, improved cookie management, and conservative crawling strategy.
"""

import re
import json
import time
import random
import os
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

from playwright.sync_api import sync_playwright, Browser, BrowserContext, Page

# Import stealth mode if available
try:
    from playwright_stealth import stealth_sync
    STEALTH_AVAILABLE = True
except ImportError:
    STEALTH_AVAILABLE = False
    stealth_sync = None

from memory_price_monitor.crawlers.base import BaseCrawler, RawProduct, PricePoint
from memory_price_monitor.utils.errors import CrawlerError
from memory_price_monitor.utils.logging import get_business_logger
from memory_price_monitor.utils.proxy_pool import get_proxy_pool


logger = get_business_logger('enhanced_crawler')


class DailyCrawlLimiter:
    """管理每日爬取限制"""
    
    def __init__(self, max_daily_requests: int = 100, storage_path: str = "data/crawl_limits.json"):
        self.max_daily_requests = max_daily_requests
        self.storage_path = Path(storage_path)
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        self._load_limits()
    
    def _load_limits(self):
        """加载爬取限制数据"""
        try:
            if self.storage_path.exists():
                with open(self.storage_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.daily_counts = data.get('daily_counts', {})
                    self.last_reset = datetime.fromisoformat(data.get('last_reset', datetime.now().isoformat()))
            else:
                self.daily_counts = {}
                self.last_reset = datetime.now()
        except Exception as e:
            logger.warning(f"Failed to load crawl limits: {e}")
            self.daily_counts = {}
            self.last_reset = datetime.now()
    
    def _save_limits(self):
        """保存爬取限制数据"""
        try:
            data = {
                'daily_counts': self.daily_counts,
                'last_reset': self.last_reset.isoformat()
            }
            with open(self.storage_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"Failed to save crawl limits: {e}")
    
    def _reset_if_new_day(self):
        """如果是新的一天，重置计数"""
        now = datetime.now()
        if now.date() > self.last_reset.date():
            self.daily_counts = {}
            self.last_reset = now
            self._save_limits()
            logger.info("Daily crawl limits reset for new day")
    
    def can_crawl(self, source: str) -> bool:
        """检查是否可以继续爬取"""
        self._reset_if_new_day()
        current_count = self.daily_counts.get(source, 0)
        return current_count < self.max_daily_requests
    
    def record_request(self, source: str):
        """记录一次请求"""
        self._reset_if_new_day()
        self.daily_counts[source] = self.daily_counts.get(source, 0) + 1
        self._save_limits()
    
    def get_remaining_requests(self, source: str) -> int:
        """获取剩余请求数"""
        self._reset_if_new_day()
        current_count = self.daily_counts.get(source, 0)
        return max(0, self.max_daily_requests - current_count)


class CookieManager:
    """高级Cookie管理器"""
    
    def __init__(self, storage_path: str = "data/cookies"):
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
    
    def save_cookies(self, context: BrowserContext, domain: str):
        """保存浏览器上下文的cookies"""
        try:
            cookies = context.cookies()
            cookie_file = self.storage_path / f"{domain}_cookies.json"
            
            # 过滤和清理cookies
            valid_cookies = []
            for cookie in cookies:
                # 只保存有效的cookies
                if cookie.get('name') and cookie.get('value'):
                    # 移除敏感信息
                    clean_cookie = {
                        'name': cookie['name'],
                        'value': cookie['value'],
                        'domain': cookie.get('domain', ''),
                        'path': cookie.get('path', '/'),
                        'expires': cookie.get('expires', -1),
                        'httpOnly': cookie.get('httpOnly', False),
                        'secure': cookie.get('secure', False),
                        'sameSite': cookie.get('sameSite', 'Lax')
                    }
                    valid_cookies.append(clean_cookie)
            
            with open(cookie_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'cookies': valid_cookies,
                    'saved_at': datetime.now().isoformat(),
                    'domain': domain
                }, f, ensure_ascii=False, indent=2)
            
            logger.debug(f"Saved {len(valid_cookies)} cookies for {domain}")
            
        except Exception as e:
            logger.warning(f"Failed to save cookies for {domain}: {e}")
    
    def load_cookies(self, context: BrowserContext, domain: str) -> bool:
        """加载cookies到浏览器上下文"""
        try:
            cookie_file = self.storage_path / f"{domain}_cookies.json"
            
            if not cookie_file.exists():
                logger.debug(f"No saved cookies found for {domain}")
                return False
            
            with open(cookie_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # 检查cookies是否过期
            saved_at = datetime.fromisoformat(data.get('saved_at', '2020-01-01T00:00:00'))
            if datetime.now() - saved_at > timedelta(days=7):
                logger.debug(f"Cookies for {domain} are too old, skipping")
                return False
            
            cookies = data.get('cookies', [])
            if cookies:
                # 过滤未过期的cookies
                valid_cookies = []
                now_timestamp = int(time.time())
                
                for cookie in cookies:
                    expires = cookie.get('expires', -1)
                    if expires == -1 or expires > now_timestamp:
                        valid_cookies.append(cookie)
                
                if valid_cookies:
                    context.add_cookies(valid_cookies)
                    logger.debug(f"Loaded {len(valid_cookies)} cookies for {domain}")
                    return True
            
            return False
            
        except Exception as e:
            logger.warning(f"Failed to load cookies for {domain}: {e}")
            return False
    
    def clear_expired_cookies(self):
        """清理过期的cookie文件"""
        try:
            for cookie_file in self.storage_path.glob("*_cookies.json"):
                try:
                    with open(cookie_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    saved_at = datetime.fromisoformat(data.get('saved_at', '2020-01-01T00:00:00'))
                    if datetime.now() - saved_at > timedelta(days=30):
                        cookie_file.unlink()
                        logger.debug(f"Removed expired cookie file: {cookie_file.name}")
                        
                except Exception as e:
                    logger.warning(f"Error processing cookie file {cookie_file}: {e}")
                    
        except Exception as e:
            logger.warning(f"Failed to clear expired cookies: {e}")


class HumanBehaviorSimulator:
    """高级人类行为模拟器"""
    
    def __init__(self, page: Page):
        self.page = page
        self.viewport = page.viewport_size
    
    def simulate_reading_behavior(self, duration_range: Tuple[float, float] = (2.0, 8.0)):
        """模拟阅读行为"""
        try:
            duration = random.uniform(*duration_range)
            start_time = time.time()
            
            # 模拟眼球运动 - 随机移动鼠标
            while time.time() - start_time < duration:
                # 随机鼠标位置
                x = random.randint(100, self.viewport['width'] - 100)
                y = random.randint(100, self.viewport['height'] - 100)
                
                # 平滑移动鼠标
                self.page.mouse.move(x, y)
                
                # 随机停顿
                time.sleep(random.uniform(0.1, 0.5))
                
                # 偶尔点击（但不在链接上）
                if random.random() < 0.1:
                    self.page.mouse.click(x, y)
                    time.sleep(random.uniform(0.2, 0.8))
            
        except Exception as e:
            logger.warning(f"Error in reading behavior simulation: {e}")
    
    def simulate_scrolling_behavior(self, scroll_count: int = None):
        """模拟自然的滚动行为"""
        try:
            if scroll_count is None:
                scroll_count = random.randint(3, 8)
            
            for _ in range(scroll_count):
                # 随机滚动方向和距离
                if random.random() < 0.8:  # 80%向下滚动
                    scroll_y = random.randint(200, 600)
                else:  # 20%向上滚动
                    scroll_y = -random.randint(100, 300)
                
                # 执行滚动
                self.page.evaluate(f'window.scrollBy(0, {scroll_y})')
                
                # 滚动后停顿，模拟阅读
                pause_time = random.uniform(0.5, 2.0)
                time.sleep(pause_time)
                
                # 偶尔小幅度调整滚动位置
                if random.random() < 0.3:
                    adjustment = random.randint(-50, 50)
                    self.page.evaluate(f'window.scrollBy(0, {adjustment})')
                    time.sleep(random.uniform(0.2, 0.5))
            
        except Exception as e:
            logger.warning(f"Error in scrolling behavior simulation: {e}")
    
    def simulate_typing_behavior(self, text: str, element_selector: str):
        """模拟人类打字行为"""
        try:
            element = self.page.locator(element_selector).first
            if element.count() == 0:
                return False
            
            # 点击输入框
            element.click()
            time.sleep(random.uniform(0.2, 0.5))
            
            # 清空现有内容
            element.fill('')
            time.sleep(random.uniform(0.1, 0.3))
            
            # 逐字符输入，模拟真实打字速度
            for char in text:
                element.type(char)
                # 随机打字间隔，模拟不同的打字速度
                delay = random.uniform(0.05, 0.2)
                # 偶尔有较长停顿，模拟思考
                if random.random() < 0.1:
                    delay += random.uniform(0.3, 1.0)
                time.sleep(delay)
            
            # 输入完成后短暂停顿
            time.sleep(random.uniform(0.5, 1.5))
            return True
            
        except Exception as e:
            logger.warning(f"Error in typing behavior simulation: {e}")
            return False
    
    def simulate_page_interaction(self):
        """模拟页面交互行为"""
        try:
            # 随机移动鼠标到不同区域
            regions = [
                (100, 200, 400, 300),  # 顶部区域
                (100, 300, 400, 500),  # 中部区域
                (100, 500, 400, 700),  # 底部区域
            ]
            
            for region in random.sample(regions, random.randint(1, 3)):
                x = random.randint(region[0], region[2])
                y = random.randint(region[1], region[3])
                
                # 移动到区域
                self.page.mouse.move(x, y)
                time.sleep(random.uniform(0.3, 1.0))
                
                # 偶尔悬停在元素上
                if random.random() < 0.3:
                    time.sleep(random.uniform(0.5, 2.0))
            
        except Exception as e:
            logger.warning(f"Error in page interaction simulation: {e}")


class EnhancedPlaywrightCrawler(BaseCrawler):
    """增强版Playwright爬虫，具有高级人类行为模拟和保守爬取策略"""
    
    def __init__(self, source_name: str = 'enhanced_playwright', config: Optional[Dict[str, Any]] = None):
        """
        初始化增强版Playwright爬虫
        
        Args:
            source_name: 源名称
            config: 爬虫配置
        """
        super().__init__(source_name, config)
        
        # 保守的爬取策略配置
        self.min_delay = self.config.get('min_delay', 10.0)  # 增加到10-15秒
        self.max_delay = self.config.get('max_delay', 15.0)
        self.page_delay = self.config.get('page_delay', 20.0)  # 页面间延迟20秒
        self.max_pages = self.config.get('max_pages', 1)  # 限制为单页
        self.max_daily_requests = self.config.get('max_daily_requests', 50)  # 每日最多50个请求
        
        # 浏览器配置
        self.headless = self.config.get('headless', True)
        self.browser_type = self.config.get('browser_type', 'chromium')
        self.viewport_width = self.config.get('viewport_width', 1366)  # 更常见的分辨率
        self.viewport_height = self.config.get('viewport_height', 768)
        self.page_timeout = self.config.get('page_timeout', 60000)  # 增加到60秒
        
        # 人类行为模拟配置
        self.enable_human_behavior = self.config.get('enable_human_behavior', True)
        self.reading_time_range = self.config.get('reading_time_range', (3.0, 10.0))
        self.scroll_probability = self.config.get('scroll_probability', 0.8)
        
        # 初始化组件
        self.daily_limiter = DailyCrawlLimiter(self.max_daily_requests)
        self.cookie_manager = CookieManager()
        
        # 浏览器实例
        self.playwright = None
        self.browser = None
        self.context = None
        
        # 高级User-Agent池
        self.user_agents = [
            # Windows Chrome
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            # macOS Chrome
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            # Windows Edge
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
        ]
        
        # 清理过期cookies
        self.cookie_manager.clear_expired_cookies()
    
    def _check_daily_limit(self) -> bool:
        """检查每日爬取限制"""
        if not self.daily_limiter.can_crawl(self.source_name):
            remaining = self.daily_limiter.get_remaining_requests(self.source_name)
            raise CrawlerError(
                f"Daily crawl limit reached for {self.source_name}",
                {"remaining_requests": remaining, "max_daily": self.max_daily_requests}
            )
        return True
    
    def _init_browser(self):
        """初始化浏览器，包含高级反检测特性"""
        try:
            if not self.playwright:
                self.playwright = sync_playwright().start()
            
            if not self.browser:
                # 高级反检测启动参数
                browser_args = [
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor',
                    '--disable-extensions',
                    '--no-first-run',
                    '--disable-default-apps',
                    '--disable-background-timer-throttling',
                    '--disable-backgrounding-occluded-windows',
                    '--disable-renderer-backgrounding',
                    '--disable-field-trial-config',
                    '--disable-back-forward-cache',
                    '--disable-ipc-flooding-protection',
                    '--enable-features=NetworkService,NetworkServiceInProcess',
                    '--force-color-profile=srgb',
                    '--metrics-recording-only',
                    '--use-mock-keychain',
                ]
                
                if self.browser_type == 'chromium':
                    self.browser = self.playwright.chromium.launch(
                        headless=self.headless,
                        args=browser_args,
                        slow_mo=random.randint(50, 150)  # 随机减慢操作速度
                    )
                elif self.browser_type == 'firefox':
                    self.browser = self.playwright.firefox.launch(
                        headless=self.headless,
                        slow_mo=random.randint(50, 150)
                    )
                else:
                    self.browser = self.playwright.webkit.launch(
                        headless=self.headless,
                        slow_mo=random.randint(50, 150)
                    )
            
            if not self.context:
                self._create_browser_context()
            
            logger.info("Enhanced Playwright browser initialized successfully")
            
        except Exception as e:
            raise CrawlerError(
                f"Failed to initialize enhanced Playwright browser",
                {"error": str(e), "browser_type": self.browser_type}
            )
    
    def _create_browser_context(self):
        """创建高度真实的浏览器上下文"""
        # 选择随机User-Agent
        user_agent = random.choice(self.user_agents)
        
        # 生成真实的屏幕分辨率
        screen_resolutions = [
            (1920, 1080), (1366, 768), (1536, 864), (1440, 900), (1280, 720)
        ]
        screen_width, screen_height = random.choice(screen_resolutions)
        
        # 创建上下文选项
        context_options = {
            'user_agent': user_agent,
            'viewport': {'width': self.viewport_width, 'height': self.viewport_height},
            'screen': {'width': screen_width, 'height': screen_height},
            'locale': 'zh-CN',
            'timezone_id': 'Asia/Shanghai',
            'permissions': ['geolocation'],
            'geolocation': {
                'latitude': 39.9042 + random.uniform(-0.1, 0.1),  # 北京附近随机位置
                'longitude': 116.4074 + random.uniform(-0.1, 0.1)
            },
            'extra_http_headers': {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
            },
            'device_scale_factor': random.choice([1.0, 1.25, 1.5]),
            'is_mobile': False,
            'has_touch': False,
            'color_scheme': 'light',
            'reduced_motion': 'no-preference',
            'forced_colors': 'none',
        }
        
        # 添加代理配置（如果启用）
        if self.config.get('use_proxy', False):
            proxy_pool = get_proxy_pool()
            if proxy_pool:
                proxy_info = proxy_pool.get_next_proxy()
                if proxy_info:
                    context_options['proxy'] = {
                        'server': f"{proxy_info.protocol}://{proxy_info.host}:{proxy_info.port}"
                    }
                    if proxy_info.username and proxy_info.password:
                        context_options['proxy']['username'] = proxy_info.username
                        context_options['proxy']['password'] = proxy_info.password
                    
                    logger.info(f"Using proxy: {proxy_info.host}:{proxy_info.port}")
        
        self.context = self.browser.new_context(**context_options)
        
        # 设置默认超时
        self.context.set_default_timeout(self.page_timeout)
        
        # 加载cookies
        domain = self._extract_domain_from_url(self.config.get('base_url', 'https://www.zol.com.cn'))
        self.cookie_manager.load_cookies(self.context, domain)
    
    def _extract_domain_from_url(self, url: str) -> str:
        """从URL提取域名"""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            return parsed.netloc.replace('www.', '')
        except Exception:
            return 'unknown'
    
    def _create_stealth_page(self) -> Page:
        """创建具有隐身模式的页面"""
        if not self.context:
            self._init_browser()
        
        page = self.context.new_page()
        
        # 应用隐身模式
        if STEALTH_AVAILABLE and stealth_sync:
            stealth_sync(page)
        else:
            logger.warning("Stealth mode not available, using enhanced anti-detection")
        
        # 高级反检测脚本
        page.add_init_script("""
            // 覆盖 navigator.webdriver
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            
            // 覆盖 navigator.plugins
            Object.defineProperty(navigator, 'plugins', {
                get: () => [
                    {name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer'},
                    {name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai'},
                    {name: 'Native Client', filename: 'internal-nacl-plugin'}
                ]
            });
            
            // 覆盖 navigator.languages
            Object.defineProperty(navigator, 'languages', {
                get: () => ['zh-CN', 'zh', 'en-US', 'en']
            });
            
            // 覆盖 navigator.permissions
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );
            
            // 添加 chrome 对象
            if (!window.chrome) {
                window.chrome = {
                    runtime: {},
                    loadTimes: function() {
                        return {
                            commitLoadTime: Date.now() / 1000 - Math.random(),
                            finishDocumentLoadTime: Date.now() / 1000 - Math.random(),
                            finishLoadTime: Date.now() / 1000 - Math.random(),
                            firstPaintAfterLoadTime: 0,
                            firstPaintTime: Date.now() / 1000 - Math.random(),
                            navigationType: 'Other',
                            npnNegotiatedProtocol: 'h2',
                            requestTime: Date.now() / 1000 - Math.random(),
                            startLoadTime: Date.now() / 1000 - Math.random(),
                            wasAlternateProtocolAvailable: false,
                            wasFetchedViaSpdy: true,
                            wasNpnNegotiated: true
                        };
                    },
                    csi: function() {
                        return {
                            onloadT: Date.now(),
                            pageT: Date.now() - Math.random() * 1000,
                            tran: 15
                        };
                    }
                };
            }
            
            // 模拟真实的屏幕属性
            Object.defineProperty(screen, 'availTop', {get: () => 0});
            Object.defineProperty(screen, 'availLeft', {get: () => 0});
            Object.defineProperty(screen, 'availHeight', {get: () => screen.height});
            Object.defineProperty(screen, 'availWidth', {get: () => screen.width});
            Object.defineProperty(screen, 'colorDepth', {get: () => 24});
            Object.defineProperty(screen, 'pixelDepth', {get: () => 24});
            
            // 添加真实的 connection 信息
            Object.defineProperty(navigator, 'connection', {
                get: () => ({
                    downlink: 10,
                    effectiveType: '4g',
                    rtt: 50,
                    saveData: false
                })
            });
            
            // 覆盖 Date.prototype.getTimezoneOffset
            const originalGetTimezoneOffset = Date.prototype.getTimezoneOffset;
            Date.prototype.getTimezoneOffset = function() {
                return -480; // UTC+8 (China Standard Time)
            };
        """)
        
        return page
    
    def _simulate_human_page_behavior(self, page: Page, duration_range: Tuple[float, float] = None):
        """模拟人类页面行为"""
        if not self.enable_human_behavior:
            return
        
        if duration_range is None:
            duration_range = self.reading_time_range
        
        try:
            simulator = HumanBehaviorSimulator(page)
            
            # 模拟阅读行为
            simulator.simulate_reading_behavior(duration_range)
            
            # 模拟滚动行为
            if random.random() < self.scroll_probability:
                simulator.simulate_scrolling_behavior()
            
            # 模拟页面交互
            if random.random() < 0.4:  # 40%概率进行页面交互
                simulator.simulate_page_interaction()
            
        except Exception as e:
            logger.warning(f"Error in human behavior simulation: {e}")
    
    def _intelligent_delay(self, base_delay: float = None, variance: float = 0.3):
        """智能延迟，模拟真实用户行为"""
        if base_delay is None:
            base_delay = random.uniform(self.min_delay, self.max_delay)
        
        # 添加随机变化
        actual_delay = base_delay * (1 + random.uniform(-variance, variance))
        
        # 确保最小延迟
        actual_delay = max(actual_delay, 5.0)
        
        logger.debug(f"Intelligent delay: {actual_delay:.2f} seconds")
        time.sleep(actual_delay)
    
    def fetch_products(self) -> List[RawProduct]:
        """
        获取产品列表，使用保守策略和高级人类行为模拟
        
        Returns:
            产品列表
            
        Raises:
            CrawlerError: 如果获取失败
        """
        # 检查每日限制
        self._check_daily_limit()
        
        all_products = []
        
        try:
            self._init_browser()
            
            # 记录请求
            self.daily_limiter.record_request(self.source_name)
            
            # 使用保守策略：只爬取一个主要页面
            logger.info("Starting conservative crawling with enhanced human behavior simulation")
            
            # 选择最佳的爬取目标
            target_url = self._select_optimal_target()
            products = self._crawl_single_page(target_url)
            all_products.extend(products)
            
            # 保存cookies
            domain = self._extract_domain_from_url(target_url)
            self.cookie_manager.save_cookies(self.context, domain)
            
            # 去重
            unique_products = self._deduplicate_products(all_products)
            
            logger.info(f"Conservative crawling completed: {len(unique_products)} unique products from {len(all_products)} total")
            
            return unique_products
            
        except Exception as e:
            raise CrawlerError(
                f"Failed to fetch products using enhanced crawler",
                {"error": str(e), "source": self.source_name}
            )
    
    def _select_optimal_target(self) -> str:
        """选择最佳的爬取目标"""
        # 优先选择分类页面，因为通常比搜索页面更稳定
        targets = [
            "https://detail.zol.com.cn/memory/",  # ZOL内存分类页
            "https://search.zol.com.cn/s/all.php?kword=内存条",  # ZOL搜索页
        ]
        
        # 可以根据历史成功率选择最佳目标
        return targets[0]  # 暂时选择分类页面
    
    def _crawl_single_page(self, url: str) -> List[RawProduct]:
        """爬取单个页面，使用高级人类行为模拟"""
        products = []
        page = None
        
        try:
            page = self._create_stealth_page()
            
            logger.info(f"Navigating to: {url}")
            
            # 导航到页面
            page.goto(url, wait_until='networkidle', timeout=self.page_timeout)
            
            # 检查是否被重定向或阻止
            current_url = page.url
            if self._is_blocked_or_redirected(current_url, url):
                logger.warning(f"Detected blocking or redirection: {current_url}")
                return products
            
            # 等待页面完全加载
            self._wait_for_page_load(page)
            
            # 模拟人类浏览行为
            self._simulate_human_page_behavior(page, (5.0, 12.0))  # 更长的阅读时间
            
            # 提取产品
            products = self._extract_products_from_page(page)
            
            logger.info(f"Extracted {len(products)} products from page")
            
        except Exception as e:
            logger.error(f"Failed to crawl page {url}: {str(e)}")
        finally:
            if page:
                # 模拟关闭页面前的行为
                if self.enable_human_behavior and random.random() < 0.3:
                    time.sleep(random.uniform(1.0, 3.0))
                page.close()
        
        return products
    
    def _is_blocked_or_redirected(self, current_url: str, expected_url: str) -> bool:
        """检查是否被阻止或重定向"""
        # 检查常见的阻止指示器
        blocked_indicators = [
            'risk_handler',
            'captcha',
            'verify',
            'blocked',
            'forbidden',
            'error'
        ]
        
        current_lower = current_url.lower()
        for indicator in blocked_indicators:
            if indicator in current_lower:
                return True
        
        # 检查是否被重定向到首页
        from urllib.parse import urlparse
        current_parsed = urlparse(current_url)
        expected_parsed = urlparse(expected_url)
        
        if (current_parsed.netloc == expected_parsed.netloc and 
            current_parsed.path in ['/', '/index.html', '/index.php']):
            return True
        
        return False
    
    def _wait_for_page_load(self, page: Page):
        """等待页面完全加载"""
        try:
            # 等待网络空闲
            page.wait_for_load_state('networkidle', timeout=30000)
            
            # 等待主要内容加载
            selectors_to_wait = [
                '#J_PicMode',  # ZOL主要产品容器
                'li[data-follow-id]',  # ZOL产品项
                '.list-item',  # 通用列表项
                '.product-item'  # 通用产品项
            ]
            
            for selector in selectors_to_wait:
                try:
                    page.wait_for_selector(selector, timeout=10000)
                    logger.debug(f"Found content with selector: {selector}")
                    break
                except Exception:
                    continue
            
        except Exception as e:
            logger.warning(f"Page load wait timeout: {e}")
    
    def _extract_products_from_page(self, page: Page) -> List[RawProduct]:
        """从页面提取产品信息"""
        products = []
        
        try:
            # 尝试多种选择器策略
            selectors = [
                '#J_PicMode li[data-follow-id]',  # ZOL主容器内的产品
                'li[data-follow-id]',  # 任何位置的产品
                '.pic-mode-box li',  # 图片模式容器
                '.product-list li'  # 产品列表
            ]
            
            product_elements = []
            for selector in selectors:
                elements = page.locator(selector).all()
                if elements:
                    product_elements = elements
                    logger.debug(f"Found {len(elements)} products using selector: {selector}")
                    break
            
            if not product_elements:
                logger.warning("No product elements found on page")
                return products
            
            # 限制提取数量以保持保守策略
            max_products = min(len(product_elements), 20)  # 最多20个产品
            
            for i, element in enumerate(product_elements[:max_products]):
                try:
                    product = self._extract_single_product(element)
                    if product:
                        products.append(product)
                        
                        # 在产品提取间添加小延迟
                        if i < max_products - 1:  # 不在最后一个产品后延迟
                            time.sleep(random.uniform(0.1, 0.3))
                            
                except Exception as e:
                    logger.warning(f"Failed to extract product {i}: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Failed to extract products from page: {e}")
        
        return products
    
    def _extract_single_product(self, element) -> Optional[RawProduct]:
        """从单个元素提取产品信息"""
        try:
            # 提取产品ID
            product_id = element.get_attribute('data-follow-id')
            if not product_id:
                return None
            
            # 清理产品ID
            if product_id.startswith('p'):
                product_id = product_id[1:]
            
            # 提取产品链接和标题
            link_element = element.locator('h3 a').first
            if link_element.count() == 0:
                link_element = element.locator('a[href*="/memory/index"]').first
                if link_element.count() == 0:
                    return None
            
            product_url = link_element.get_attribute('href')
            if not product_url:
                return None
            
            # 确保URL是绝对路径
            if not product_url.startswith('http'):
                base_url = "https://www.zol.com.cn"
                product_url = f"{base_url}{product_url}" if product_url.startswith('/') else f"{base_url}/{product_url}"
            
            # 提取标题
            title = link_element.get_attribute('title') or link_element.inner_text()
            if not title or len(title.strip()) < 5:
                return None
            
            title = title.strip()
            
            # 提取价格
            price = 0.0
            price_element = element.locator('.price-type').first
            if price_element.count() > 0:
                price_text = price_element.inner_text()
                try:
                    price_clean = re.sub(r'[^\d.]', '', price_text)
                    if price_clean:
                        price = float(price_clean)
                except (ValueError, TypeError):
                    price = 0.0
            
            # 提取品牌
            brand = self._extract_brand_from_title(title)
            
            # 提取图片URL
            img_url = ''
            img_element = element.locator('img').first
            if img_element.count() > 0:
                img_src = img_element.get_attribute('src') or img_element.get_attribute('.src')
                if img_src:
                    if not img_src.startswith('http'):
                        img_src = f"https:{img_src}" if img_src.startswith('//') else f"https://www.zol.com.cn{img_src}"
                    img_url = img_src
            
            # 构建原始数据
            raw_data = {
                'name': title,
                'current_price': price,
                'market_price': price,
                'brand': brand,
                'image_url': img_url,
                'zol_id': product_id,
                'extraction_method': 'enhanced_playwright'
            }
            
            return RawProduct(
                source=self.source_name,
                product_id=product_id,
                raw_data=raw_data,
                url=product_url,
                timestamp=datetime.now()
            )
            
        except Exception as e:
            logger.warning(f"Failed to extract single product: {e}")
            return None
    
    def _extract_brand_from_title(self, title: str) -> str:
        """从标题提取品牌名称"""
        brands = [
            'CORSAIR', '海盗船',
            'KINGSTON', '金士顿',
            'G.SKILL', '芝奇',
            'CRUCIAL', '英睿达',
            'SAMSUNG', '三星',
            'SK HYNIX', 'SK海力士',
            'ADATA', '威刚',
            'TEAMGROUP', '十铨',
            'MUSHKIN',
            'PATRIOT', '博帝',
            'GEIL', '金邦',
            'APACER', '宇瞻'
        ]
        
        title_upper = title.upper()
        for brand in brands:
            if brand.upper() in title_upper:
                return brand
        
        # 尝试提取第一个单词作为品牌
        words = title.split()
        if words:
            return words[0]
        
        return ''
    
    def _deduplicate_products(self, products: List[RawProduct]) -> List[RawProduct]:
        """去除重复产品"""
        unique_products = {}
        for product in products:
            if product.product_id not in unique_products:
                unique_products[product.product_id] = product
        
        return list(unique_products.values())
    
    def cleanup(self) -> None:
        """清理资源"""
        try:
            if self.context:
                # 保存cookies
                try:
                    domain = self._extract_domain_from_url(self.config.get('base_url', 'https://www.zol.com.cn'))
                    self.cookie_manager.save_cookies(self.context, domain)
                except Exception as e:
                    logger.warning(f"Failed to save cookies during cleanup: {e}")
                
                self.context.close()
                self.context = None
            
            if self.browser:
                self.browser.close()
                self.browser = None
            
            if self.playwright:
                self.playwright.stop()
                self.playwright = None
            
            logger.info("Enhanced Playwright resources cleaned up")
            
        except Exception as e:
            logger.warning(f"Error during cleanup: {e}")
    
    def get_daily_stats(self) -> Dict[str, Any]:
        """获取每日统计信息"""
        return {
            'source': self.source_name,
            'requests_made': self.daily_limiter.daily_counts.get(self.source_name, 0),
            'requests_remaining': self.daily_limiter.get_remaining_requests(self.source_name),
            'max_daily_requests': self.max_daily_requests,
            'last_reset': self.daily_limiter.last_reset.isoformat()
        }
    
    def __del__(self):
        """析构函数确保清理"""
        self.cleanup()
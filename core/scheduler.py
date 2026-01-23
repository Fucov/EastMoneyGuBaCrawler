#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
24小时持续调度器
循环爬取所有A股数据，智能管理IP池
"""

import time
import sys
import configparser
from pathlib import Path
import threading

# 添加父目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.stock_loader import StockLoader
from core.proxy_manager import ProxyManager
from storage.logger import get_logger


class Crawler24HScheduler:
    """
    24小时持续调度器
    
    功能：
    - 自动获取所有A股代码
    - 循环爬取每只股票
    - IP池智能监控
    - 低于阈值自动补充
    - 完整日志记录
    """
    
    def __init__(self, config_path='config.ini'):
        """初始化调度器"""
        # 读取配置
        self.config = configparser.ConfigParser()
        self.config.read(config_path, encoding='utf-8')
        
        # 日志
        self.logger = get_logger('scheduler')
        
        # 股票加载器
        self.stock_loader = StockLoader(
            exclude_st=True,
            exclude_delisted=True
        )
        
        # 代理管理器
        redis_config = {
            'redis_host': self.config.get('Redis', 'host'),
            'redis_port': self.config.getint('Redis', 'port'),
            'redis_password': self.config.get('Redis', 'password') or None,
            'redis_db': self.config.getint('Redis', 'db'),
            'cache_key': self.config.get('Redis', 'proxy_cache_key'),
            'min_threshold': self.config.getint('Proxy', 'min_count')
        }
        self.proxy_manager = ProxyManager(**redis_config)
        
        # 调度配置
        self.mode = self.config.get('Scheduler', 'mode')
        self.interval = self.config.getint('Scheduler', 'interval')
        self.stock_delay = self.config.getint('Scheduler', 'stock_delay')
        self.ip_check_interval = self.config.getint('Scheduler', 'ip_check_interval')
        
        # 代理配置
        self.min_proxy_count = self.config.getint('Proxy', 'min_count')
        self.target_proxy_count = self.config.getint('Proxy', 'target_count')
        
        # 统计
        self.total_crawled = 0
        self.total_failed = 0
        
        # 代理池守护线程控制
        self.proxy_daemon_running = False
        self.proxy_daemon_thread = None
        self.use_free_proxy = self.config.getboolean('proxies', 'use_free_proxy_pool', fallback=False)
        
        # 启动代理池守护线程（如果使用免费代理）
        if self.use_free_proxy:
            self._start_proxy_daemon()
    
    def check_and_refill_proxies(self):
        """检查并补充代理池"""
        current_count = self.proxy_manager.count()
        
        if current_count < self.min_proxy_count:
            self.logger.warning(f"IP池不足（{current_count}个），开始补充...")
            try:
                self.proxy_manager.refill_pool(
                    target_count=self.target_proxy_count,
                    max_workers=30
                )
                new_count = self.proxy_manager.count()
                self.logger.info(f"补充完成，当前{new_count}个代理")
                return True
            except Exception as e:
                self.logger.error(f"补充代理失败: {e}")
                return False
        
        return True
    
    def _start_proxy_daemon(self):
        """启动代理池守护线程"""
        if self.proxy_daemon_running:
            return
        
        self.proxy_daemon_running = True
        self.proxy_daemon_thread = threading.Thread(
            target=self._proxy_pool_daemon,
            daemon=True,  # 守护线程，主进程退出时自动结束
            name="ProxyPoolDaemon"
        )
        self.proxy_daemon_thread.start()
        self.logger.info("✓ 代理池守护线程已启动")
    
    def _proxy_pool_daemon(self):
        """
        代理池守护线程 - 持续监控并补充代理池
        每5分钟检查一次，低于阈值自动补充
        """
        min_threshold = self.config.getint('Proxy', 'min_count', fallback=5)
        check_interval = 300  # 每5分钟检查一次
        
        self.logger.info(f"代理池守护线程运行中 (阈值: {min_threshold}, 间隔: {check_interval}秒)")
        
        # 首次加载：尝试从文件加载，失败则建立新池
        if not self.proxy_manager.load_from_file('valid_proxies.txt'):
            self.logger.info("首次运行，建立代理池...")
            self.proxy_manager.build_pool(max_workers=50, max_per_source=200)
            self.proxy_manager.save_to_file('valid_proxies.txt')
        
        while self.proxy_daemon_running:
            try:
                current_count = self.proxy_manager.count()
                
                if current_count < min_threshold:
                    self.logger.warning(f"⚠️ 代理池不足: {current_count}/{min_threshold}，开始补充...")
                    
                    # 先重新验证现有代理
                    self.proxy_manager.revalidate_pool()
                    
                    # 如果仍不足，抓取新代理
                    if self.proxy_manager.count() < min_threshold:
                        self.logger.info("重新抓取代理...")
                        self.proxy_manager.build_pool(max_workers=50, max_per_source=200)
                        self.proxy_manager.save_to_file('valid_proxies.txt')
                        
                        self.logger.info(f"✓ 代理池补充完成: {self.proxy_manager.count()}个可用")
                else:
                    self.logger.debug(f"代理池健康: {current_count}个可用")
                
                # 等待下次检查
                time.sleep(check_interval)
                
            except Exception as e:
                self.logger.error(f"代理池守护线程异常: {e}")
                time.sleep(60)  # 异常后等待1分钟再重试
    
    def crawl_single_stock(self, stock_code: str) -> bool:
        """
        爬取单只股票 - 使用main_class.py的完整逻辑
        
        main()方法会自动：
        1. 调用get_total_pages()动态检测每种类型(news/report/notice)的实际页数
        2. 使用ThreadPoolExecutor并发爬取多页
        3. 每个请求动态轮换UserAgent
        4. 从proxy_pool获取随机代理
        5. 自动重试和错误处理
        
        Args:
            stock_code: 股票代码
        
        Returns:
            是否成功
        """
        try:
            self.logger.info(f"开始爬取 {stock_code}")
            
            # 导入爬虫类
            from core.crawler import guba_comments
            import configparser
            
            # 重新读取config.ini以确保获取所有配置
            config = configparser.ConfigParser()
            config.read('config.ini', encoding='utf-8')
            
            # 创建爬虫实例 - 不传递pages_start/pages_end
            # main()会自动调用get_total_pages()检测每种类型的实际页数
            crawler = guba_comments(
                config_path='config.ini',
                config=config,
                secCode=stock_code,
                pages_start=1,  # main()方法不使用此参数
                pages_end=1,    # main()方法不使用此参数
                num_start=0,
                MongoDB=True,
                collectionName='stock_news',
                full_text=False,
                max_workers=12  # 并发爬取的线程数
            )
            
            # 执行爬取
            # main()内部会：
            # - 遍历3种类型: news, report, notice
            # - 每种类型调用get_total_pages()获取实际页数
            # - 使用ThreadPoolExecutor并发爬取所有页面
            # - 每个请求使用随机UserAgent和代理
            crawler.main()
            
            self.logger.info(f"✓ {stock_code} 爬取完成")
            return True
            
        except KeyboardInterrupt:
            self.logger.warning(f"用户中断 {stock_code} 的爬取")
            raise  # 向上传递中断信号
        except Exception as e:
            self.logger.error(f"❌ 爬取 {stock_code} 失败: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return False
    
    def run_single_round(self):
        """运行单轮爬取"""
        self.logger.info("="*60)
        self.logger.info("开始新一轮爬取")
        self.logger.info("="*60)
        
        # 1. 获取股票列表
        stocks = self.stock_loader.get_all_stocks()
        if not stocks:
            self.logger.error("未获取到股票列表，跳过本轮")
            return
        
        self.logger.info(f"本轮目标：{len(stocks)}只股票")
        
        # 2. 检查IP池
        if not self.check_and_refill_proxies():
            self.logger.error("IP池不足且补充失败，等待1小时后重试")
            time.sleep(3600)
            return
        
        # 3. 循环爬取
        for i, stock_code in enumerate(stocks, 1):
            # 定期检查IP池
            if i % 10 == 0:
                self.check_and_refill_proxies()
            
            # 爬取
            self.logger.info(f"进度 {i}/{len(stocks)}: {stock_code}")
            
            success = self.crawl_single_stock(stock_code)
            if success:
                self.total_crawled += 1
            else:
                self.total_failed += 1
            
            # 延迟
            time.sleep(self.stock_delay)
            
            # 如果IP池耗尽，补充
            if self.proxy_manager.count() < self.min_proxy_count:
                self.logger.warning("IP池耗尽，暂停补充")
                if not self.check_and_refill_proxies():
                    self.logger.error("补充失败，等待30分钟")
                    time.sleep(1800)
        
        # 4. 本轮统计
        self.logger.info("="*60)
        self.logger.info(f"本轮完成：成功{self.total_crawled}，失败{self.total_failed}")
        self.logger.info("="*60)
    
    def run(self):
        """主循环"""
        self.logger.info("调度器启动")
        self.logger.info(f"模式: {self.mode}")
        self.logger.info(f"轮次间隔: {self.interval}秒")
        self.logger.info(f"股票间延迟: {self.stock_delay}秒")
        self.logger.info(f"IP池阈值: {self.min_proxy_count}")
        
        # 初始检查
        if self.proxy_manager.count() == 0:
            self.logger.info("首次运行，建立代理池...")
            self.proxy_manager.build_pool(max_workers=50, max_per_source=100)
        
        # 主循环
        round_number = 0
        while True:
            round_number += 1
            self.logger.info(f"\n第 {round_number} 轮")
            
            try:
                self.run_single_round()
            except Exception as e:
                self.logger.exception(f"轮次{round_number}异常")
            
            # 单次模式，运行一轮后退出
            if self.mode == 'once':
                self.logger.info("单次模式，退出")
                break
            
            # 等待下一轮
            self.logger.info(f"等待{self.interval}秒...")
            time.sleep(self.interval)


if __name__ == '__main__':
    # 测试
    scheduler = Crawler24HScheduler()
    scheduler.run()

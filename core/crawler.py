# -*- coding: utf-8 -*-
# @Time    : 2023/2/11 21:27
# @Author  : Euclid-Jie
# @File    : main_class.py
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import logging
from datetime import datetime

# from retrying import retry
from concurrent.futures import ThreadPoolExecutor
# from Utils.MongoClient import MongoClient
# from Utils.EuclidDataTools import CsvClient
# from TreadCrawler import RedisClient

import configparser
from storage.database_client import DatabaseManager
from core.user_agent_manager import get_user_agent_manager
import time
import random

import sys

from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from pymongo.errors import BulkWriteError
from core.proxy_manager import ProxyManager  # 使用统一的代理池管理器


# 定义自定义异常类
class NetworkException(Exception):
    pass


class ServerException(Exception):
    pass


class ContentChangedException(Exception):
    pass


class guba_comments:
    """
    this class is designed for get hot comments for guba, have two method which can be set at def get_data()
    1、all: https://guba.eastmoney.com/list,600519_1.html, secCode: 600519, page: 1
    2、hot: https://guba.eastmoney.com/list,600519,99_1.html secCode: 600519, page: 1

    because to the ip control, this need to set proxies pools
    by using proxies https://www.kuaidaili.com/usercenter/overview/, can solve this problem

    Program characteristics:
        1、default write data to mongoDB, by init "MogoDB=False", can switch to write data to csv file
        2、Use retry mechanism, once rise error, the program will restart at the least page and num (each page has 80 num)

    """

    failed_proxies = {}
    proxy_fail_times_treshold = 3

    def __init__(
        self,
        config_path: str,
        config: configparser.ConfigParser,
        secCode,
        pages_start: int = 0,
        pages_end: int = 100,
        num_start: int = 0,
        MongoDB: bool = True,
        collectionName: str = "default",
        full_text: bool = False,
        max_workers: int = 8,  # 新增：多线程并发数
    ):
        self.config_path = config_path
        self.db_manager = DatabaseManager(self.config_path)

        # 参数初始化 - 修复secCode未赋值的bug
        if isinstance(secCode, int):
            # 补齐6位数
            self.secCode = str(secCode).zfill(6)
        elif isinstance(secCode, str):
            self.secCode = secCode
        else:
            self.secCode = str(secCode)

        self.pages_start = pages_start
        self.pages_end = pages_end
        self.num_start = num_start
        self.full_text = full_text
        self._year = pd.Timestamp.now().year

        # 年份推断：用于处理publish_time没有年份的问题
        self.current_year = datetime.now().year
        self.last_month = None  # 记录上一条新闻的月份，用于推断年份

        # 增量更新优化：页级重复计数器（检测连续重复页面）
        self.consecutive_duplicate_pages = 0
        self.duplicate_page_threshold = 2  # 连续2页全为重复则跳过该栏目

        # 多线程配置
        self.max_workers = max_workers

        # redis client for full_text_Crawler
        # 检查是否启用Redis
        self.redis_enabled = config.getboolean("Redis", "enabled", fallback=True)
        if self.redis_enabled:
            self.redis_client = self.db_manager.get_redis_client()
        else:
            self.redis_client = None

        # 配置文件可以覆盖参数
        if config.has_option("mainClass", "secCode"):
            self.secCode = config.get("mainClass", "secCode")
            print(
                f"secCode has been overridden by {self.secCode} in the configuration file."
            )
        if config.has_option("mainClass", "pages_start"):
            self.pages_start = int(config.get("mainClass", "pages_start"))
            print(
                f"pages_start has been overridden by {self.pages_start} in the configuration file."
            )
        if config.has_option("mainClass", "pages_end"):
            self.pages_end = int(config.get("mainClass", "pages_end"))
            print(
                f"pages_end has been overridden by {self.pages_end} in the configuration file."
            )
        if config.has_option("mainClass", "collectionname"):
            collectionName = config.get("mainClass", "collectionname")
            print(
                f"collectionName has been overridden by {collectionName} in the configuration file."
            )
        if config.has_option("mainClass", "max_workers"):
            self.max_workers = int(config.get("mainClass", "max_workers"))
            print(
                f"max_workers has been overridden by {self.max_workers} in the configuration file."
            )

        # choose one save method, default MongoDB
        # 1、csv
        # 2、MongoDB
        # 帖子数据存储到配置的数据库
        # self.log_path = config.get("logging", "log_file")

        # print("这是读取到标题爬取的log"+self.log_path)

        # 从配置文件读取数据库名称和Collection名称
        db_name = config.get("MongoDB", "database", fallback="guba")
        # 使用统一的Collection名称：stock_news（所有股票共用）
        collection_name = config.get(
            "mainClass", "collectionName", fallback="stock_news"
        )
        # 使用 database_client 的 MongoDB 客户端
        self.col = self.db_manager.get_mongo_client(db_name, collection_name).collection
        self.collection_name = collection_name  # 保存Collection名称供后续使用

        # log setting
        # log_format = "%(levelname)s %(asctime)s %(filename)s %(lineno)d %(message)s"
        # logging.basicConfig(filename=self.log_path, format=log_format, level=logging.INFO,encoding = 'utf-8')

        # 设置日志级别
        log_level_str = config.get("logging", "log_level", fallback="INFO").upper()
        log_level = getattr(logging, log_level_str, logging.INFO)

        self.logger = logging.getLogger(
            config.get("logging", "log_file", fallback="main_controller.log")
        )
        if not self.logger.handlers:
            self.logger.setLevel(log_level)
            # 使用 sys.stdout 确保日志输出到 run.log (因为 start.sh 中 > run.log)
            handler = logging.StreamHandler(sys.stdout)
            handler.setLevel(log_level)
            # 设置该handler的格式
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        # User-Agent管理器初始化
        self.ua_manager = get_user_agent_manager("random")  # 使用随机模式

        # header setting - 使用动态User-Agent
        self.header = {
            "User-Agent": self.ua_manager.get_user_agent(),
        }

        # 代理配置 - 统一从 [proxies] 部分读取
        proxy_enabled = config.getboolean("proxies", "enabled", fallback=False)

        # 初始化代理池
        self.proxy_pool = None
        if proxy_enabled:
            print("\n🔧 初始化代理池...")
            # 从配置读取代理池参数
            min_proxy_threshold = config.getint(
                "proxies", "min_proxy_count", fallback=5
            )
            target_proxy_count = config.getint("proxies", "target_count", fallback=20)

            self.proxy_pool = ProxyManager(
                redis_host=config.get("Redis", "host", fallback="localhost"),
                redis_port=config.getint("Redis", "port", fallback=6379),
                redis_password=config.get("Redis", "password", fallback="") or None,
                redis_db=config.getint("Redis", "db", fallback=0),
                cache_key=config.get(
                    "Redis", "proxy_cache_key", fallback="guba:proxies:valid"
                ),
                min_threshold=min_proxy_threshold,
                target_count=target_proxy_count,
                context=self.secCode,
                config_path=self.config_path,  # 传递配置文件路径
            )

            # 检查代理池状态
            if self.proxy_pool.count() == 0:
                print("⚠️ 代理池为空，尝试使用现有代理或等待调度器补充...")
            else:
                current_count = self.proxy_pool.count()
                if self.proxy_pool.provider == "kdl":
                    mode = "快代理(KDL)"
                elif self.proxy_pool.provider == "qingguo":
                    mode = "青果代理"
                else:
                    mode = "免费代理源"
                print(f"✅ 代理池就绪 (模式: {mode}, 当前可用: {current_count})")
            print("")

            self.proxies = None  # 使用代理池而非固定代理
            self.backup_proxies = None
        else:
            self.proxies = None
            self.backup_proxies = None
            print("⚠️ 代理已禁用 - 直接连接")

        self.use_backup_proxy = False

    @staticmethod
    def clear_str(str_raw):
        for pat in ["\n", " ", " ", "\r", "\xa0", "\n\r\n"]:
            str_raw.strip(pat).replace(pat, "")
        return str_raw

    @staticmethod
    def run_thread_pool_sub(target, args, max_work_count):
        with ThreadPoolExecutor(max_workers=max_work_count) as t:
            res = [t.submit(target, i) for i in args]
            return res

    def get_total_pages(self, content_type="news"):
        """
        自动检测指定内容类型的总页数

        通过解析页面中的JavaScript变量 var article_list = {...}
        提取 count 字段（总帖子数），然后计算总页数 = ceil(count / 80)

        Args:
            content_type: 'news' (资讯) | 'report' (研报) | 'notice' (公告)

        Returns:
            tuple: (total_pages, total_count)
            int: 总页数，如果检测失败返回0
            int: 总条数，如果检测失败返回0
        """
        import math
        # import re  <-- Removed unused import

        # 类型映射
        type_map = {"news": "1,f", "report": "2,f", "notice": "3,f"}

        if content_type not in type_map:
            self.logger.error(f"未知的内容类型: {content_type}")
            return 0, 0

        # 构造第一页URL
        url = f"https://guba.eastmoney.com/list,{self.secCode},{type_map[content_type]}.html"

        max_retries = 5  # User requested 5 retries for this critical step
        for attempt in range(max_retries):
            try:
                soup, proxy_used = self.get_soup_form_url(url)
                if not soup:
                    if attempt < max_retries - 1:
                        time.sleep(random.uniform(1, 3))
                        continue
                    return 0, 0

                # 查找所有script标签
                scripts = soup.find_all("script")

                # Flag to track if we found a "bad" result that necessitates a force retry
                force_retry = False

                for script in scripts:
                    script_text = script.string
                    if script_text and "var article_list" in script_text:
                        # 找到包含 article_list 的脚本
                        # [Refactor] 校验逻辑变更：不再检查count, 而是检查user_nickname后缀
                        try:
                            import json

                            # Use raw_decode to parse the JSON object starting from '{'
                            start_index = script_text.find("{")
                            if start_index != -1:
                                decoder = json.JSONDecoder()
                                article_list_data, _ = decoder.raw_decode(
                                    script_text[start_index:]
                                )

                                # Extract items list
                                items = article_list_data.get("re", [])
                                if items:
                                    # Check if ALL items have user_nickname ending with "资讯"
                                    # Note: We need to be careful if the list is empty, but if it has items, check them.
                                    # If list is empty, maybe it's valid (no news), or maybe invalid.
                                    # Assuming if we get data, we validate it.

                                    invalid_nicknames = []
                                    for item in items:
                                        nickname = item.get("user_nickname", "")
                                        if not nickname.endswith("资讯"):
                                            invalid_nicknames.append(nickname)

                                    if invalid_nicknames:
                                        print(
                                            f"⚠️ {content_type}: 发现非'资讯'结尾的昵称 ({invalid_nicknames[:3]}...), 可能被反爬虫，正在重试(Attempt {attempt + 1}/{max_retries})..."
                                        )
                                        # [Repetitive Logic] Penalize IP
                                        if (
                                            self.proxy_pool
                                            and proxy_used
                                            and proxy_used.startswith("http")
                                        ):
                                            self.logger.info(
                                                f"🚫 对IP {proxy_used} 进行扣分处理"
                                            )
                                            self.proxy_pool.update_score(
                                                proxy_used, success=False
                                            )

                                        force_retry = True
                                        break  # Break from script loop to trigger outer retry

                                # extracting count for total_pages calculation
                                total_count = int(article_list_data.get("count", 0))

                                # 每页80条，计算总页数
                                total_pages = math.ceil(total_count / 80)
                                print(
                                    f"✓ {content_type}: 校验通过 (昵称后缀检查)，共{total_count}条数据，{total_pages}页"
                                )
                                return total_pages, total_count

                        except Exception as e:
                            print(f"⚠️ {content_type}: JSON解析或校验失败: {e}")
                            continue

                # Logic for retry:
                # If we are here, it means either:
                # 1. No script contained 'article_list'
                # 2. 'article_list' was found but count was > 50000 (force_retry=True)

                if force_retry:
                    # Log already printed above
                    pass
                else:
                    print(
                        f"⚠️ {content_type}: 未找到article_list变量 (Attempt {attempt + 1}/{max_retries})"
                    )

                if attempt < max_retries - 1:
                    time.sleep(random.uniform(1, 3))
                    continue
                else:
                    print(f"⚠️ {content_type}: 多次重试后均失败，默认1页")
                    return 1, 0

            except Exception as e:
                print(f"❌ {content_type}: 检测页数异常(第{attempt + 1}次) - {e}")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(1, 3))
                else:
                    self.logger.error(f"检测{content_type}总页数失败: {e}")
                    return 0, 0

        return 0, 0

    def get_soup_form_url(self, url: str) -> tuple[BeautifulSoup | None, str | None]:
        """
        获取页面内容（内置IP轮换重试机制）
        Returns:
            (soup, proxy_url_str)
        """
        # Session should be created FRESH for each attempt if we want to simulate a completely new user
        # However, if we want to reuse connections for the same proxy, it should be outside.
        # Given the anti-crawl context (redirecting to other sites), a fresh session per attempt is safer
        # to avoid carrying over any "flagged" cookies.

        # session = requests.Session() <--- MOVED INSIDE LOOP

        current_headers = {
            "User-Agent": self.ua_manager.get_user_agent(),
            "Referer": f"https://guba.eastmoney.com/list,{self.secCode}.html",
            "Connection": "keep-alive",
        }

        # 内置重试循环：尝试3个不同的代理
        for attempt in range(1, 4):
            # Create a fresh session for each attempt to avoid cookie tracking across proxies
            session = requests.Session()

            proxies_to_use = None
            proxy_url_str = "Direct"

            if self.proxy_pool:
                proxies_to_use = self.proxy_pool.get_random_proxy()
                if not proxies_to_use:
                    # 代理池空了，抛错
                    # Close session before raising
                    session.close()
                    raise NetworkException("代理池耗尽")
                proxy_url_str = proxies_to_use.get("http", "Unknown")

            elif self.proxies:
                proxies_to_use = (
                    self.backup_proxies if self.use_backup_proxy else self.proxies
                )
                proxy_url_str = "ConfigProxy"

            try:
                # self.logger.info(f"下载尝试 {attempt}/3: {url} (IP: {proxy_url_str})")
                response = session.get(
                    url,
                    headers=current_headers,
                    timeout=10,  # 稍微放宽超时到10秒
                    proxies=proxies_to_use,
                )

                if response.status_code != 200:
                    self.logger.warning(
                        f"请求失败 [{response.status_code}] (IP: {proxy_url_str})"
                    )
                    if self.proxy_pool and proxies_to_use:
                        self.proxy_pool.remove_proxy(proxies_to_use)
                    session.close()  # Close failed session
                    continue

                html = response.content.decode("utf-8", "ignore")

                # 简单内容校验
                if "listitem" not in html:
                    if "验证" in html or "captcha" in html:
                        self.logger.warning(f"触发反爬虫验证 (IP: {proxy_url_str})")
                        if self.proxy_pool:
                            self.proxy_pool.remove_proxy(proxies_to_use)
                        session.close()
                        continue

                    # 正常返回200但无内容，可能是最后一页，也可能是代理被软封锁
                    # 策略：不扣分，不移除代理，但算作一次失败（continue尝试其他代理）
                    # User Modified: "如果为空不应该给ip扣分（目标网站的反爬只有两个策略：非200错误，或者导航到其他的网站）"
                    # So we just continue to try another proxy if this one returned weird empty data,
                    # but we do NOT remove it from the pool.
                    self.logger.info(
                        f"页面无内容 (IP: {proxy_url_str})，重试下一个代理"
                    )
                    # Close session and continue
                    session.close()
                    continue

                session.close()
                return BeautifulSoup(html, features="lxml"), proxy_url_str

            except Exception as e:
                # 发生网络错误
                self.logger.warning(f"网络异常: {e} (IP: {proxy_url_str})")
                if self.proxy_pool and proxies_to_use:
                    self.proxy_pool.remove_proxy(proxies_to_use)
                session.close()
                continue

        # session.close() # Removed because session is now local in loop and closed there
        return None, None

    # --- get_data is below ---

    @retry(
        retry=(
            retry_if_exception_type(NetworkException)
            | retry_if_exception_type(ServerException)
            | retry_if_exception_type(ContentChangedException)
        ),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=4),
    )
    def get_data(self, page, content_type="news", expected_total_count=None):
        """
        获取指定页面的数据 - 统一JSON解析版
        支持 news, report, notice 三种类型
        """
        import re
        import json
        from datetime import datetime, timedelta, timezone

        # 北京时间工具
        def get_beijing_now():
            tz_beijing = timezone(timedelta(hours=8))
            return datetime.now(tz_beijing)

        # 类型映射
        type_map = {"news": "1,f", "report": "2,f", "notice": "3,f"}

        # 构造URL
        if page == 1:
            Url = f"https://guba.eastmoney.com/list,{self.secCode},{type_map[content_type]}.html"
        else:
            type_prefix = type_map[content_type]
            Url = f"https://guba.eastmoney.com/list,{self.secCode},{type_prefix}_{page}.html"

        try:
            self.logger.info(f"开始处理页面 {Url}")

            soup, _ = self.get_soup_form_url(Url)
            if soup is None:
                raise NetworkException(f"无法获取页面 {page} (soup is None)")

            # 1. 提取 JSON 数据
            # 查找包含 var article_list 的脚本
            scripts = soup.find_all("script")
            article_list_data = None

            for script in scripts:
                script_text = script.string
                if script_text and "var article_list" in script_text:
                    try:
                        # 使用 raw_decode 解决 "Extra data" 问题
                        # 只需要找到开始的 '{'
                        start_index = script_text.find("{")
                        if start_index != -1:
                            decoder = json.JSONDecoder()
                            # raw_decode 会从字符串开头解析一个完整的JSON对象，并返回 (obj, end_index)
                            # 我们传入从 '{' 开始的字符串
                            article_list_data, _ = decoder.raw_decode(
                                script_text[start_index:]
                            )
                            break
                    except Exception as e:
                        # self.logger.warning(f"JSON解析尝试失败: {e}")
                        # 降低日志级别，只有Debug才显示，避免刷屏
                        continue

            # 2. 校验数据有效性
            if not article_list_data or "re" not in article_list_data:
                if "没有相关数据" in str(soup):
                    return []

                if "验证" in str(soup):
                    raise ContentChangedException("页面包含验证信息，需要重试")

                # 如果找不到数据，可能是反爬或者页面变更
                raise NetworkException(
                    f"页面 {page} 未找到有效的 article_list JSON数据"
                )

            # 3. 反爬虫校验 (检查 count)
            if expected_total_count and expected_total_count > 0:
                page_count = article_list_data.get("count", 0)
                if abs(page_count - expected_total_count) > 100:
                    self.logger.warning(
                        f"⚠️ 反爬虫警告: 页面{page} count({page_count}) 与预期({expected_total_count}) 差异过大"
                    )
                    raise ContentChangedException(
                        f"页面内容不匹配 (Count deviation: {page_count} vs {expected_total_count})"
                    )

            # 4. 解析数据列表
            items = article_list_data["re"]
            batch_data = []

            for item in items:
                try:
                    post_id = str(item.get("post_id"))
                    title = item.get("post_title")

                    if not post_id or not title:
                        continue

                    # 统一URL处理
                    raw_url = item.get("Art_Url")
                    if raw_url:
                        url = raw_url
                    else:
                        url = f"https://guba.eastmoney.com/news,{self.secCode},{post_id}.html"

                    # 统一构建数据字典
                    data_json = {
                        "stock_code": self.secCode,
                        "content_type": content_type,
                        "title": title,
                        "url": url,
                        "url_id": post_id,
                        "read_count": item.get("post_click_count", 0),
                        "comment_count": item.get("post_comment_count", 0),
                        "publish_time": item.get("post_publish_time"),  # 完整时间
                        # Author / Nickname
                        "author": item.get("user_nickname"),
                        # Report Specific
                        "grade": item.get("grade_type"),  # 评级
                        "institution": item.get("institution"),  # 机构
                        # Notice Specific
                        "notice_type": item.get("notice_type"),  # 公告类型
                        "summary": title,
                        "source": "official",
                        "created_at": get_beijing_now(),
                        "updated_at": get_beijing_now(),
                    }

                    batch_data.append(data_json)
                except Exception as e:
                    self.logger.warning(f"解析条目失败: {e}")
                    continue

            self.logger.info(f"成功获取页面 {page}: {len(batch_data)} 条数据")
            return batch_data

        except ContentChangedException as e:
            raise e  # 直接抛出，触发retry
        except Exception as e:
            time.sleep(random.uniform(0.5, 3))
            self.logger.error(f"获取页面数据失败: {e}, 直接跳过该页面！")
            # raise NetworkException(e)

    # 检查索引存在性
    def index_exists_by_name(self, collection, index_name):
        return index_name in collection.index_information()

    def _crawl_single_page(
        self, page: int, content_type: str, total_count: int
    ) -> list:
        """
        线程池使用的单页爬取包装方法
        增加逻辑：针对最后一页（或接近末尾页）可能为空的情况进行特殊处理
        返回 None 表示失败，返回 [] 表示成功但无数据
        """
        retry_count = 0
        max_retries = 3  # 默认网络重试
        empty_retries = 0  # 空数据重试

        # 判断是否可能是最后一页 (每页80条)
        is_last_page = False
        if total_count > 0:
            if page * 80 >= total_count:
                is_last_page = True

        while retry_count < max_retries:
            try:
                # 只负责下载
                data_list = self.get_data(
                    page, content_type, expected_total_count=total_count
                )

                # [新增] 针对最后一页为空的BUG处理 (数据为空的情况)
                if not data_list and is_last_page:
                    if empty_retries < 2:
                        empty_retries += 1
                        time.sleep(1)
                        self.logger.info(
                            f"页面{page} (最后一页?) 返回数据空，重试第{empty_retries}次..."
                        )
                        continue  # 触发重试
                    else:
                        self.logger.info(
                            f"页面{page} (最后一页) 连续为空，视为正常结束 (网站BUG)"
                        )
                        return []  # explicitly return empty list for success

                return data_list

            except NetworkException as e:
                # [新增] 专门处理 "soup is None" 这种情况
                if "soup is None" in str(e):
                    # 如果是最后一页，保留原有的容错逻辑
                    if is_last_page:
                        if empty_retries < 2:
                            empty_retries += 1
                            time.sleep(1)
                            self.logger.info(
                                f"页面{page} (最后一页?) 获取为空(soup=None)，重试第{empty_retries}次..."
                            )
                            continue
                        else:
                            self.logger.info(
                                f"页面{page} (最后一页) 连续获取为空，视为正常结束"
                            )
                            return []

                    # 对于非最后一页，如果soup is None，说明底层已经重试多次均失败
                    # 此时直接放弃该页，不再进行外层重试
                    self.logger.warning(
                        f"页面{page} 无法获取数据 (soup is None)，放弃该页任务"
                    )
                    return []

                # 其他网络错误，走正常重试
                retry_count += 1
                if retry_count >= max_retries:
                    self.logger.error(f"页面{page}重试{max_retries}次后仍失败: {e}")
                    return None  # Failure
                time.sleep(random.uniform(1, 2))

            except Exception as e:
                retry_count += 1
                if retry_count >= max_retries:
                    self.logger.error(f"页面{page}重试{max_retries}次后仍失败: {e}")
                    return None  # Failure
                time.sleep(random.uniform(1, 2))

        return None  # Failure if loop finishes without return

    def main(self):
        # 创建复合唯一索引：stock_code + content_type + url_id
        try:
            if not self.index_exists_by_name(self.col, "idx_stock_type_url"):
                self.logger.info("创建复合唯一索引: stock_code + content_type + url_id")
                self.col.create_index(
                    [("stock_code", 1), ("content_type", 1), ("url_id", 1)],
                    unique=True,
                    name="idx_stock_type_url",
                )

            # 创建查询优化索引
            if not self.index_exists_by_name(self.col, "idx_stock_type_time"):
                self.logger.info("创建查询索引: stock_code + content_type + crawl_time")
                self.col.create_index(
                    [("stock_code", 1), ("content_type", 1), ("crawl_time", -1)],
                    name="idx_stock_type_time",
                )
        except Exception as e:
            self.logger.error(f"创建索引失败: {e}")
            pass

        # 三种内容类型
        content_types = ["news", "report", "notice"]
        type_names = {"news": "资讯", "report": "研报", "notice": "公告"}

        for content_type in content_types:
            # 自动检测总页数和总条数
            total_pages, total_count = self.get_total_pages(content_type)

            if total_pages == 0:
                print(f"⚠️ 无法获取{type_names[content_type]}页数，跳过")
                continue

            print(f"\n{'=' * 60}")
            print(f"开始爬取{type_names[content_type]}，共{total_pages}页")
            print(f"{'=' * 60}")

            # 重置重复计数器和年份推断状态
            self.consecutive_duplicate_pages = 0
            self.last_month = None
            self.current_year = datetime.now().year

            # 爬取所有页面 - 多线程版本
            # 这里的改变是：多线程下载，但单线程顺序处理结果（入库）

            # [新增] 记录IP池健康状况
            if self.proxy_pool:
                pool_size = self.proxy_pool.count()
                self.logger.info(f"当前IP池健康状况: {pool_size}个可用代理")
                if pool_size < 5:
                    self.logger.warning(
                        f"⚠️ IP池数量较低 ({pool_size})，爬取速度可能受限"
                    )

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # 提交所有页面任务
                # page -> future
                future_map = {
                    page: executor.submit(
                        self._crawl_single_page, page, content_type, total_count
                    )
                    for page in range(1, total_pages + 1)
                }

                # 使用tqdm显示进度
                # [Request] Add stock code to tqdm description for err.log clarity
                with tqdm(
                    total=total_pages,
                    desc=f"[{self.secCode}] {type_names[content_type]}",
                ) as pbar:
                    should_stop = False

                    # 严格按顺序 1, 2, 3... 获取结果
                    for page in range(1, total_pages + 1):
                        future = future_map[page]
                        try:
                            # 获取结果（如果该页还没下载完，会阻塞直到下载完）
                            batch_data = future.result()

                            # [新增] 如果返回None，说明该页爬取失败（重试耗尽），跳过处理，不计入重复停止逻辑
                            if batch_data is None:
                                self.logger.error(f"页面 {page} 最终失败，跳过处理")
                                pbar.update(1)
                                continue

                            # 1. 顺序处理 (JSON解析模式下不需要推断年份)
                            valid_data = []
                            for data in batch_data:
                                # JSON直接返回完整时间，无需推断
                                # data["publish_time"] 已经是 "2026-01-19 19:02:17" 格式
                                valid_data.append(data)

                            # 2. 写入数据库
                            new_count = 0
                            if valid_data:
                                try:
                                    result = self.col.insert_many(
                                        valid_data, ordered=False
                                    )
                                    new_count = len(result.inserted_ids)
                                except BulkWriteError as e:
                                    new_count = e.details.get("nInserted", 0)
                                except Exception as e:
                                    self.logger.error(f"写入DB失败: {e}")

                            # 3. 页级重复检测
                            if new_count == 0:
                                self.consecutive_duplicate_pages += 1
                                pbar.set_postfix(
                                    {
                                        "新增": 0,
                                        "状态": f"页重复x{self.consecutive_duplicate_pages}",
                                    }
                                )
                            else:
                                self.consecutive_duplicate_pages = 0
                                pbar.set_postfix({"新增": new_count, "状态": "进行中"})

                            # 4. 阈值检查
                            if (
                                self.consecutive_duplicate_pages
                                >= self.duplicate_page_threshold
                            ):
                                self.logger.warning(
                                    f"⏹ 连续{self.consecutive_duplicate_pages}页重复，跳过剩余"
                                )
                                should_stop = True

                            pbar.update(1)

                            if should_stop:
                                # 取消剩余任务
                                for p in range(page + 1, total_pages + 1):
                                    if p in future_map:
                                        future_map[p].cancel()
                                break

                        except Exception as e:
                            self.logger.error(f"处理页面{page}流程失败: {e}")
                            pbar.update(1)

            if should_stop:
                print(f"\n⏹️ {self.secCode} {type_names[content_type]} 提前结束")
            else:
                print(f"\n✓ {self.secCode} {type_names[content_type]} 完成")

            time.sleep(random.uniform(0, 1))
            self.num_start = 0


if __name__ == "__main__":
    """直接运行此文件时的入口"""
    import sys

    # 读取配置
    config_path = "config.ini"
    config = configparser.ConfigParser()
    config.read(config_path, encoding="utf-8")

    # 从配置文件读取参数
    sec_code = config.get("mainClass", "secCode", fallback="600519")
    pages_start = config.getint("mainClass", "pages_start", fallback=1)
    pages_end = config.getint("mainClass", "pages_end", fallback=10)
    collection_name = config.get("mainClass", "collectionName", fallback="stock_news")
    db_name = config.get("MongoDB", "database", fallback="guba")

    print("=" * 60)
    print("东方财富股吧爬虫 - 官方咨询抓取 (新版)")
    print("=" * 60)
    print(f"股票代码: {sec_code}")
    print(f"抓取范围: 第 {pages_start} 页到第 {pages_end} 页")
    print(f"数据存储: MongoDB - {db_name}.{collection_name}")
    print(f"数据结构: 统一Collection (所有股票共用)")
    print("=" * 60)

    try:
        # 创建爬虫实例
        crawler = guba_comments(
            config_path=config_path,
            config=config,
            secCode=sec_code,
            pages_start=pages_start,
            pages_end=pages_end,
            num_start=0,
            MongoDB=True,
            collectionName=collection_name,
            full_text=False,
        )

        # 开始爬取
        print("\n开始爬取...")
        crawler.main()

        print("\n" + "=" * 60)
        print("✅ 爬取完成！")
        print("=" * 60)

    except KeyboardInterrupt:
        print("\n\n⚠️ 用户中断爬取")
        sys.exit(0)
    except Exception as e:
        print(f"\n\n❌ 爬取失败: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)

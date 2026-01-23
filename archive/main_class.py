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
from typing import Optional, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
# from Utils.MongoClient import MongoClient
# from Utils.EuclidDataTools import CsvClient
# from TreadCrawler import RedisClient
import configparser
from storage.database_client import DatabaseManager
from core.user_agent_manager import get_user_agent_manager
import time
import random

from tenacity import retry,retry_if_exception_type,stop_after_attempt,wait_exponential
from pymongo.errors import DuplicateKeyError, BulkWriteError
from core.proxy_manager import ProxyManager  # 使用统一的代理池管理器
# 定义自定义异常类
class NetworkException(Exception): pass
class ServerException(Exception): pass
class ContentChangedException(Exception): pass


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
        config_path:str,
        config: configparser.ConfigParser,
        secCode,
        pages_start: int = 0,
        pages_end: int = 100,
        num_start: int = 0,
        MongoDB: bool = True,
        collectionName : str = 'default',
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
        
        # 增量更新优化：连续重复计数器
        self.consecutive_duplicates = 0
        self.duplicate_threshold = 5  # 连续5条重复则停止爬取
        
        # 多线程配置
        self.max_workers = max_workers

        # redis client for full_text_Crawler
        # 检查是否启用Redis
        self.redis_enabled = config.getboolean('Redis', 'enabled', fallback=True)
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
        db_name = config.get('MongoDB', 'database', fallback='guba')
        # 使用统一的Collection名称：stock_news（所有股票共用）
        collection_name = config.get('mainClass', 'collectionName', fallback='stock_news')
        # 使用 database_client 的 MongoDB 客户端
        self.col = self.db_manager.get_mongo_client(db_name, collection_name).collection
        self.collection_name = collection_name  # 保存Collection名称供后续使用

        # log setting
        # log_format = "%(levelname)s %(asctime)s %(filename)s %(lineno)d %(message)s"
        # logging.basicConfig(filename=self.log_path, format=log_format, level=logging.INFO,encoding = 'utf-8')
        
        # 设置日志级别为WARNING，减少刷屏（只显示警告和错误）
        self.logger = logging.getLogger(config.get('logging', 'log_file', fallback='main_controller.log'))
        if not self.logger.handlers:  # 避免重复添加handler
            self.logger.setLevel(logging.WARNING)  # 只显示WARNING及以上级别
            handler = logging.StreamHandler()
            handler.setLevel(logging.WARNING)
            self.logger.addHandler(handler)
        
        # User-Agent管理器初始化
        self.ua_manager = get_user_agent_manager('random')  # 使用随机模式
        
        # header setting - 使用动态User-Agent
        self.header = {
            "User-Agent": self.ua_manager.get_user_agent(),
        }
        
        # # proxies setting
        # if config.has_option("proxies", "tunnel"):
        #     tunnel = config.get("proxies", "tunnel")
        #     self.proxies = {
        #         "http": "http://%(proxy)s/" % {"proxy": tunnel},
        #         "https": "http://%(proxy)s/" % {"proxy": tunnel},
        #     }
        # else:
        #     self.proxies = None
        
        # 隧道域名:端口号
        tunnel = "x291.kdltps.com:15818"

        # 用户名密码方式
        username = "t15462021520395"
        password = "wkjzgkdb"
        
        # 代理设置
        proxy_enabled = config.getboolean('proxies', 'enabled', fallback=False)
        use_free_proxy_pool = config.getboolean('proxies', 'use_free_proxy_pool', fallback=False)
        
        # 初始化代理池
        self.proxy_pool = None
        if use_free_proxy_pool:
            print("\n🔧 初始化免费代理池...")
            # 从配置读取代理池阈值
            min_proxy_threshold = config.getint('proxies', 'min_proxy_count', fallback=5)
            self.proxy_pool = ProxyPool(
                target_url="https://guba.eastmoney.com/",
                min_threshold=min_proxy_threshold
            )
            # 尝试从文件加载
            if not self.proxy_pool.load_from_file():
                # 文件不存在，建立新池（每个源最多200个，全面测试，不限时间）
                print("⏳ 开始全面代理测试，预计5-10分钟...")
                self.proxy_pool.build_pool(max_workers=50, max_per_source=200)
                self.proxy_pool.save_to_file()
            print("✅ 代理池就绪\n")
        
        if proxy_enabled and not use_free_proxy_pool:
            # 使用配置文件中的固定代理
            username = config.get('proxies', 'username', fallback='t15462021520395')
            password = config.get('proxies', 'password', fallback='wkjzgkdb')
            tunnel = config.get('proxies', 'tunnel', fallback='')
            
            self.proxies = {
                "http": f"http://{username}:{password}@{tunnel}/",
                "https": f"http://{username}:{password}@{tunnel}/"
            }
            self.backup_proxies = {
                "http": f"http://{username}:{password}@x292.kdltps.com:15818/",
                "https": f"http://{username}:{password}@x292.kdltps.com:15818/"
            }
            print("✓ 代理已启用")
        elif use_free_proxy_pool:
            # 使用免费代理池
            self.proxies = None  # 动态获取
            self.backup_proxies = None
            print("✓ 免费代理池已启用")
        else:
            self.proxies = None
            self.backup_proxies = None
            print("⚠️ 代理已禁用 - 直接连接")
        
        self.use_backup_proxy = False

    def _change_proxy_ip(self):
        """调用快代理更换隧道IP接口"""
        try:
            import requests
            change_url = "https://tps.kdlapi.com/api/changetpsip"
            params = {
                "secret_id": "oqifdb1h1ykoxm8comcv",
                "signature": "vhp8ervln42dkh85ht3ijw6adctj1wah"
            }
            
            response = requests.get(change_url, params=params, timeout=3)
            if response.status_code == 200:
                self.logger.info("隧道IP更换成功")
                return True
            else:
                self.logger.error(f"更换隧道IP失败，状态码: {response.status_code}")
                return False
        except Exception as e:
            self.logger.error(f"更换隧道IP时发生异常: {e}")
            return False

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
    
    def get_total_pages(self, content_type='news'):
        """
        自动检测指定内容类型的总页数
        
        通过解析页面中的JavaScript变量 var article_list = {...} 
        提取 count 字段（总帖子数），然后计算总页数 = ceil(count / 80)
        
        Args:
            content_type: 'news' (资讯) | 'report' (研报) | 'notice' (公告)
        
        Returns:
            int: 总页数，如果检测失败返回0
        """
        import re
        import math
        
        # 类型映射
        type_map = {
            'news': '1,f',
            'report': '2,f',
            'notice': '3,f'
        }
        
        if content_type not in type_map:
            self.logger.error(f"未知的内容类型: {content_type}")
            return 0
        
        # 构造第一页URL
        url = f"https://guba.eastmoney.com/list,{self.secCode},{type_map[content_type]}.html"
        
        try:
            soup = self.get_soup_form_url(url)
            if not soup:
                return 0
            
            # 查找所有script标签
            scripts = soup.find_all('script')
            
            for script in scripts:
                script_text = script.string
                if script_text and 'var article_list' in script_text:
                    # 找到包含 article_list 的脚本
                    # 使用正则提取 "count": 数字
                    match = re.search(r'"count"\s*:\s*(\d+)', script_text)
                    if match:
                        total_count = int(match.group(1))
                        
                        # 安全检查：如果count异常大，可能是服务器返回了错误数据
                        if total_count > 50000:
                            print(f"⚠️ {content_type}: count值异常大({total_count})，可能被反爬虫，使用保守值")
                            return 1  # 返回1页，让用户察觉问题
                        
                        # 每页80条，计算总页数
                        total_pages = math.ceil(total_count / 80)
                        print(f"✓ {content_type}: 共{total_count}条数据，{total_pages}页")
                        return total_pages
            
            # 如果没找到JavaScript变量，回退到只返回1页
            print(f"⚠️ {content_type}: 未找到article_list变量，默认1页")
            return 1
            
        except Exception as e:
            print(f"❌ {content_type}: 检测页数异常 - {e}")
            self.logger.error(f"检测{content_type}总页数失败: {e}")
            return 0
    
    def _infer_year_for_publish_time(self, publish_time_raw):
        """
        推断发布时间的年份
        
        逻辑：
        1. 解析出月份
        2. 如果当前月份 > 上一条的月份+3（例如从1月到12月），说明跨年了，年份-1
        3. 返回完整的年月日时间字符串
        
        Args:
            publish_time_raw: 原始时间字符串，例如 "01-21 15:30" 或 "12-31 23:59"
        
        Returns:
            完整时间字符串，例如 "2026-01-21 15:30" 或 "2025-12-31 23:59"
        """
        try:
            # 解析月份（例如："01-21 15:30" -> 1）
            parts = publish_time_raw.split()
            if len(parts) < 1:
                return publish_time_raw
            
            date_part = parts[0]  # "01-21"
            month = int(date_part.split("-")[0])
            
            # 推断年份逻辑
            if self.last_month is not None:
                # 如果当前月份 > 上一条月份+3，说明跨年反向了（例如：1月 -> 12月）
                if month > self.last_month + 3:
                    self.current_year -= 1
            
            self.last_month = month
            
            # 拼接完整时间
            return f"{self.current_year}-{publish_time_raw}"
            
        except Exception as e:
            # 如果解析失败，返回原始字符串
            return publish_time_raw

    # @retry(stop_max_attempt_number=5, wait_fixed=2000)  # 最多尝试5次，每次间隔2秒
    def get_soup_form_url(self, url: str) -> BeautifulSoup:
        session = requests.Session()
        current_headers = {
            "User-Agent": self.ua_manager.get_user_agent(),
            "Referer": f"https://guba.eastmoney.com/list,{self.secCode}.html",
            "Connection": "keep-alive",
        }
        
        proxies_to_use = None
        if self.proxy_pool:
            proxies_to_use = self.proxy_pool.get_random_proxy()
            if not proxies_to_use:
                # 关键：如果启用了池但拿不到代理，抛错防止泄露本地IP
                raise NetworkException("代理池已空，为保护本地IP，停止请求")
        elif self.proxies:
            proxies_to_use = self.backup_proxies if self.use_backup_proxy else self.proxies

        try:
            response = session.get(
                url,
                headers=current_headers,
                timeout=5, # 缩短超时到5秒
                proxies=proxies_to_use
            )
            session.close()

            if response.status_code != 200:
                # 如果是403或502，说明代理被封或失效
                if self.proxy_pool and proxies_to_use:
                    self.proxy_pool.remove_proxy(proxies_to_use)
                return None

            html = response.content.decode("utf-8", "ignore")
            if "listitem" not in html:
                if "验证" in html or "captcha" in html:
                    if self.proxy_pool: self.proxy_pool.remove_proxy(proxies_to_use)
                return None
            return BeautifulSoup(html, features="lxml")

        except Exception as e:
            # 发生任何网络错误，立即移除该代理
            if self.proxy_pool and proxies_to_use:
                self.proxy_pool.remove_proxy(proxies_to_use)
            self.logger.warning(f"代理请求异常: {e}")
            return None

    def get_data_json(self, item, content_type='news'):
        """
        解析数据项，支持三种内容类型
        
        Args:
            item: BeautifulSoup元素
            content_type: 'news' (资讯) | 'report' (研报) | 'notice' (公告)
        
        Returns:
            dict: 数据字典，解析失败返回None
        """

        tds = item.find_all("td")
        if len(tds) < 5:
            # 静默跳过，不打印警告
            return None
        
        try:
            # 提取URL和ID
            href = tds[2].a["href"]
            full_url = "https://guba.eastmoney.com" + href
            
            # 从URL中提取唯一ID（例如：/news,600519,1234567890.html -> 1234567890）
            try:
                url_id = href.split(",")[-1].replace(".html", "").strip()
            except:
                url_id = href  # 如果提取失败，使用完整href
            
            # 数字字段转换
            try:
                read_count = int(tds[0].text.strip())
            except:
                read_count = 0
            
            try:
                comment_count = int(tds[1].text.strip())
            except:
                comment_count = 0
            
            # 根据内容类型解析特定字段
            author = None
            grade = None
            institution = None
            notice_type = None
            publish_time_raw = ""
            
            if content_type == 'news':
                # 资讯：第4列是作者，第5列是时间
                try:
                    author = tds[3].a.text.strip() if tds[3].a else None
                except:
                    author = None
                publish_time_raw = tds[4].text.strip()
                
            elif content_type == 'report':
                # 研报：第4列是评级，第5列是机构，第6列是时间
                try:
                    grade = tds[3].text.strip() if len(tds) > 3 else None
                    institution = tds[4].text.strip() if len(tds) > 4 else None
                    publish_time_raw = tds[5].text.strip() if len(tds) > 5 else ""
                except:
                    grade = None
                    institution = None
                    publish_time_raw = ""
                    
            elif content_type == 'notice':
                # 公告：第4列是公告类型，第5列是时间
                try:
                    notice_type = tds[3].text.strip() if len(tds) > 3 else None
                    publish_time_raw = tds[4].text.strip() if len(tds) > 4 else ""
                except:
                    notice_type = None
                    publish_time_raw = ""
            
            # 解析publish_time并推断年份
            publish_time_with_year = self._infer_year_for_publish_time(publish_time_raw)
            
            # 新的规范化字段结构
            data_json = {
                "stock_code": self.secCode,                        # 股票代码
                "content_type": content_type,                      # 内容类型
                "title": tds[2].a.text.strip(),                   # 标题
                "url": full_url,                                   # 完整URL
                "url_id": url_id,                                  # URL唯一ID
                "read_count": read_count,                          # 阅读数（数字）
                "comment_count": comment_count,                    # 评论数（数字）
                "publish_time": publish_time_with_year,            # 发布时间（带年份）
                "author": author,                                  # 作者（仅资讯）
                "grade": grade,                                    # 评级（仅研报）
                "institution": institution,                        # 机构（仅研报）
                "notice_type": notice_type,                        # 公告类型（仅公告）
                "summary": tds[2].a.text.strip()[:100],           # 摘要（标题前100字）
                "crawl_time": datetime.now(),                     # 爬取时间
                "source": "official",                             # 来源标识
                "created_at": datetime.now(),                     # 创建时间
                "updated_at": datetime.now()                      # 更新时间
            }
            return data_json
            
        except Exception as e:
            # 静默跳过解析失败的数据
            return None


        return data_json




    @retry(
    retry=(
        retry_if_exception_type( NetworkException) |
        retry_if_exception_type(ServerException)
        # retry_if_exception(lambda e: isinstance(e, ContentChangedException))
    ),
    stop=stop_after_attempt(5) ,
    wait=wait_exponential(multiplier=1, min=2, max=4)
)
    def get_data(self, page, content_type='news'):
        """
        获取指定页面的数据
        
        Args:
            page: 页码
            content_type: 'news' (资讯) | 'report' (研报) | 'notice' (公告)
        """
        # 类型映射
        type_map = {
            'news': '1,f',
            'report': '2,f',
            'notice': '3,f'
        }
        
        # 正确的官方资讯/研报/公告URL格式
        if page == 1:
            Url = f"https://guba.eastmoney.com/list,{self.secCode},{type_map[content_type]}.html"
        else:
            type_prefix = type_map[content_type]
            Url = f"https://guba.eastmoney.com/list,{self.secCode},{type_prefix}_{page}.html"
        data_list = None
        
        try:
            self.logger.info(f"开始处理页面 {Url}")
            
            soup = self.get_soup_form_url(Url)
            if soup is None:
                raise NetworkException(f"无法获取页面 {page}")
                
            data_list = soup.find_all("tr", "listitem")
            if not data_list:
                raise NetworkException(f"页面 {page} 无数据")

            new_insert_count = 0
            batch_data = []
            
            for item in data_list:
                data_json = self.get_data_json(item, content_type)
                if data_json:
                    batch_data.append(data_json)
            
            if not batch_data:
                return 0

            # 使用 unordered 批量插入，速度更快且会自动跳过重复 ID
            try:
                # 这里的 self.col 是 pymongo collection 对象
                result = self.col.insert_many(batch_data, ordered=False)
                new_insert_count = len(result.inserted_ids)
            except BulkWriteError as e:
                # 部分插入成功，部分因重复 ID 失败
                new_insert_count = e.details.get('nInserted', 0)
            except Exception as e:
                self.logger.error(f"批量写入异常: {e}")
                
            return new_insert_count
        except Exception as e:
            time.sleep(random.uniform(0.5, 3))
            self.logger.error(f"soup数据转data_list失败: {e}")
            raise NetworkException(e)
        success_count = 0
        exist_count = 0  # 初始化，避免引用错误
        is_retry = False
        for item in data_list:
            try:
                data_json = self.get_data_json(item, content_type)
                if data_json:
                    try:
                        exist_count = 0
        
                        	
                            
                        try:
                            self.col.insert_many([data_json]) # 注意包装成列表
                            # self.logger.info(f"写入数据成功: {data_json['url']}")
                            # 成功插入，重置重复计数器
                            self.consecutive_duplicates = 0
                        except DuplicateKeyError as e:
                            # 重复数据，计数器+1
                            self.consecutive_duplicates += 1
                            
                            if self.secCode not in  data_json['url']  and "zssh000001" in  data_json['url']:
                                # self.logger.error(f"可能被重定向: {data_json['href']}")
                                raise ContentChangedException("内容重复")
                                is_retry = True
                            else:
                                # 静默跳过重复数据，不打印日志
                                continue
                        except BulkWriteError as e:
                            # 检查是否是重复键错误
                            if self.secCode not in  data_json['url']  and "zssh000001" in  data_json['url']:
                                # self.logger.error(f"可能被重定向: {data_json['href']}")
                                is_retry = True
                                raise ContentChangedException("内容重复")
                            if e.details.get('writeErrors') and any(err.get('code') == 11000 for err in e.details['writeErrors']):
                                # 静默跳过重复数据
                                self.consecutive_duplicates += 1
                                exist_count += 1
                                continue
                            else:
                                self.logger.error(f"批量写入失败: {e}")
                                continue
                        except Exception as e:
                            self.logger.error(
                                f"写入数据失败: {e}"
                            )
                            continue
                        # 写入Redis（如果启用）
                        if self.redis_enabled and self.redis_client:
                            try:
                                self.redis_client.add_url(data_json["url"])
                            except Exception:
                                pass  # 静默失败，不刷屏
                        success_count += 1
                    except Exception as e:
                        self.logger.error(f"插入数据失败: {e}, url: {data_json.get('url', '无url')}")
                    # 移除了 self.t.set_postfix，因为已改用tqdm的page_bar
                    self.num_start += 1
                    
                    # 检查是否达到连续重复阈值
                    if self.consecutive_duplicates >= self.duplicate_threshold:
                        self.logger.warning(
                            f"连续{self.consecutive_duplicates}条重复数据，"
                            f"已达到阈值({self.duplicate_threshold})，"
                            f"停止爬取该股票: {self.secCode}"
                        )
                        # 提前终止当前页面的处理
                        return -1  # 返回特殊值表示提前终止
                else:
                    # 静默跳过空数据
                    pass
            except Exception as e:
                self.logger.error(f"处理单个 item 失败: {e}")
        if is_retry == True:
            # 页面没有新数据，准备重试
            self.logger.warning(f"页面 {page} 没有新数据，准备重试")
            #抛出 ContentChangedException 错误
            raise ContentChangedException("内容重复")
        
        self.logger.info(f"页面 {page} 处理完成，成功插入 {success_count}/{len(data_list)} 条数据")
        return exist_count

 
    #检查索引存在性
    def index_exists_by_name(self, collection, index_name):
        return index_name in collection.index_information()
    
    def _crawl_single_page(self, page: int, content_type: str) -> int:
        """
        线程池使用的单页爬取包装方法
        
        Args:
            page: 页码
            content_type: 内容类型
        
        Returns:
            新增数据条数，-1表示达到重复阈值
        """
        retry_count = 0
        max_retries = 5
        
        while retry_count < max_retries:
            try:
                new_count = self.get_data(page, content_type)
                return new_count
            except Exception as e:
                retry_count += 1
                if retry_count >= max_retries:
                    self.logger.error(f"页面{page}重试{max_retries}次后仍失败: {e}")
                    return 0
                time.sleep(random.uniform(1, 2))
        
        return 0

                
    def main(self):
        
        # 创建复合唯一索引：stock_code + content_type + url_id
        try:
            if not self.index_exists_by_name(self.col, "idx_stock_type_url"):
                self.logger.info("创建复合唯一索引: stock_code + content_type + url_id")
                self.col.create_index(
                    [("stock_code", 1), ("content_type", 1), ("url_id", 1)], 
                    unique=True, 
                    name="idx_stock_type_url"
                )
            
            # 创建查询优化索引
            if not self.index_exists_by_name(self.col, "idx_stock_type_time"):
                self.logger.info("创建查询索引: stock_code + content_type + crawl_time")
                self.col.create_index(
                    [("stock_code", 1), ("content_type", 1), ("crawl_time", -1)], 
                    name="idx_stock_type_time"
                )
        except Exception as e:
            self.logger.error(f"创建索引失败: {e}")
            pass
         
        # 三种内容类型
        content_types = ['news', 'report', 'notice']
        type_names = {'news': '资讯', 'report': '研报', 'notice': '公告'}
        
        # 爬取前预检代理池已移除（由scheduler统一管理）
        # if self.proxy_manager:
        #     self.proxy_manager.revalidate_pool()

        for content_type in content_types:
            # 自动检测总页数
            total_pages = self.get_total_pages(content_type)
            
            if total_pages == 0:
                print(f"⚠️ 无法获取{type_names[content_type]}页数，跳过")
                continue
            
            print(f"\n{'='*60}")
            print(f"开始爬取{type_names[content_type]}，共{total_pages}页")
            print(f"{'='*60}")
            
            # 重置重复计数器和年份推断状态
            self.consecutive_duplicates = 0
            self.last_month = None
            self.current_year = datetime.now().year
            
            # 爬取所有页面 - 多线程版本
            consecutive_empty_pages = 0  # 记录连续无新数据的页面数
            page_results = {}  # 存储每页的爬取结果
            
            # 使用线程池并发爬取
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # 提交所有页面任务
                future_to_page = {
                    executor.submit(self._crawl_single_page, page, content_type): page
                    for page in range(1, total_pages + 1)
                }
                
                # 使用tqdm显示进度
                with tqdm(total=total_pages, desc=f"{type_names[content_type]}") as pbar:
                    for future in as_completed(future_to_page):
                        page = future_to_page[future]
                        try:
                            new_count = future.result()
                            page_results[page] = new_count
                            
                            # 更新进度条
                            if new_count == 0:
                                pbar.set_postfix({"新增": 0, "状态": "已同步"})
                            elif new_count == -1:
                                pbar.set_postfix({"状态": "达到重复阈值"})
                            else:
                                pbar.set_postfix({"新增": new_count, "状态": "进行中"})
                            
                            pbar.update(1)
                            
                        except Exception as e:
                            self.logger.error(f"页面{page}爬取失败: {e}")
                            pbar.update(1)
            
            # 检查结果，判断是否提前终止
            # 统计连续为0的页面
            sorted_pages = sorted(page_results.keys())
            for page in sorted_pages[-5:]:  # 只检查最后5页
                if page_results.get(page, 0) == 0:
                    consecutive_empty_pages += 1
                else:
                    consecutive_empty_pages = 0
            
            if consecutive_empty_pages >= 2:
                print(f"\n⏹️ 最后连续{consecutive_empty_pages}页无新数据，{self.secCode} {type_names[content_type]} 抓取结束")
                        
                
                time.sleep(random.uniform(0, 1))
                self.num_start = 0


if __name__ == '__main__':
    """直接运行此文件时的入口"""
    import sys
    
    # 读取配置
    config_path = 'config.ini'
    config = configparser.ConfigParser()
    config.read(config_path, encoding='utf-8')
    
    # 从配置文件读取参数
    sec_code = config.get('mainClass', 'secCode', fallback='600519')
    pages_start = config.getint('mainClass', 'pages_start', fallback=1)
    pages_end = config.getint('mainClass', 'pages_end', fallback=10)
    collection_name = config.get('mainClass', 'collectionName', fallback='stock_news')
    db_name = config.get('MongoDB', 'database', fallback='guba')
    
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
            full_text=False
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

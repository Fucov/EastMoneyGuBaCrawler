import requests
import re
import time
import redis
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Dict
import random
from storage.logger import get_system_logger
import configparser
import os
import hmac
import hashlib
import base64
from urllib.parse import urlparse

import threading


class ProxyManager:
    """
    代理池管理器（Redis版本）

    特性：
    - Redis持久化代理
    - 自动验证和评分
    - 失效自动移除
    - 低于阈值自动补充
    - 线程安全控制
    """

    def __init__(
        self,
        redis_host="localhost",
        redis_port=6379,
        redis_password=None,
        redis_db=0,
        cache_key="guba:proxies:valid",
        target_url="https://guba.eastmoney.com/list,000001,1,f.html",
        min_threshold=5,
        target_count=20,
        max_count=30,
        context=None,
        config_path=None,
    ):
        """
        初始化代理管理器
        """
        self.logger = get_system_logger()
        self.refill_lock = threading.Lock()  # 补充代理时的锁，防止多线程并发触发

        # Redis连接
        self.redis_client = redis.StrictRedis(
            host=redis_host,
            port=redis_port,
            password=redis_password if redis_password else None,
            db=redis_db,
            decode_responses=True,
        )

        self.cache_key = cache_key
        self.test_url = target_url
        self.timeout = 3
        self.min_threshold = min_threshold
        self.target_count = target_count
        self.max_count = max_count  # 代理池最大数量
        self.context = context

        # 读取配置文件
        if config_path is None:
            # 默认从项目根目录读取config.ini
            current_dir = os.path.dirname(os.path.abspath(__file__))
            config_path = os.path.join(current_dir, "..", "config.ini")

        config = configparser.ConfigParser()
        config.read(config_path, encoding="utf-8")

        # 读取代理提供商配置
        self.provider = "free"
        if config.has_section("proxies"):
            self.provider = config.get("proxies", "provider", fallback="free")
            # 兼容旧配置: use_paid_api=true -> qingguo
            if self.provider == "free" and config.getboolean(
                "proxies", "use_paid_api", fallback=False
            ):
                self.provider = "qingguo"

            # 青果配置
            # 优先读取 qingguo_api_url，如果没有则尝试读取 api_url (兼容旧配置)
            self.qingguo_url = config.get(
                "proxies", "qingguo_api_url", fallback=config.get("proxies", "api_url", fallback="https://share.proxy.qg.net/get")
            )
            # 优先读取 qingguo_api_key，如果没有则尝试读取 api_key (兼容旧配置)
            self.qingguo_key = config.get(
                "proxies", "qingguo_api_key", fallback=config.get("proxies", "api_key", fallback="")
            )

            # KDL配置
            self.kdl_url = config.get(
                "proxies", "kdl_api_url", fallback="https://dps.kdlapi.com/api/getdps"
            )
            self.kdl_secret_id = config.get("proxies", "kdl_secret_id", fallback="")
            self.kdl_secret_key = config.get("proxies", "kdl_secret_key", fallback="")
            self.kdl_amount = config.getint("proxies", "kdl_amount", fallback=2)
            self.kdl_sign_type = config.get(
                "proxies", "kdl_sign_type", fallback="hmacsha1"
            )

            # [New] 读取代理认证信息 (Auth User/Pass)
            self.proxy_auth_user = config.get("proxies", "auth_user", fallback="")
            self.proxy_auth_pass = config.get("proxies", "auth_password", fallback="")
            if self.proxy_auth_user and self.proxy_auth_pass:
                self.logger.info(f"✓ 启用代理认证: 用户名={self.proxy_auth_user}")

            # 读取代理池最大值配置
            self.max_count = config.getint(
                "proxies", "max_proxy_count", fallback=self.max_count
            )
            self.logger.info(f"✓ 代理提供商: {self.provider}")
            self.logger.info(f"✓ 代理池最大值限制: {self.max_count}")
        else:
            self.logger.info("✓ 使用免费代理源模式（未找到proxies配置）")

        # 免费代理源配置
        self.free_proxy_sources = [
            # 89ip API（新版）
            {
                "type": "text",
                "url": "http://api.89ip.cn/tqdl.html?api=1&num=60&port=&address=&isp=",
                "name": "89ip-API",
            },
            # ProxyShare JSON API
            {
                "type": "json_list",
                "url": "https://www.proxyshare.com/web_v1/free-proxy/list?page_size=10&page=1&language=zh",
                "name": "ProxyShare",
            },
            # ProxyList JSON API
            {
                "type": "json_list",
                "url": "http://43.135.31.113:8777/proxyList?limit=50&page=1&language=zh-hans",
                "name": "ProxyList",
            },
            # proxy.scdn.io
            {
                "type": "text",
                "url": "https://proxy.scdn.io/text.php",
                "name": "proxy.scdn.io",
            },
            # GitHub开源列表
            {
                "type": "text",
                "url": "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt",
                "name": "GitHub-TheSpeedX",
            },
            {
                "type": "text",
                "url": "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
                "name": "GitHub-clarketm",
            },
        ]

    def count(self) -> int:
        """获取当前有效代理数量"""
        return self.redis_client.hlen(self.cache_key)

    def get_all(self) -> List[Dict]:
        """获取所有代理"""
        proxies = []
        for proxy_url, score in self.redis_client.hgetall(self.cache_key).items():
            proxies.append({"proxy": proxy_url, "score": int(score)})

        # 按评分排序
        proxies.sort(key=lambda x: x["score"], reverse=True)
        return proxies

    def get_random_proxy(self) -> Optional[Dict]:
        """随机获取一个代理"""
        # 检查阈值 - 双重检查锁定 (Double Checked Locking)
        if self.count() < self.min_threshold:
            # 尝试获取锁，只有获取到锁的线程才执行补充，其他线程等待
            # 这里的逻辑是：如果缺IP，大家都要停下来等补充完成
            with self.refill_lock:
                # 再次检查，防止在前一个线程补充完之后，后续获取到锁的线程再次补充
                if self.count() < self.min_threshold:
                    self.logger.info(f"⚠️ 代理池不足({self.count()}个)，触发自动补充...")
                    self.refill_pool(target_count=self.target_count)

        proxies = self.get_all()
        if not proxies:
            return None

        # 从高分代理中随机选择
        if len(proxies) > 10:
            top_half = proxies[: max(1, len(proxies) // 2)]
            selected = random.choice(top_half)
        else:
            selected = random.choice(proxies)

        proxy_url = selected["proxy"]
        return {"http": proxy_url, "https": proxy_url}

    def add_proxy(self, proxy_url: str, score: int = 100):
        """添加代理到Redis"""
        import sys

        is_new = not self.redis_client.hexists(self.cache_key, proxy_url)
        self.redis_client.hset(self.cache_key, proxy_url, score)
        if is_new:
            total = self.count()
            # 用户要求: error.log记录新增IP详情
            # 写入stderr并flush，确保进入err.log (由start.sh定义)
            prefix = f"[{self.context}] " if self.context else ""
            sys.stderr.write(
                f"{prefix}➕ [IP新增] {proxy_url} (分值:{score}, 总数:{total})\n"
            )
            sys.stderr.flush()

    def remove_proxy(self, proxy_dict: Dict):
        """移除失效代理"""
        if not proxy_dict:
            return

        proxy_url = proxy_dict.get("http")
        if proxy_url:
            self.redis_client.hdel(self.cache_key, proxy_url)

    def update_score(self, proxy_url: str, success: bool):
        """更新代理评分"""
        current_score = self.redis_client.hget(self.cache_key, proxy_url)
        if current_score is None:
            return

        score = int(current_score)

        if success:
            score = min(100, score + 5)
        else:
            score = max(0, score - 10)

        if score < 30:
            # 评分过低，移除
            self.redis_client.hdel(self.cache_key, proxy_url)
        else:
            self.redis_client.hset(self.cache_key, proxy_url, score)

    def _generate_kdl_signature(self, method, url, params):
        """生成KDL签名"""
        # 1. 解析URL获取path (e.g. /api/getdps)
        parsed_url = urlparse(url)
        path = parsed_url.path

        # 2. 构造原文字符串: METHOD + path + ? + sorted_query_string
        # 注意：KDL demo显示path可以不带域名，这里尝试使用path
        s = f"{method.upper()}{path}?"

        # 参数排序并拼接
        query_parts = []
        for k in sorted(params.keys()):
            query_parts.append(f"{k}={params[k]}")
        query_str = "&".join(query_parts)

        raw_str = s + query_str

        # 3. HMAC-SHA1加密
        try:
            hmac_str = hmac.new(
                self.kdl_secret_key.encode("utf8"), raw_str.encode("utf8"), hashlib.sha1
            ).digest()
            signature = base64.b64encode(hmac_str).decode("utf-8")
            return signature
        except Exception as e:
            self.logger.error(f"KDL签名生成失败: {e}")
            return ""

    def _fetch_from_kdl(self, max_per_source: int) -> List[str]:
        """从KDL获取代理"""
        raw_list = []
        self.logger.info("📡 [KDL] 开始提取私密代理...")

        # 构造参数
        params = {
            "secret_id": self.kdl_secret_id,
            "num": min(max_per_source, self.kdl_amount),
            "sign_type": self.kdl_sign_type,
            "timestamp": int(time.time()),
            "nonce": random.randint(100000, 999999),
            "format": "json",
            "sep": 1,
            "f_auth": 1,
            "generateType": 4,
        }

        # 生成签名
        signature = self._generate_kdl_signature("GET", self.kdl_url, params)
        params["signature"] = signature

        try:
            resp = requests.get(self.kdl_url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            if data.get("code") == 0:
                proxy_list = data.get("data", {}).get("proxy_list", [])
                raw_list.extend(proxy_list)
                self.logger.info(f"  ✅ [KDL] 成功提取 {len(proxy_list)} 个代理")
            else:
                self.logger.error(
                    f"  ✗ [KDL] API错误: {data.get('code')} - {data.get('msg')}"
                )

        except Exception as e:
            self.logger.error(f"  ✗ [KDL] 请求失败: {e}")

        return raw_list

    def _fetch_from_qingguo(self, max_per_source: int) -> List[str]:
        """从青果获取代理"""
        raw_list = []
        self.logger.info("📡 [青果] 开始提取代理...")

        params = {
            "key": self.qingguo_key,
            "num": min(max_per_source, 5),
            "area": "",
            "isp": 0,
            "format": "json",
            "distinct": "true",
        }

        try:
            resp = requests.get(self.qingguo_url, params=params, timeout=10)
            data = resp.json()

            if data.get("code") == "SUCCESS":
                proxy_list = data.get("data", [])
                for p in proxy_list:
                    server = p.get("server")
                    if server:
                        raw_list.append(server)
                self.logger.info(f"  ✅ [青果] 成功提取 {len(raw_list)} 个代理")
            else:
                self.logger.error(f"  ✗ [青果] API错误: {data.get('message')}")
        except Exception as e:
            self.logger.error(f"  ✗ [青果] 请求失败: {e}")

        return raw_list

    def _fetch_from_free(self) -> List[str]:
        """获取免费代理"""
        raw_list = []
        self.logger.info("📡 [Free] 开始提取免费代理...")

        headers = {"User-Agent": "Mozilla/5.0"}
        for source in self.free_proxy_sources:
            try:
                resp = requests.get(source["url"], headers=headers, timeout=10)
                if source["type"] == "json_list":
                    # 简单JSON解析逻辑...
                    try:
                        data = resp.json()
                        items = data.get(
                            "data", data.get("list", data.get("proxies", []))
                        )
                        for item in items:
                            if isinstance(item, dict):
                                ip = item.get("ip", item.get("host"))
                                port = item.get("port")
                                if ip and port:
                                    raw_list.append(f"{ip}:{port}")
                    except:
                        pass
                else:
                    found = re.findall(r"\d+\.\d+\.\d+\.\d+[:：]\d+", resp.text)
                    raw_list.extend(found)
                self.logger.info(f"  ✓ {source['name']}: {len(raw_list)} (cumulative)")
            except Exception as e:
                pass

        return raw_list

    def fetch_raw_ips(self, max_per_source: int = 100) -> List[str]:
        """从代理源提取代理（根据provider配置）"""
        if self.provider == "kdl":
            return self._fetch_from_kdl(max_per_source)
        elif self.provider == "qingguo":
            return self._fetch_from_qingguo(max_per_source)
        else:
            return self._fetch_from_free()

    def verify_proxy(self, proxy_str: str) -> Optional[str]:
        """验证代理是否可用"""
        proxy_url = proxy_str.replace("：", ":")
        if not proxy_url.startswith("http"):
            proxy_url = "http://" + proxy_url

        # [Modified] 支持用户名密码认证
        final_proxy_url = proxy_url
        if hasattr(self, 'proxy_auth_user') and hasattr(self, 'proxy_auth_pass') \
                and self.proxy_auth_user and self.proxy_auth_pass:
            # 提取IP和端口
            clean_ip = proxy_url.replace("http://", "").replace("https://", "")
            # 构造带认证的URL: http://user:pass@ip:port
            final_proxy_url = f"http://{self.proxy_auth_user}:{self.proxy_auth_pass}@{clean_ip}"
            
        proxies = {"http": final_proxy_url, "https": final_proxy_url}

        try:
            print(f"[VERIFY] Testing proxy: {proxy_url} (Auth: {'Yes' if final_proxy_url != proxy_url else 'No'})")
            start_time = time.time()
            resp = requests.get(
                self.test_url,
                proxies=proxies,
                timeout=self.timeout,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            response_time = time.time() - start_time
            print(
                f"[VERIFY] {proxy_url} - Status: {resp.status_code}, Time: {response_time:.2f}s"
            )

            if resp.status_code == 200:
                # [Based on User Request] 增加内容校验逻辑
                # 必须包含 article_list 且 count 值正常
                content = resp.content.decode("utf-8", "ignore")

                if "var article_list" not in content:
                    print(
                        f"[VERIFY] {proxy_url} - FAIL: No 'var article_list' in content"
                    )
                    return None

                # [Refactor] 校验逻辑变更：不再检查count, 而是检查user_nickname后缀
                try:
                    import json

                    # content is already decoded string
                    start_index = content.find("var article_list")
                    start_json = content.find("{", start_index)

                    if start_json != -1:
                        decoder = json.JSONDecoder()
                        article_list_data, _ = decoder.raw_decode(content[start_json:])

                        items = article_list_data.get("re", [])
                        # 如果没有items, 暂时认为它是有效的（可能是因为没有数据），或者无效？
                        # 原逻辑是必须有count字段。这里我们还是要求解析成功。
                        # 如果有数据，必须满足昵称规则。
                        if items:
                            for item in items:
                                nickname = item.get("user_nickname", "")
                                if not nickname.endswith("资讯"):
                                    print(
                                        f"[VERIFY] {proxy_url} - FAIL: Invalid nickname '{nickname}'"
                                    )
                                    return None

                        # 确保解析正常
                        if "count" not in article_list_data:
                            print(
                                f"[VERIFY] {proxy_url} - FAIL: No 'count' field in article_list"
                            )
                            return None
                    else:
                        print(f"[VERIFY] {proxy_url} - FAIL: Cannot find JSON start")
                        return None

                except Exception as e:
                    print(f"[VERIFY] {proxy_url} - FAIL: JSON parse error: {e}")
                    return None

                score = max(100 - int(response_time * 20), 50)
                print(f"[VERIFY] {proxy_url} - SUCCESS! Score: {score}")
                
                # [Modified] 返回带认证信息的URL以便存储
                return final_proxy_url, score
            else:
                print(f"[VERIFY] {proxy_url} - FAIL: HTTP {resp.status_code}")
        except requests.exceptions.Timeout:
            print(f"[VERIFY] {proxy_url} - FAIL: Timeout")
        except requests.exceptions.ProxyError as e:
            print(f"[VERIFY] {proxy_url} - FAIL: Proxy error: {e}")
        except requests.exceptions.ConnectionError as e:
            print(f"[VERIFY] {proxy_url} - FAIL: Connection error: {e}")
        except Exception as e:
            print(f"[VERIFY] {proxy_url} - FAIL: Unexpected error: {e}")
        return None

    def build_pool(self, max_workers: int = 30, max_per_source: int = 100):
        """初始建立代理池"""
        all_raw_ips = []

        if self.provider == "kdl" or self.provider == "qingguo":
            # 付费API模式：需要多次调用
            calls_needed = (max_per_source + 4) // 5
            if self.provider == "kdl":
                # KDL每次可以取 amount 个, 简单起见按 amount 估算
                calls_needed = (max_per_source + self.kdl_amount - 1) // self.kdl_amount

            self.logger.info(
                f"📊 预计需要 {calls_needed} 次API调用以获取 {max_per_source} 个代理"
            )

            for i in range(calls_needed):
                raw_ips = self.fetch_raw_ips(max_per_source=5)
                all_raw_ips.extend(raw_ips)

                # API限制：60次/分钟
                if i < calls_needed - 1:
                    time.sleep(1.2)
        else:
            # 免费代理模式：一次性抽取
            all_raw_ips = self.fetch_raw_ips(max_per_source)

        self.logger.info(f"🔍 开始验证（{max_workers}线程）...\n")

        valid_count = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_proxy = {
                executor.submit(self.verify_proxy, ip): ip for ip in all_raw_ips
            }

            for future in as_completed(future_to_proxy):
                result = future.result()
                if result:
                    proxy_url, score = result
                    self.add_proxy(proxy_url, score)
                    valid_count += 1

                    # 检查是否达到最大值
                    if self.count() >= self.max_count:
                        self.logger.info(
                            f"⚠️ 已达到代理池最大值 ({self.max_count})，停止获取"
                        )
                        break

        self.logger.info(f"\n✅ 验证完成，获得 {valid_count} 个有效代理")
        return valid_count

    def refill_pool(self, target_count: int = 20, max_workers: int = 30):
        """补充代理池"""
        print(
            f"[DEBUG] refill_pool called: target_count={target_count}, max_workers={max_workers}"
        )
        current = self.count()
        print(f"[DEBUG] current count: {current}, max_count: {self.max_count}")

        # 检查是否已达到最大值
        if current >= self.max_count:
            self.logger.info(
                f"✅ 代理池已达到最大值 ({current}/{self.max_count})，无需补充"
            )
            print(f"[DEBUG] Reached max count, returning")
            return

        self.logger.info(
            f"🔄 代理池补充（当前{current}个，目标{target_count}个，最大{self.max_count}个）"
        )
        print(f"[DEBUG] Starting refill process")

        # 计算需要补充的数量，但不超过最大值
        needed = min(max(0, target_count - current), self.max_count - current)
        print(f"[DEBUG] needed: {needed}")
        if needed == 0:
            self.logger.info("✅ 代理池已足够，无需补充")
            print(f"[DEBUG] needed=0, returning")
            return

        all_raw_ips = []

        if self.provider == "kdl" or self.provider == "qingguo":
            # 付费API模式：多次调用直到达到目标
            calls_needed = (needed + 4) // 5
            self.logger.info(
                f"📊 需要补充 {needed} 个代理，预计需要 {calls_needed} 次API调用"
            )
            print(f"[DEBUG] Paid API mode: calls_needed={calls_needed}")

            for i in range(calls_needed):
                print(f"[DEBUG] API call {i + 1}/{calls_needed}")
                raw_ips = self.fetch_raw_ips(max_per_source=5)
                print(f"[DEBUG] Got {len(raw_ips)} raw IPs")
                all_raw_ips.extend(raw_ips)

                # 如果已经获取足够的代理，提前退出
                if len(all_raw_ips) >= needed:
                    break

                # API限制：60次/分钟
                if i < calls_needed - 1:
                    time.sleep(1.2)
        else:
            # 免费代理模式：一次性抽取
            print(f"[DEBUG] Free proxy mode")
            all_raw_ips = self.fetch_raw_ips(max_per_source=100)
            print(f"[DEBUG] Got {len(all_raw_ips)} raw IPs")

        print(f"[DEBUG] Total raw IPs collected: {len(all_raw_ips)}")

        # 过滤已存在的
        existing = set(self.redis_client.hkeys(self.cache_key))
        new_ips = [ip for ip in all_raw_ips if f"http://{ip}" not in existing]

        self.logger.info(f"📊 过滤后 {len(new_ips)} 个新候选")
        print(f"[DEBUG] new_ips after filtering: {len(new_ips)}")

        added = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_proxy = {
                executor.submit(self.verify_proxy, ip): ip for ip in new_ips
            }

            for future in as_completed(future_to_proxy):
                result = future.result()
                if result:
                    proxy_url, score = result
                    self.add_proxy(proxy_url, score)
                    added += 1

                    # 检查是否达到目标或最大值
                    current_count = self.count()
                    if current_count >= target_count or current_count >= self.max_count:
                        if current_count >= self.max_count:
                            self.logger.info(
                                f"⚠️ 已达到代理池最大值 ({self.max_count})，停止补充"
                            )
                        break

        self.logger.info(f"✅ 补充完成，新增{added}个，当前共{self.count()}个\n")
        print(f"[DEBUG] refill_pool completed: added {added}, total: {self.count()}")

    def revalidate_pool(self, max_workers: int = 20):
        """重新验证所有代理"""
        proxies = self.get_all()
        self.logger.info(f"🔄 重新验证 {len(proxies)} 个代理...")

        # 清空
        self.redis_client.delete(self.cache_key)

        valid_count = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_proxy = {
                executor.submit(self.verify_proxy, p["proxy"].replace("http://", "")): p
                for p in proxies
            }

            for future in as_completed(future_to_proxy):
                result = future.result()
                if result:
                    proxy_url, score = result
                    self.add_proxy(proxy_url, score)
                    valid_count += 1

        self.logger.info(f"✅ 验证完成，保留 {valid_count} 个有效代理\n")

    def start_maintenance_loop(self, check_interval: int = 300, min_threshold: int = 5):
        """
        启动代理池维护循环（守护线程）
        :param check_interval: 检查间隔（秒）
        :param min_threshold: 最小可用数量阈值
        """
        if hasattr(self, "_maintenance_thread") and self._maintenance_thread.is_alive():
            self.logger.warning("代理池维护线程已在运行")
            return

        self._running = True
        self._maintenance_thread = threading.Thread(
            target=self._maintenance_loop,
            args=(check_interval, min_threshold),
            name="ProxyMaintenanceThread",
            daemon=True,
        )
        self._maintenance_thread.start()
        self.logger.info("✓ 代理池维护线程已启动")

    def stop_maintenance_loop(self):
        """停止维护循环"""
        self._running = False

    def _maintenance_loop(self, check_interval, min_threshold):
        """维护循环实体"""
        self.logger.info(
            f"代理池维护线程运行中 (阈值: {min_threshold}, 间隔: {check_interval}秒)"
        )

        # 首次检查
        if self.count() < min_threshold:
            self.logger.info(f"首次检测代理不足({self.count()})，执行初始补充...")
            self.build_pool(max_workers=50, max_per_source=200)

        while getattr(self, "_running", True):
            try:
                current_count = self.count()

                if current_count < min_threshold:
                    self.logger.warning(
                        f"⚠️ [自动维护] 代理池不足: {current_count}/{min_threshold}，开始补充..."
                    )
                    # 1. 重新验证现有
                    self.revalidate_pool()
                    # 2. 如果仍不足，补充
                    if self.count() < min_threshold:
                        self.refill_pool(target_count=self.target_count)
                        self.logger.info(
                            f"✓ [自动维护] 补充完成，当前可用: {self.count()}"
                        )
                else:
                    # self.logger.debug(f"[自动维护] 代理池健康 ({current_count}个)")
                    pass

                time.sleep(check_interval)

            except Exception as e:
                self.logger.error(f"代理池维护循环异常: {e}")
                time.sleep(60)

    # 文件存储相关方法已移除，完全使用Redis管理


if __name__ == "__main__":
    # 测试
    print("=" * 60)
    print("代理池管理器测试（Redis版本）")
    print("=" * 60)

    manager = ProxyManager()

    # 测试Redis连接
    try:
        manager.redis_client.ping()
        print("✅ Redis连接成功\n")
    except Exception:
        print("❌ Redis连接失败，请启动Redis服务\n")
        exit(1)

    # 建立代理池
    manager.build_pool(max_workers=30, max_per_source=50)

    # 获取代理
    print(f"\n当前代理数: {manager.count()}")
    proxy = manager.get_random_proxy()
    print(f"随机代理: {proxy}")

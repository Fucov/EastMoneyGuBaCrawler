import requests
import re
import time
import redis
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Dict
import random
from storage.logger import get_system_logger

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
        context=None,
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
        self.context = context

        # 代理源配置
        self.sources = [
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

    def fetch_raw_ips(self, max_per_source: int = 100) -> List[str]:
        """从源站抓取原始IP列表"""
        raw_list = []
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }

        self.logger.info("📡 开始抓取代理源...")
        for source in self.sources:
            try:
                url = source["url"]
                name = source["name"]
                source_type = source["type"]
                params = source.get("params", {})

                if source_type == "zdaili_api":
                    resp = requests.get(url, params=params, headers=headers, timeout=10)
                    time.sleep(10)
                else:
                    resp = requests.get(url, headers=headers, timeout=10)

                # 解析响应
                if source_type == "zdaili_api":
                    try:
                        data = resp.json()
                        code = str(data.get("code"))
                        if code == "10001":
                            proxy_list = data.get("data", {}).get("proxy_list", [])
                            for proxy_info in proxy_list:
                                ip = proxy_info.get("ip")
                                port = proxy_info.get("port")
                                if ip and port:
                                    raw_list.append(f"{ip}:{port}")
                            self.logger.info(f"  ✓ {name}: {len(proxy_list)}个")
                        else:
                            self.logger.warning(f"  ✗ {name}: {data.get('msg')}")
                    except:
                        pass
                else:
                    # 文本格式
                    found = re.findall(r"\d+\.\d+\.\d+\.\d+[:：]\d+", resp.text)
                    raw_list.extend(found)
                    self.logger.info(f"  ✓ {name}: {len(found)}个")

            except Exception as e:
                self.logger.warning(f"  ✗ {source['name']}: {e}")

        unique_list = list(set(raw_list))
        if not unique_list:
            self.logger.warning(
                "⚠️ 警告: 从所有源获取到的IP数量为0，请检查网络或源站可用性"
            )
        self.logger.info(f"📊 共抓取 {len(unique_list)} 个唯一代理\n")
        return unique_list

    def verify_proxy(self, proxy_str: str) -> Optional[str]:
        """验证代理是否可用"""
        proxy_url = proxy_str.replace("：", ":")
        if not proxy_url.startswith("http"):
            proxy_url = "http://" + proxy_url

        proxies = {"http": proxy_url, "https": proxy_url}

        try:
            start_time = time.time()
            resp = requests.get(
                self.test_url,
                proxies=proxies,
                timeout=self.timeout,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            response_time = time.time() - start_time

            if resp.status_code == 200:
                # [Based on User Request] 增加内容校验逻辑
                # 必须包含 article_list 且 count 值正常
                content = resp.content.decode("utf-8", "ignore")

                if "var article_list" not in content:
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
                                    # self.logger.debug(f"⚠️ {proxy_url} 返回异常昵称 ({nickname})")
                                    return None

                        # 确保解析正常
                        if "count" not in article_list_data:
                            return None
                    else:
                        return None

                except Exception:
                    return None

                score = max(100 - int(response_time * 20), 50)
                # self.logger.debug(f" ✓ {proxy_url} (响应{response_time:.2f}s, 评分{score})")
                return proxy_url, score
        except:
            pass

        return None

    def build_pool(self, max_workers: int = 30, max_per_source: int = 100):
        """初始建立代理池"""
        raw_ips = self.fetch_raw_ips(max_per_source)
        self.logger.info(f"🔍 开始验证（{max_workers}线程）...\n")

        valid_count = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_proxy = {
                executor.submit(self.verify_proxy, ip): ip for ip in raw_ips
            }

            for future in as_completed(future_to_proxy):
                result = future.result()
                if result:
                    proxy_url, score = result
                    self.add_proxy(proxy_url, score)
                    valid_count += 1

        self.logger.info(f"\n✅ 验证完成，获得 {valid_count} 个有效代理")
        return valid_count

    def refill_pool(self, target_count: int = 20, max_workers: int = 30):
        """补充代理池"""
        current = self.count()
        self.logger.info(f"🔄 代理池补充（当前{current}个，目标{target_count}个）")

        raw_ips = self.fetch_raw_ips(max_per_source=100)

        # 过滤已存在的
        existing = set(self.redis_client.hkeys(self.cache_key))
        new_ips = [ip for ip in raw_ips if f"http://{ip}" not in existing]

        self.logger.info(f"📊 过滤后 {len(new_ips)} 个新候选")

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

                    if self.count() >= target_count:
                        break

        self.logger.info(f"✅ 补充完成，新增{added}个，当前共{self.count()}个\n")

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
    except:
        print("❌ Redis连接失败，请启动Redis服务\n")
        exit(1)

    # 建立代理池
    manager.build_pool(max_workers=30, max_per_source=50)

    # 获取代理
    print(f"\n当前代理数: {manager.count()}")
    proxy = manager.get_random_proxy()
    print(f"随机代理: {proxy}")

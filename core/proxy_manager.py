#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
代理池管理器 - 使用Redis缓存代理
重构自 proxy_pool.py，优化为生产环境使用
"""

import requests
import re
import time
import json
import redis
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Dict
import random


class ProxyManager:
    """
    代理池管理器（Redis版本）

    特性：
    - Redis持久化代理
    - 自动验证和评分
    - 失效自动移除
    - 低于阈值自动补充
    """

    def __init__(
        self,
        redis_host="localhost",
        redis_port=6379,
        redis_password=None,
        redis_db=0,
        cache_key="guba:proxies:valid",
        target_url="https://guba.eastmoney.com/",
        min_threshold=5,
    ):
        """
        初始化代理管理器

        Args:
            redis_host: Redis主机
            redis_port: Redis端口
            redis_password: Redis密码（None表示无密码）
            redis_db: Redis数据库编号
            cache_key: 缓存key
            target_url: 验证目标URL
            min_threshold: 最小代理阈值
        """
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

        # 代理源配置
        self.sources = [
            {
                "type": "text",
                "url": "http://api.89ip.cn/tqdl.html?api=1&num=60",
                "name": "89ip-API",
            },
            {
                "type": "text",
                "url": "https://proxy.scdn.io/text.php",
                "name": "proxy.scdn.io",
            },
        ]

    def count(self) -> int:
        """获取当前有效代理数量"""
        return self.redis_client.hlen(self.cache_key)

    def get_all(self) -> List[Dict]:
        """
        获取所有代理

        Returns:
            [{'proxy': 'http://...', 'score': 95}, ...]
        """
        proxies = []
        for proxy_url, score in self.redis_client.hgetall(self.cache_key).items():
            proxies.append({"proxy": proxy_url, "score": int(score)})

        # 按评分排序
        proxies.sort(key=lambda x: x["score"], reverse=True)
        return proxies

    def get_random_proxy(self) -> Optional[Dict]:
        """
        随机获取一个代理

        Returns:
            {'http': 'http://...', 'https': 'http://...'} 或 None
        """
        # 检查阈值
        if self.count() < self.min_threshold:
            print(f"⚠️ 代理池不足({self.count()}个)，触发自动补充...")
            self.refill_pool()

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
        self.redis_client.hset(self.cache_key, proxy_url, score)

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

        print("📡 开始抓取代理源...")
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
                            print(f"  ✓ {name}: {len(proxy_list)}个")
                        else:
                            print(f"  ✗ {name}: {data.get('msg')}")
                    except:
                        pass
                else:
                    # 文本格式
                    found = re.findall(r"\d+\.\d+\.\d+\.\d+[:：]\d+", resp.text)
                    raw_list.extend(found)
                    print(f"  ✓ {name}: {len(found)}个")

            except Exception as e:
                print(f"  ✗ {source['name']}: {e}")

        unique_list = list(set(raw_list))
        print(f"📊 共抓取 {len(unique_list)} 个唯一代理\n")
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
                score = max(100 - int(response_time * 20), 50)
                print(f" ✓ {proxy_url} (响应{response_time:.2f}s, 评分{score})")
                return proxy_url, score
        except:
            pass

        return None

    def build_pool(self, max_workers: int = 30, max_per_source: int = 100):
        """初始建立代理池"""
        raw_ips = self.fetch_raw_ips(max_per_source)
        print(f"🔍 开始验证（{max_workers}线程）...\n")

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

        print(f"\n✅ 验证完成，获得 {valid_count} 个有效代理")
        return valid_count

    def refill_pool(self, target_count: int = 20, max_workers: int = 30):
        """补充代理池"""
        current = self.count()
        print(f"🔄 代理池补充（当前{current}个，目标{target_count}个）")

        raw_ips = self.fetch_raw_ips(max_per_source=100)

        # 过滤已存在的
        existing = set(self.redis_client.hkeys(self.cache_key))
        new_ips = [ip for ip in raw_ips if f"http://{ip}" not in existing]

        print(f"📊 过滤后 {len(new_ips)} 个新候选")

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

        print(f"✅ 补充完成，新增{added}个，当前共{self.count()}个\n")

    def revalidate_pool(self, max_workers: int = 20):
        """重新验证所有代理"""
        proxies = self.get_all()
        print(f"🔄 重新验证 {len(proxies)} 个代理...")

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

        print(f"✅ 验证完成，保留 {valid_count} 个有效代理\n")

    def save_to_file(self, filename: str = "valid_proxies.txt"):
        """
        将Redis中的代理导出到文件备份

        Args:
            filename: 文件名
        """
        proxies = self.get_all()
        with open(filename, "w") as f:
            for proxy in proxies:
                f.write(f"{proxy['proxy']} # 评分:{proxy['score']}\n")
        print(f"💾 已保存{len(proxies)}个代理到 {filename}")

    def load_from_file(self, filename: str = "valid_proxies.txt") -> bool:
        """
        从文件加载代理到Redis

        Args:
            filename: 文件名

        Returns:
            是否成功加载（至少1个代理）
        """
        try:
            loaded_count = 0
            with open(filename, "r") as f:
                for line in f:
                    if line.strip() and not line.startswith("#"):
                        proxy = line.split("#")[0].strip()
                        if proxy:
                            # 解析评分
                            score = 100
                            if "评分:" in line:
                                try:
                                    score = int(line.split("评分:")[1].strip())
                                except:
                                    pass
                            self.add_proxy(proxy, score)
                            loaded_count += 1

            if loaded_count > 0:
                print(f"📂 从 {filename} 加载了 {loaded_count} 个代理")
                return True
            else:
                print(f"⚠️ {filename} 文件为空，将重新抓取")
                return False
        except FileNotFoundError:
            print(f"⚠️ 文件 {filename} 不存在")
            return False


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

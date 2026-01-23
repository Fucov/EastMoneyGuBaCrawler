#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
免费代理池 - 专为东方财富股吧爬虫设计
自动抓取、验证、管理免费代理IP
"""

import requests
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional
import random


class ProxyPool:
    """
    免费代理池管理器
    
    功能：
    1. 从多个源抓取免费代理
    2. 并发验证代理有效性
    3. 针对目标网站测试
    4. 动态评分机制
    """
    
    def __init__(self, target_url="https://guba.eastmoney.com/", min_threshold=5):
        """
        初始化代理池
        
        Args:
            target_url: 目标网站，用于验证代理是否被目标站封禁
            min_threshold: 最小代理数量阈值，低于此值自动补充
        """
        # 免费代理源（定期维护）
        self.sources = [
            # 站大爷专业免费代理API（推荐，高质量）
            {
                'type': 'zdaili_api',
                'url': 'http://www.zdopen.com/FreeProxy/Get/',
                'name': '站大爷',
                'params': {
                    'app_id': '202601221510072813',
                    'akey': 'febfef47436c13bf',
                    'count': 100,
                    'return_type': 3,  # JSON格式
                    'level_type': 1,  # 高匿
                    'lastcheck_type': 2,  # 10分钟内验证
                    'sleep_type': 3,  # 5秒内响应
                }
            },
            # 89ip API（新版）
            {
                'type': 'text',
                'url': 'http://api.89ip.cn/tqdl.html?api=1&num=60&port=&address=&isp=',
                'name': '89ip-API'
            },
            # ProxyShare JSON API
            {
                'type': 'json_list',
                'url': 'https://www.proxyshare.com/web_v1/free-proxy/list?page_size=10&page=1&language=zh',
                'name': 'ProxyShare'
            },
            # ProxyList JSON API
            {
                'type': 'json_list',
                'url': 'http://43.135.31.113:8777/proxyList?limit=50&page=1&language=zh-hans',
                'name': 'ProxyList'
            },
            # proxy.scdn.io
            {
                'type': 'text',
                'url': 'https://proxy.scdn.io/text.php',
                'name': 'proxy.scdn.io'
            },
            # GitHub开源列表
            {
                'type': 'text',
                'url': 'https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt',
                'name': 'GitHub-TheSpeedX'
            },
            {
                'type': 'text',
                'url': 'https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt',
                'name': 'GitHub-clarketm'
            }
        ]
        
        self.valid_proxies = []  # 有效代理列表
        self.proxy_scores = {}   # 代理评分 {proxy_url: score}
        self.test_url = target_url
        self.timeout = 3  # 免费代理超过3秒没响应基本没用
        self.min_threshold = min_threshold  # 最小代理阈值
        self.last_refill_time = 0  # 上次补充时间戳（用于站大爷API限流）
        
    def fetch_raw_ips(self, max_per_source: int = 999999) -> List[str]:
        """
        从源站点抓取原始 IP:PORT 文本
        支持多种格式：
        1. 站大爷JSON API
        2. 通用JSON列表
        3. 纯文本列表
        
        Args:
            max_per_source: 每个源最多获取的代理数量，默认不限制
        
        Returns:
            去重后的代理列表
        """
        raw_list = []
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        print("📡 开始抓取代理源...")
        for source in self.sources:
            try:
                if isinstance(source, dict):
                    url = source['url']
                    name = source['name']
                    source_type = source['type']
                    params = source.get('params', {})
                else:
                    # 兼容旧格式
                    url = source
                    name = url.split('/')[2]
                    source_type = 'text'
                    params = {}
                
                # 根据类型选择请求方法
                if source_type == 'zdaili_api':
                    # 站大爷支持GET/POST，使用GET更稳定
                    resp = requests.get(url, params=params, headers=headers, timeout=10)
                    time.sleep(10)  # API要求至少10秒间隔
                else:
                    resp = requests.get(url, headers=headers, timeout=10)
                
                # 处理不同类型的响应
                if source_type == 'zdaili_api':
                    # 站大爷JSON API
                    try:
                        data = resp.json()
                        code = str(data.get('code'))  # 转为字符串比较
                        if code == '10001':  # 成功
                            proxy_list = data.get('data', {}).get('proxy_list', [])
                            for proxy_info in proxy_list:
                                ip = proxy_info.get('ip')
                                port = proxy_info.get('port')
                                if ip and port:
                                    raw_list.append(f"{ip}:{port}")
                            print(f"  ✓ 从 {name} (站大爷API) 获取了 {len(proxy_list)} 个候选代理")
                        else:
                            msg = data.get('msg', 'unknown')
                            print(f"  ✗ {name} API错误: code={code}, msg={msg}")
                    except ValueError as e:
                        print(f"  ✗ {name} JSON解析失败")
                        
                elif source_type == 'json_list':
                    # 通用JSON列表格式（ProxyShare等）
                    try:
                        data = resp.json()
                        proxies = []
                        
                        # 尝试多种可能的JSON结构
                        if isinstance(data, list):
                            proxies = data
                        elif 'data' in data:
                            if isinstance(data['data'], list):
                                proxies = data['data']
                            elif isinstance(data['data'], dict):
                                proxies = data['data'].get('list', [])
                        elif 'list' in data:
                            proxies = data['list']
                        
                        # 提取IP:Port
                        for item in proxies:
                            if isinstance(item, dict):
                                ip = item.get('ip') or item.get('host')
                                port = item.get('port')
                                if ip and port:
                                    raw_list.append(f"{ip}:{port}")
                            elif isinstance(item, str):
                                raw_list.append(item)
                        
                        print(f"  ✓ 从 {name} (JSON) 获取了 {len(proxies)} 个候选代理")
                    except ValueError:
                        # JSON解析失败，尝试文本格式
                        found = re.findall(r'\d+\.\d+\.\d+\.\d+[:：]\d+', resp.text)
                        raw_list.extend(found)
                        print(f"  ✓ 从 {name} (文本fallback) 获取了 {len(found)} 个候选代理")
                        
                else:
                    # 纯文本格式
                    found = re.findall(r'\d+\.\d+\.\d+\.\d+[:：]\d+', resp.text)
                    raw_list.extend(found)
                    print(f"  ✓ 从 {name} (文本) 获取了 {len(found)} 个候选代理")
                    
            except Exception as e:
                source_name = source.get('name', source) if isinstance(source, dict) else source.split('/')[2]
                print(f"  ✗ 抓取 {source_name} 失败: {e}")
        
        unique_list = list(set(raw_list))  # 去重
        print(f"📊 共抓取到 {len(unique_list)} 个唯一代理\n")
        return unique_list
    
    def verify_proxy(self, proxy_str: str) -> Optional[str]:
        """
        验证代理是否真正可用
        
        Args:
            proxy_str: 代理字符串，格式如 "1.2.3.4:8080"
        
        Returns:
            如果有效返回完整代理URL，否则返回None
        """
        # 统一格式为 http://ip:port
        proxy_url = proxy_str.replace('：', ':')
        if not proxy_url.startswith('http'):
            proxy_url = 'http://' + proxy_url
        
        proxies = {"http": proxy_url, "https": proxy_url}
        
        try:
            # 必须设置 timeout，否则程序会卡死在坏 IP 上
            start_time = time.time()
            resp = requests.get(
                self.test_url, 
                proxies=proxies, 
                timeout=self.timeout,
                headers={'User-Agent': 'Mozilla/5.0'}
            )
            response_time = time.time() - start_time
            
            if resp.status_code == 200:
                # 初始评分：基于响应时间
                initial_score = max(100 - int(response_time * 20), 50)
                self.proxy_scores[proxy_url] = initial_score
                
                print(f" ✓ 发现有效代理: {proxy_url} (响应时间: {response_time:.2f}s, 评分: {initial_score})")
                return proxy_url
        except Exception as e:
            # 静默失败，避免刷屏
            pass
        
        return None
    
    def build_pool(self, max_workers: int = 30, max_per_source: int = 999999) -> List[str]:
        """
        运行入口：抓取并并发校验
        
        Args:
            max_workers: 并发线程数，建议20-50
            max_per_source: 每个源最多获取的代理数，默认不限
        
        Returns:
            有效代理列表
        """
        raw_ips = self.fetch_raw_ips(max_per_source=max_per_source)
        print(f"🔍 开始高并发验证（{max_workers}线程）...\n")
        
        # 使用多线程池
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_proxy = {
                executor.submit(self.verify_proxy, ip): ip 
                for ip in raw_ips
            }
            
            for future in as_completed(future_to_proxy):
                result = future.result()
                if result:
                    self.valid_proxies.append(result)
        
        print(f"\n{'='*60}")
        print(f"验证完成！获得有效代理 {len(self.valid_proxies)} 个")
        print(f"{'='*60}\n")
        
        # 按评分排序
        self.valid_proxies.sort(
            key=lambda x: self.proxy_scores.get(x, 0), 
            reverse=True
        )
        
        return self.valid_proxies
    # proxy_pool.py 建议增加/修改的方法

    def remove_proxy(self, proxy_url_dict: dict):
        """实时移除失效代理"""
        if not proxy_url_dict: return
        p_url = proxy_url_dict.get('http')
        if p_url in self.valid_proxies:
            self.valid_proxies.remove(p_url)
            if p_url in self.proxy_scores:
                del self.proxy_scores[p_url]
            # print(f"🗑️ 实时剔除失效代理: {p_url}")

    def revalidate_pool(self, max_workers: int = 20):
        """在使用前验证本地已有的代理，无效的删除"""
        if not self.valid_proxies:
            return
        
        print(f"🔄 正在预检本地 {len(self.valid_proxies)} 个代理的有效性...")
        old_proxies = list(self.valid_proxies)
        self.valid_proxies = [] # 清空，准备接收有效的
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_proxy = {executor.submit(self.verify_proxy, p): p for p in old_proxies}
            for future in as_completed(future_to_proxy):
                result = future.result()
                if result:
                    self.valid_proxies.append(result)
        
        # 重新排序并保存
        self.valid_proxies.sort(key=lambda x: self.proxy_scores.get(x, 0), reverse=True)
        self.save_to_file()
        print(f"✅ 预检完成，保留有效代理 {len(self.valid_proxies)} 个\n")
    
    def refill_pool(self, target_count: int = 20, max_workers: int = 30):
        """
        当代理池数量不足时，从所有代理源补充新代理
        
        Args:
            target_count: 目标补充数量
            max_workers: 并发验证线程数
        """
        current_count = len(self.valid_proxies)
        print(f"\n🔄 开始自动补充代理（当前{current_count}个，目标{target_count}个）...")
        
        # 获取新的原始IP列表
        raw_ips = self.fetch_raw_ips(max_per_source=100)
        
        if not raw_ips:
            print("❌ 未获取到新代理，补充失败")
            return
        
        # 去除已存在的代理
        new_ips = [ip for ip in raw_ips if not any(ip in p for p in self.valid_proxies)]
        print(f"📊 过滤后获得 {len(new_ips)} 个新候选代理")
        
        # 并发验证
        print(f"🔍 开始验证（{max_workers}线程）...")
        newly_added = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_proxy = {executor.submit(self.verify_proxy, ip): ip for ip in new_ips}
            for future in as_completed(future_to_proxy):
                result = future.result()
                if result and result not in self.valid_proxies:
                    self.valid_proxies.append(result)
                    newly_added += 1
                    # 达到目标数量，提前停止
                    if len(self.valid_proxies) >= target_count:
                        break
        
        # 重新排序并保存
        self.valid_proxies.sort(key=lambda x: self.proxy_scores.get(x, 0), reverse=True)
        self.save_to_file()
        
        print(f"✅ 补充完成！新增{newly_added}个，当前共{len(self.valid_proxies)}个有效代理\n")


    def get_random_proxy(self) -> Optional[dict]:
        # 检查阈值，触发自动补充
        if len(self.valid_proxies) < self.min_threshold:
            print(f"⚠️ 代理池不足({len(self.valid_proxies)}个)，触发自动补充...")
            self.refill_pool()
        
        if not self.valid_proxies:
            return None
        
        # 稍微优化：如果代理多，从高分里选；如果少，全部随机选
        if len(self.valid_proxies) > 10:
            top_half = self.valid_proxies[:max(1, len(self.valid_proxies) // 2)]
            selected = random.choice(top_half)
        else:
            selected = random.choice(self.valid_proxies)
        
        return {"http": selected, "https": selected}
    
    def update_score(self, proxy_url: str, success: bool):
        """
        更新代理评分
        
        Args:
            proxy_url: 代理URL
            success: 是否成功
        """
        if proxy_url not in self.proxy_scores:
            self.proxy_scores[proxy_url] = 100
        
        if success:
            self.proxy_scores[proxy_url] = min(100, self.proxy_scores[proxy_url] + 5)
        else:
            self.proxy_scores[proxy_url] = max(0, self.proxy_scores[proxy_url] - 10)
        
        # 如果评分低于30，从有效列表中移除
        if self.proxy_scores[proxy_url] < 30 and proxy_url in self.valid_proxies:
            self.valid_proxies.remove(proxy_url)
            print(f"⚠️ 移除低分代理: {proxy_url} (评分: {self.proxy_scores[proxy_url]})")
    
    def save_to_file(self, filename: str = "valid_proxies.txt"):
        """
        保存有效代理到文件
        
        Args:
            filename: 文件名
        """
        with open(filename, 'w') as f:
            for proxy in self.valid_proxies:
                score = self.proxy_scores.get(proxy, 0)
                f.write(f"{proxy} # 评分:{score}\n")
        print(f"💾 已保存{len(self.valid_proxies)}个代理到 {filename}")
    
    def load_from_file(self, filename: str = "valid_proxies.txt") -> bool:
        """
        从文件加载代理
        
        Args:
            filename: 文件名
        
        Returns:
            是否成功加载（至少1个代理）
        """
        try:
            with open(filename, 'r') as f:
                for line in f:
                    if line.strip() and not line.startswith('#'):
                        proxy = line.split('#')[0].strip()
                        if proxy:  # 确保不是空行
                            self.valid_proxies.append(proxy)
                            # 尝试解析评分
                            if '评分:' in line:
                                try:
                                    score = int(line.split('评分:')[1].strip())
                                    self.proxy_scores[proxy] = score
                                except:
                                    pass
            
            # 检查是否加载到有效代理
            if len(self.valid_proxies) > 0:
                print(f"📂 从 {filename} 加载了 {len(self.valid_proxies)} 个代理")
                return True
            else:
                print(f"⚠️ {filename} 文件为空，将重新抓取")
                return False
                
        except FileNotFoundError:
            print(f"⚠️ 文件 {filename} 不存在")
            return False


# 独立测试
if __name__ == "__main__":
    print("="*60)
    print("免费代理池 - 东方财富股吧专用")
    print("="*60 + "\n")
    
    pool = ProxyPool(target_url="https://guba.eastmoney.com/")
    proxies = pool.build_pool(max_workers=30)
    
    if proxies:
        print("\n📋 可用代理示例（前5个）:")
        for i, proxy in enumerate(proxies[:5], 1):
            score = pool.proxy_scores.get(proxy, 0)
            print(f"  {i}. {proxy} (评分: {score})")
        
        # 保存到文件
        pool.save_to_file()
        
        # 测试随机获取
        print("\n🎲 随机获取一个代理:")
        random_proxy = pool.get_random_proxy()
        print(f"  {random_proxy}")
    else:
        print("\n❌ 未找到可用代理，请稍后重试或检查网络")

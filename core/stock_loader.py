#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票加载器 - 从AkShare获取所有A股代码
"""

import time
from typing import List, Dict, Optional

class StockLoader:
    """
    从AkShare加载股票列表
    
    特性：
    - 自动缓存24小时
    - 过滤ST、退市股票
    - 支持按市场筛选
    """
    
    _cache: Optional[List[str]] = None
    _cache_time: Optional[float] = None
    _cache_ttl = 3600 * 24  # 缓存24小时
    
    def __init__(self, exclude_st=True, exclude_delisted=True):
        """
        初始化
        
        Args:
            exclude_st: 是否排除ST股票
            exclude_delisted: 是否排除退市股票
        """
        self.exclude_st = exclude_st
        self.exclude_delisted = exclude_delisted
    
    def get_all_stocks(self) -> List[str]:
        """
        获取所有A股代码
        
        Returns:
            股票代码列表，如 ['600519', '000001', ...]
        """
        # 检查缓存
        if self._is_cache_valid():
            print(f"✓ 使用缓存的股票列表（{len(self._cache)}只）")
            return self._cache
        
        # 从AkShare加载
        try:
            print("📡 从 AkShare 加载股票列表...")
            import akshare as ak
            df = ak.stock_info_a_code_name()
            
            stock_list = []
            skipped_count = 0
            
            for _, row in df.iterrows():
                code = row["code"]
                name = row["name"]
                
                # 过滤规则
                if self.exclude_st and ("ST" in name or "st" in name):
                    skipped_count += 1
                    continue
                
                if self.exclude_delisted and ("退" in name):
                    skipped_count += 1
                    continue
                
                stock_list.append(code)
            
            # 更新缓存
            self._cache = stock_list
            self._cache_time = time.time()
            
            print(f"✅ 加载了 {len(stock_list)} 只股票（跳过{skipped_count}只）")
            return stock_list
            
        except Exception as e:
            print(f"❌ 加载股票失败: {e}")
            # 如果有旧缓存，返回旧缓存
            if self._cache:
                print(f"⚠️ 使用旧缓存（{len(self._cache)}只）")
                return self._cache
            return []
    
    def get_stock_info(self, code: str) -> Optional[Dict]:
        """
        获取单只股票信息
        
        Args:
            code: 股票代码
        
        Returns:
            股票信息字典 {'code': '600519', 'name': '贵州茅台', 'market': 'SH'}
        """
        try:
            import akshare as ak
            df = ak.stock_info_a_code_name()
            
            row = df[df['code'] == code]
            if row.empty:
                return None
            
            name = row.iloc[0]['name']
            
            # 判断市场
            if code.startswith("6"):
                market = "SH"
            elif code.startswith(("0", "3")):
                market = "SZ"
            else:
                market = "SZ"
            
            return {
                "code": code,
                "name": name,
                "market": market
            }
            
        except Exception as e:
            print(f"获取股票{code}信息失败: {e}")
            return None
    
    def _is_cache_valid(self) -> bool:
        """检查缓存是否有效"""
        if not self._cache or not self._cache_time:
            return False
        
        elapsed = time.time() - self._cache_time
        return elapsed < self._cache_ttl
    
    def clear_cache(self):
        """清除缓存"""
        self._cache = None
        self._cache_time = None
        print("✓ 缓存已清除")


if __name__ == '__main__':
    # 测试
    loader = StockLoader()
    
    # 测试获取所有股票
    stocks = loader.get_all_stocks()
    print(f"\n前10只股票: {stocks[:10]}")
    
    # 测试获取单只股票信息
    info = loader.get_stock_info('600519')
    print(f"\n贵州茅台信息: {info}")
    
    # 测试缓存
    stocks2 = loader.get_all_stocks()
    print(f"\n第二次调用（使用缓存）")

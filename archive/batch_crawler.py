#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量爬取多个股票的官方咨询
适用于定时任务每日增量更新
"""

import configparser
from main_class import guba_comments
import time
import sys
from datetime import datetime

# A股股票代码列表（示例）
# 你可以从CSV、数据库或API获取完整的A股列表
STOCK_LIST = [
    "600519",  # 贵州茅台
    "000001",  # 平安银行
    "601318",  # 中国平安
    "600036",  # 招商银行
    "000858",  # 五粮液
    # 添加更多股票代码...
    # 可以使用脚本自动获取全部A股
]

def load_stock_list_from_file(filename='stock_list.txt'):
    """
    从文件加载股票列表
    文件格式：每行一个股票代码
    """
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            stocks = [line.strip() for line in f if line.strip()]
        return stocks
    except FileNotFoundError:
        print(f"⚠️ 未找到股票列表文件 {filename}，使用默认列表")
        return STOCK_LIST

def crawl_all_stocks(stock_list=None, pages_per_stock=5):
    """
    爬取所有股票的官方咨询
    
    Args:
        stock_list: 股票代码列表，如果为None则使用默认列表
        pages_per_stock: 每个股票爬取的页数（默认5页，约400条最新数据）
    """
    if stock_list is None:
        stock_list = STOCK_LIST
    
    config = configparser.ConfigParser()
    config.read('config.ini', encoding='utf-8')
    
    total = len(stock_list)
    success_count = 0
    failed_stocks = []
    
    print("\n" + "="*60)
    print(f"批量爬取任务开始")
    print("="*60)
    print(f"任务时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"股票总数: {total}")
    print(f"每股页数: {pages_per_stock}")
    print(f"预计抓取: {total * pages_per_stock * 80} 条数据")
    print("="*60 + "\n")
    
    for idx, stock_code in enumerate(stock_list, 1):
        print(f"\n[{idx}/{total}] 开始爬取: {stock_code}")
        print("-" * 60)
        
        try:
            crawler = guba_comments(
                config_path='config.ini',
                config=config,
                secCode=stock_code,
                pages_start=1,
                pages_end=pages_per_stock,  # 每天只爬前N页最新数据
                num_start=0,
                MongoDB=True,
                collectionName="stock_news",  # 统一Collection
                full_text=False
            )
            crawler.main()
            
            success_count += 1
            print(f"✅ {stock_code} 完成")
            
            # 避免请求过快，防止被封IP
            if idx < total:  # 最后一个不需要等待
                wait_time = 3
                print(f"⏳ 等待 {wait_time} 秒...")
                time.sleep(wait_time)
            
        except KeyboardInterrupt:
            print(f"\n\n⚠️ 用户中断，已完成 {success_count}/{total} 个股票")
            sys.exit(0)
            
        except Exception as e:
            failed_stocks.append((stock_code, str(e)))
            print(f"❌ {stock_code} 失败: {e}")
            continue
    
    # 打印汇总信息
    print("\n" + "="*60)
    print("批量爬取任务完成")
    print("="*60)
    print(f"总数: {total}")
    print(f"成功: {success_count}")
    print(f"失败: {len(failed_stocks)}")
    print(f"成功率: {success_count/total*100:.1f}%")
    
    if failed_stocks:
        print("\n失败的股票:")
        for stock_code, error in failed_stocks:
            print(f"  - {stock_code}: {error}")
    
    print("="*60 + "\n")
    
    return success_count, failed_stocks

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='批量爬取A股官方咨询')
    parser.add_argument('--file', '-f', help='股票列表文件路径')
    parser.add_argument('--pages', '-p', type=int, default=5, help='每个股票爬取页数（默认5）')
    args = parser.parse_args()
    
    # 加载股票列表
    if args.file:
        stock_list = load_stock_list_from_file(args.file)
    else:
        stock_list = STOCK_LIST
    
    # 执行爬取
    crawl_all_stocks(stock_list, args.pages)

if __name__ == '__main__':
    main()

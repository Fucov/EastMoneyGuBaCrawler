#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
生产级24小时爬虫系统
主入口 - 一键启动
"""

import sys
import signal
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from core.scheduler import Crawler24HScheduler
from storage.logger import get_logger


def signal_handler(sig, frame):
    """优雅退出"""
    logger = get_logger('main')
    logger.info("\n收到退出信号，正在停止...")
    sys.exit(0)


def main():
    """主函数"""
    # 注册信号处理
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # 初始化日志
    logger = get_logger('main')
    
    print("="*60)
    print("东方财富股吧爬虫 - 生产系统 v2.0")
    print("="*60)
    print("特性：")
    print("  ✓ 24小时持续运行")
    print("  ✓ 自动获取所有A股代码（AkShare）")
    print("  ✓ 智能IP池管理（Redis缓存）")
    print("  ✓ 完整日志记录（按天切割）")
    print("  ✓ 多线程并发爬取")
    print("="*60)
    print()
    
    try:
        # 创建调度器
        scheduler = Crawler24HScheduler()
        
        logger.info("系统启动")
        logger.info("开始24小时持续爬取")
        
        # 运行
        scheduler.run()
        
    except KeyboardInterrupt:
        logger.info("用户中断，正常退出")
        sys.exit(0)
    except Exception as e:
        logger.exception(f"系统异常")
        sys.exit(1)


if __name__ == '__main__':
    main()

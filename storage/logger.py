#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简化日志系统 - 只保留爬虫主日志和错误日志
"""

import logging
import os
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler


class CrawlerLogger:
    """
    简化的统一日志管理器

    特性：
    - 控制台输出（彩色）
    - 爬虫主日志（按天切割，保留7天）
    - 错误单独记录（按大小切割）
    """

    _instances = {}  # 单例模式

    def __new__(cls, name="crawler"):
        if name not in cls._instances:
            cls._instances[name] = super().__new__(cls)
        return cls._instances[name]

    def __init__(self, name="crawler"):
        # 避免重复初始化
        if hasattr(self, "_initialized"):
            return
        self._initialized = True

        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        # 清除已有handler
        self.logger.handlers.clear()

        # 确保logs目录存在
        os.makedirs("logs", exist_ok=True)

        # 1. 控制台输出（INFO及以上）
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"
        )
        console_handler.setFormatter(console_formatter)

        # 2. 爬虫主日志（按天切割，保留7天）
        file_handler = TimedRotatingFileHandler(
            "logs/crawler.log",
            when="midnight",
            interval=1,
            backupCount=7,  # 只保留7天
            encoding="utf-8",
        )
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(file_formatter)

        # 3. 错误日志（单独文件，按大小切割，最大10MB，保留3个）
        error_handler = RotatingFileHandler(
            "logs/error.log",
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=3,  # 只保留3个备份
            encoding="utf-8",
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(file_formatter)

        # 添加处理器
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)
        self.logger.addHandler(error_handler)

    def debug(self, msg, *args, **kwargs):
        """调试信息"""
        self.logger.debug(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        """普通信息"""
        self.logger.info(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        """警告信息"""
        self.logger.warning(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        """错误信息"""
        self.logger.error(msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        """严重错误"""
        self.logger.critical(msg, *args, **kwargs)

    def exception(self, msg, *args, **kwargs):
        """异常（自动包含堆栈跟踪）"""
        self.logger.exception(msg, *args, **kwargs)


# 便捷函数
def get_logger(name="crawler"):
    """获取日志实例（所有模块统一使用crawler日志）"""
    return CrawlerLogger("crawler")  # 强制使用统一的crawler日志


if __name__ == "__main__":
    # 测试
    logger = get_logger()

    logger.debug("这是调试信息")
    logger.info("这是普通信息")
    logger.warning("这是警告信息")
    logger.error("这是错误信息")

    try:
        1 / 0
    except Exception as e:
        logger.exception("捕获到异常")

    print("\n✅ 日志测试完成，请查看 logs/ 目录")

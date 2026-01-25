#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
双通道日志系统
- progress.log: 宏观进度（轮次、统计）
- details.log: 系统详情（错误、代理池变动、调试信息）
"""

import logging
import os
import sys
from logging.handlers import RotatingFileHandler

# 定义日志名称常量
PROGRESS_LOGGER_NAME = "progress"
SYSTEM_LOGGER_NAME = "system"


def setup_logging():
    """初始化日志配置"""
    # 确保logs目录存在
    os.makedirs("logs", exist_ok=True)

    # 1. 宏观进度日志 (logs/progress.log)
    # 只记录关键流程节点：第几轮、爬取进度、统计结果
    prog_logger = logging.getLogger(PROGRESS_LOGGER_NAME)
    prog_logger.setLevel(logging.INFO)
    prog_logger.propagate = False  # 不向上传播，避免重复

    if not prog_logger.handlers:
        prog_handler = RotatingFileHandler(
            "logs/progress.log",
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
        prog_formatter = logging.Formatter(
            "%(asctime)s %(message)s", datefmt="%H:%M:%S"
        )
        prog_handler.setFormatter(prog_formatter)
        prog_logger.addHandler(prog_handler)

        # 同时输出到控制台(sys.stdout)，方便start.sh查看
        console = logging.StreamHandler(sys.stdout)
        console.setLevel(logging.INFO)
        console.setFormatter(prog_formatter)
        prog_logger.addHandler(console)

    # 2. 系统详情日志 (logs/details.log)
    # 记录错误、代理池活动、详细运行状态
    sys_logger = logging.getLogger(SYSTEM_LOGGER_NAME)
    sys_logger.setLevel(logging.INFO)
    sys_logger.propagate = False

    if not sys_logger.handlers:
        detail_handler = RotatingFileHandler(
            "logs/details.log",
            maxBytes=20 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
        detail_formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        detail_handler.setFormatter(detail_formatter)
        sys_logger.addHandler(detail_handler)


def get_progress_logger():
    """获取进度日志记录器"""
    return logging.getLogger(PROGRESS_LOGGER_NAME)


def get_system_logger():
    """获取系统详情日志记录器"""
    return logging.getLogger(SYSTEM_LOGGER_NAME)


# 兼容旧代码的helper
def get_logger(name="system"):
    if name == "scheduler" or name == "progress":
        # 注意：这里需要调用方自己区分，暂时默认返回system
        # 如果是scheduler，它其实同时需要progress和system
        return logging.getLogger(SYSTEM_LOGGER_NAME)
    return logging.getLogger(SYSTEM_LOGGER_NAME)


if __name__ == "__main__":
    setup_logging()

    p = get_progress_logger()
    p.info("第1轮开始")
    p.info("进度 50/100")

    s = get_system_logger()
    s.info("代理池补充中...")
    s.error("连接超时")

    print("日志测试完成")

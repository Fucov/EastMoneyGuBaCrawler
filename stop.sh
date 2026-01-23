#!/bin/bash
# 东方财富股吧爬虫 - 停止脚本

# 颜色定义
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# 配置
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$SCRIPT_DIR/.crawler.pid"

echo "============================================================"
echo "东方财富股吧爬虫 - 停止"
echo "============================================================"

# 检查PID文件是否存在
if [ ! -f "$PID_FILE" ]; then
    echo -e "${RED}✗ 未找到PID文件，爬虫可能未运行${NC}"
    echo ""
    echo "查找可能的进程:"
    ps aux | grep "python main.py" | grep -v grep
    exit 1
fi

# 读取PID
PID=$(cat "$PID_FILE")

# 检查进程是否存在
if ! ps -p $PID > /dev/null 2>&1; then
    echo -e "${YELLOW}⚠ 进程不存在（PID: $PID）${NC}"
    rm -f "$PID_FILE"
    exit 1
fi

# 尝试优雅停止
echo "发送SIGINT信号（优雅退出）..."
kill -SIGINT $PID

# 等待进程退出（最多10秒）
for i in {1..10}; do
    if ! ps -p $PID > /dev/null 2>&1; then
        echo -e "${GREEN}✓ 爬虫已优雅停止${NC}"
        rm -f "$PID_FILE"
        exit 0
    fi
    echo "等待退出... ($i/10)"
    sleep 1
done

# 如果10秒后还未退出，强制终止
echo -e "${YELLOW}进程未在10秒内退出，发送SIGKILL信号（强制终止）${NC}"
kill -9 $PID

sleep 1

if ! ps -p $PID > /dev/null 2>&1; then
    echo -e "${GREEN}✓ 爬虫已强制停止${NC}"
    rm -f "$PID_FILE"
else
    echo -e "${RED}✗ 无法终止进程${NC}"
    exit 1
fi

echo "============================================================"

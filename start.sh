#!/bin/bash
# 东方财富股吧爬虫 - 后台启动脚本

# 颜色定义
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# 配置
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$SCRIPT_DIR/.crawler.pid"
LOG_DIR="$SCRIPT_DIR/logs"

# 创建logs目录
mkdir -p "$LOG_DIR"

# 生成带时间戳的日志文件名
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
MAIN_LOG="$LOG_DIR/crawler_${TIMESTAMP}.log"
ERROR_LOG="$LOG_DIR/error_${TIMESTAMP}.log"

echo "============================================================"
echo "东方财富股吧爬虫 - 后台启动"
echo "============================================================"

# 检查是否已经在运行
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if ps -p $OLD_PID > /dev/null 2>&1; then
        echo -e "${RED}✗ 爬虫已在运行（PID: $OLD_PID）${NC}"
        echo "如需停止，请运行: ./stop.sh"
        exit 1
    else
        echo -e "${YELLOW}⚠ 清理旧的PID文件${NC}"
        rm -f "$PID_FILE"
    fi
fi


# 启动爬虫（后台运行）
echo "启动爬虫..."
nohup python main.py > "$MAIN_LOG" 2> "$ERROR_LOG" &
PID=$!

# 保存PID
echo $PID > "$PID_FILE"

# 等待1秒检查是否成功启动
sleep 1

if ps -p $PID > /dev/null 2>&1; then
    echo -e "${GREEN}✓ 爬虫已启动${NC}"
    echo ""
    echo "PID: $PID"
    echo "PID文件: $PID_FILE"
    echo "主日志: $MAIN_LOG"
    echo "错误日志: $ERROR_LOG"
    echo ""
    echo "查看实时日志: tail -f $MAIN_LOG"
    echo "停止爬虫: ./stop.sh"
    echo "查看状态: ./status.sh"
else
    echo -e "${RED}✗ 启动失败${NC}"
    rm -f "$PID_FILE"
    echo "查看错误日志: cat $ERROR_LOG"
    exit 1
fi

echo "============================================================"

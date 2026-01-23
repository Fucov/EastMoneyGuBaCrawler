#!/bin/bash
# 东方财富股吧爬虫 - 状态查看脚本

# 颜色定义
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 配置
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$SCRIPT_DIR/.crawler.pid"
LOG_DIR="$SCRIPT_DIR/logs"

echo "============================================================"
echo "东方财富股吧爬虫 - 运行状态"
echo "============================================================"

# 1. 检查进程状态
echo -e "\n${BLUE}[进程状态]${NC}"
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p $PID > /dev/null 2>&1; then
        echo -e "${GREEN}✓ 运行中${NC}"
        echo "PID: $PID"
        
        # 显示进程信息
        echo ""
        ps -p $PID -o pid,comm,%cpu,%mem,etime,state
        
        # 显示运行时间
        UPTIME=$(ps -p $PID -o etime= | tr -d ' ')
        echo "运行时长: $UPTIME"
    else
        echo -e "${RED}✗ 已停止（PID文件存在但进程不存在）${NC}"
    fi
else
    echo -e "${RED}✗ 未运行${NC}"
fi

# 2. 检查日志文件
echo -e "\n${BLUE}[日志文件]${NC}"
if [ -d "$LOG_DIR" ]; then
    # 最新的主日志
    LATEST_MAIN_LOG=$(ls -t $LOG_DIR/crawler_*.log 2>/dev/null | head -1)
    if [ -n "$LATEST_MAIN_LOG" ]; then
        LOG_SIZE=$(du -h "$LATEST_MAIN_LOG" | cut -f1)
        echo "最新主日志: $(basename $LATEST_MAIN_LOG) ($LOG_SIZE)"
        echo "  路径: $LATEST_MAIN_LOG"
    fi
    
    # 最新的错误日志
    LATEST_ERROR_LOG=$(ls -t $LOG_DIR/error_*.log 2>/dev/null | head -1)
    if [ -n "$LATEST_ERROR_LOG" ]; then
        ERROR_SIZE=$(du -h "$LATEST_ERROR_LOG" | cut -f1)
        ERROR_LINES=$(wc -l < "$LATEST_ERROR_LOG" 2>/dev/null || echo "0")
        echo "最新错误日志: $(basename $LATEST_ERROR_LOG) ($ERROR_SIZE, $ERROR_LINES行)"
        echo "  路径: $LATEST_ERROR_LOG"
    fi
    
    # 调度器日志
    if [ -f "$LOG_DIR/scheduler.log" ]; then
        SCHEDULER_SIZE=$(du -h "$LOG_DIR/scheduler.log" | cut -f1)
        echo "调度器日志: scheduler.log ($SCHEDULER_SIZE)"
    fi
else
    echo "日志目录不存在"
fi

# 3. Redis代理池状态
echo -e "\n${BLUE}[代理池状态]${NC}"
if command -v redis-cli > /dev/null 2>&1; then
    if redis-cli ping > /dev/null 2>&1; then
        PROXY_COUNT=$(redis-cli HLEN guba:proxies:valid 2>/dev/null || echo "0")
        echo "Redis连接: ${GREEN}正常${NC}"
        echo "有效代理数: $PROXY_COUNT"
    else
        echo "Redis连接: ${RED}失败${NC}"
    fi
else
    echo "redis-cli未安装"
fi

# 4. MongoDB数据库状态
echo -e "\n${BLUE}[数据库状态]${NC}"
if command -v python > /dev/null 2>&1; then
    DOC_COUNT=$(python -c "
try:
    from database_client import DatabaseManager
    db = DatabaseManager('config/settings.ini')
    client = db.get_mongo_client('xiaoyi_db', 'stock_news')
    print(client.count_documents())
except:
    print('无法连接')
" 2>/dev/null)
    echo "文档数: $DOC_COUNT"
else
    echo "Python未找到"
fi

# 5. 最近日志（最后10行）
echo -e "\n${BLUE}[最近日志（最后10行）]${NC}"
if [ -n "$LATEST_MAIN_LOG" ]; then
    tail -10 "$LATEST_MAIN_LOG"
else
    echo "无日志文件"
fi

echo ""
echo "============================================================"
echo "操作命令:"
echo "  启动: ./start.sh"
echo "  停止: ./stop.sh"
echo "  查看实时日志: tail -f $LATEST_MAIN_LOG"
echo "============================================================"

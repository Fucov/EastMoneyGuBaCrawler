# 东方财富股吧爬虫 - 生产系统 v2.0

生产级24小时持续爬虫系统，自动爬取所有A股的资讯、研报、公告数据

## ✨ 特性

- **24小时持续运行** - 自动循环爬取，无需人工干预
- **智能IP管理** - Redis缓存代理，低于阈值自动补充
- **AkShare集成** - 自动获取所有A股代码（~5000只）
- **多线程并发** - 12线程并发，充分利用代理池
- **完整日志** - 按天切割，错误单独记录
- **数据持久化** - MongoDB存储，统一Collection

## 📁 项目结构

```
Guba-Crawler/
├── config/
│   └── settings.ini          # 统一配置文件
├── core/
│   ├── scheduler.py          # 24h调度器
│   ├── stock_loader.py       # AkShare股票加载
│   └── proxy_manager.py      # Redis代理管理
├── storage/
│   ├── logger.py             # 日志系统
│   └── database.py           # 数据库客户端
├── logs/                     # 日志目录
│   ├── crawler.log           # 爬虫日志
│   ├── scheduler.log         # 调度日志
│   └── error.log             # 错误日志
├── main.py                   # 主入口
├── proxy_pool.py             # 原代理池（兼容）
├── main_class.py             # 爬虫核心（待集成）
└── full_text_CrawlerAsync.py # 全文爬虫（保留）
```

## 🚀 快速开始

### 1. 环境准备

```bash
# 安装依赖
pip install pymongo redis akshare requests beautifulsoup4 lxml tqdm tenacity

# 启动Redis（无密码）
redis-server
```

### 2. 配置文件

编辑 `config/settings.ini`：

```ini
[MongoDB]
host = 10.139.197.213
database = xiaoyi_db

[Redis]
host = localhost
port = 6379
password =              # 本地无密码

[Proxy]
min_count = 5          # IP低于此值自动补充
target_count = 20      # 补充到此数量

[Crawler]
max_workers = 12       # 并发线程数

[Scheduler]
mode = continuous      # continuous=24h运行 | once=单次
interval = 1800        # 每轮间隔30分钟
stock_delay = 5        # 每只股票间隔5秒
```

### 3. 运行系统

```bash
# 前台运行（测试）
python main.py

# 后台运行（生产）
nohup python main.py > /dev/null 2>&1 &

# 查看日志
tail -f logs/scheduler.log
```

## 📊 核心模块

### 调度器 (scheduler.py)

24小时循环调度，智能管理IP池

```python
from core.scheduler import Crawler24HScheduler

scheduler = Crawler24HScheduler()
scheduler.run()
```

### 股票加载 (stock_loader.py)

自动从AkShare获取所有A股

```python
from core.stock_loader import StockLoader

loader = StockLoader()
stocks = loader.get_all_stocks()  # ['600519', '000001', ...]
```

### 代理管理 (proxy_manager.py)

Redis缓存代理，自动补充

```python
from core.proxy_manager import ProxyManager

manager = ProxyManager()
proxy = manager.get_random_proxy()  # {'http': '...', 'https': '...'}
```

### 日志系统 (logger.py)

持久化日志，按天切割

```python
from storage.logger import get_logger

logger = get_logger('my_module')
logger.info("正常信息")
logger.error("错误信息")
```

## 🔧 使用示例

### 测试单个模块

```bash
# 测试日志
python storage/logger.py

# 测试股票加载
python core/stock_loader.py

# 测试代理池（需要Redis）
python core/proxy_manager.py
```

### 查看运行状态

```bash
# 查看进程
ps aux | grep main.py

# 查看日志
tail -100 logs/scheduler.log

# 查看错误
cat logs/error.log

# 查看IP池
redis-cli HGETALL guba:proxies:valid
```

### 停止系统

```bash
# 查找进程ID
ps aux | grep main.py

# 优雅停止
kill -SIGINT <PID>

# 强制停止
kill -9 <PID>
```

## 📈 数据查询

### MongoDB查询

```python
from database_client import DatabaseManager

db = DatabaseManager('config/settings.ini')
client = db.get_mongo_client('xiaoyi_db', 'stock_news')

# 查看总数
print(f"总数: {client.count_documents()}")

# 查看某只股票
docs = client.find({"stock_code": "600519"})
for doc in docs:
    print(doc['title'])
```

### Redis查询

```bash
# 查看代理数量
redis-cli HLEN guba:proxies:valid

# 查看所有代理
redis-cli HGETALL guba:proxies:valid

# 查看得分最高的代理
redis-cli HSCAN guba:proxies:valid 0 MATCH * COUNT 100
```

## 🛡️ 生产环境部署

### 使用Systemd管理

创建 `/etc/systemd/system/guba-crawler.service`：

```ini
[Unit]
Description=Guba Crawler Service
After=network.target redis.service

[Service]
Type=simple
User=your_user
WorkingDirectory=/path/to/Guba-Crawler
ExecStart=/usr/bin/python3 main.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

启动服务：

```bash
sudo systemctl start guba-crawler
sudo systemctl enable guba-crawler
sudo systemctl status guba-crawler
```

### 日志轮转

日志已自动按天切割，保留30天

### 监控告警

可集成钉钉/企业微信webhook：

```python
# 在scheduler.py中添加
def send_alert(msg):
    import requests
    webhook = "your_webhook_url"
    requests.post(webhook, json={"text": msg})
```

## 🔍 故障排查

### 1. Redis连接失败

```bash
# 检查Redis是否运行
redis-cli ping

# 启动Redis
redis-server
```

### 2. MongoDB连接失败

```bash
# 检查配置
cat config/settings.ini | grep MongoDB

# 测试连接
python test_db_connection.py
```

### 3. IP池耗尽

```bash
# 手动补充代理
python core/proxy_manager.py

# 查看代理数量
redis-cli HLEN guba:proxies:valid
```

### 4. 查看错误日志

```bash
tail -50 logs/error.log
```

## 📝 版本历史

### v2.0 (2026-01-23)
- ✅ 重构为生产级架构
- ✅ 24小时持续调度
- ✅ Redis代理缓存
- ✅ AkShare集成
- ✅ 完整日志系统

### v1.0
- 基础多线程爬虫
- 文件代理缓存
- 手动股票列表

## 📄 许可证

MIT License

## 🤝 贡献

欢迎提交Issue和PR

---

**Made with ❤️ by Your Team**

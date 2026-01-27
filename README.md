# 东方财富股吧爬虫系统 (Guba Crawler)

这是一个生产级的高可用爬虫系统，专门用于抓取东方财富网股吧的**资讯、研报、公告**数据。系统设计目标是**24小时不间断运行**，具备自动反爬、智能IP池管理和数据完整性校验功能。

## 🏗 系统架构原理

### 1. 核心流程 (Core Logic)
系统采用 **"并发下载 + 顺序处理"** 的混合模式，既保证了速度，又解决了业务逻辑的时序依赖问题。

*   **并发下载 (ThreadPoolExecutor)**: 
    *   使用多线程 (`max_workers=12`) 同时请求多个分页（如同时请求第1-12页）。
    *   通过免费代理池或隧道代理自动切换IP，规避封锁。
*   **顺序处理 (Sequential Processing)**: 
    *   尽管下载是乱序完成的，但主线程严格按页码顺序（Page 1 -> Page 2 -> ...）处理数据。
*   **反爬虫校验**:
    *   自动提取页面源码中的 `count` 变量，与预期总数比对。如果返回的页面是反爬虫伪造的（内容无关但状态码200），系统会识别偏差并自动重试。

### 2. 智能代理池 (Smart Proxy Pool - Independent 24h Thread)
*   **独立线程架构**: 代理池维护逻辑被封装在独立的守护线程中 (`ProxyMaintenanceThread`)，不阻塞主爬虫流程。
*   **持久化运行**: 无论主爬虫正在处理什么任务，该线程都会24小时持续运行，确保Redis中始终有足够可用的代理。
*   **双模式支持**: 
    - **付费API模式**: 使用付费代理API (share.proxy.qg.net) 提供稳定的代理服务，每次提取5个代理，自动记录地区、运营商和过期时间。
    - **免费代理模式**: 从多个免费代理源（89ip、ProxyShare、GitHub等）抓取并验证代理。
*   **灵活切换**: 通过配置文件 `use_paid_api` 参数一键切换模式，适应不同使用场景。
*   **双重维护**: 默认每5分钟（可配置）检查一次代理池健康度。
*   **自动评分**: 每次爬虫请求都会反馈代理质量，成功+5分，失败-10分，低于30分自动剔除。
*   **智能补充**: 当可用代理数低于 `min_proxy_count` 时，根据配置模式自动补充代理（付费API支持多次调用直至达到目标数量）。

### 3. 数据持久化 (MongoDB)
*   **复合索引**: 使用 `(stock_code, content_type, url_id)` 建立唯一索引，天然去重。
*   **增量更新**: 每次爬取时，如果连续N页（默认2页）都是重复数据，自动跳过该股票的剩余页面，极大节省资源。

---

## 🚀 快速开始

### 1. 环境依赖
确保安装了 [Redis](https://redis.io/) 和 [MongoDB](https://www.mongodb.com/)。

```bash
pip install -r requirements.txt
# 或手动安装核心包
pip install requests beautifulsoup4 pymongo redis tenacity tqdm akshare
```

### 2. 配置文件 (`config.ini`)

**首次使用，请先复制示例配置：**
```bash
cp config.ini.example config.ini
```

然后编辑 `config.ini`，填入您的实际配置：

```ini
[MongoDB]
host = localhost  # 数据库地址
database = EastMoneyGubaNews

[PaidProxyAPI]
# 代理模式选择：true=使用付费API, false=使用免费代理源
use_paid_api = true
api_url = https://share.proxy.qg.net/get
api_key = YOUR_API_KEY_HERE  # 付费API密钥（仅在use_paid_api=true时需要）

[proxies]
use_free_proxy_pool = true  # 启用代理池
min_proxy_count = 5         # 代理池最小数量
target_count = 10           # 补充时的目标数量

[Scheduler]
mode = loop           # loop=死循环模式, once=单次模式
stock_delay = 2       # 每只股票间隔2秒
```

**代理模式说明**：
- **付费API模式** (`use_paid_api = true`): 使用稳定的付费代理服务，需要配置API Key，代理质量高但有成本
- **免费代理模式** (`use_paid_api = false`): 从多个免费代理源抓取，无需API Key，免费但可能不稳定

> **重要提示**: `config.ini` 包含敏感信息（数据库密码、API密钥等），已添加到 `.gitignore`，不会被提交到版本控制系统。请勿将此文件分享或上传到公开仓库。

### 3. 运行

#### 直接运行某只股票（测试用）
```bash
python core/crawler.py
```
*注意：这会读取config.ini中的 `secCode` 进行单只测试。*

#### 启动全量调度器（生产用）
```bash
python core/scheduler.py
```
*这将自动获取所有A股列表，开始循环爬取。*

### 日志说明
### 日志说明
- `logs/run.log`: **[主日志]** 记录所有爬虫运行详情，按天切割，保留7天。日常查看这个即可。
- `logs/err.log`: **[错误日志]** 仅记录程序崩溃、异常堆栈等严重错误。正常运行时应为空或很少内容。
- `logs/console.log`: **[控制台镜像]** 记录最近一次启动时的控制台输出，用于排查启动问题。
- `logs/startup_error.log`: **[启动报错]** 记录启动脚本因为环境问题导致的Python解释器报错（如ModuleNotFoundError）。

---

## ❓ 常见问题 (FAQ)

### Q: 为什么感觉速度没有把宽带跑满？
**A:** 系统设计上**故意限制了速度**。
1.  **顺序处理限制**: 为了保证年份推断正确，我们必须等第1页处理完才能处理第2页（即使第2页已经下载好了）。
2.  **写库瓶颈**: 这里的瓶颈通常在数据库写入（MongoDB远程连接）而非网络下载。
3.  **反爬考量**: 过快的并发会导致IP池极速耗尽，导致大量重试（Retry），反而降低整体有效吞吐量。目前的配置（12线程 + 2秒间隔）是稳定性和速度的最佳平衡点。

### Q: 代理池总是空的？
**A:** 根据您使用的代理模式进行排查：

**付费API模式** (`use_paid_api = true`):
1.  检查本地 Redis 是否启动 (`redis-cli ping`)。
2.  确认 `config.ini` 中 `[PaidProxyAPI]` 的 `api_key` 已正确配置。
3.  检查付费代理API账户余额是否充足。
4.  查看日志中是否有API调用错误信息（如 `REQUEST_LIMIT_EXCEEDED`、`BALANCE_INSUFFICIENT` 等）。

**免费代理模式** (`use_paid_api = false`):
1.  检查本地 Redis 是否启动 (`redis-cli ping`)。
2.  免费代理源可能暂时不可用，或您的本地IP被源站限制。
3.  查看日志确认哪些代理源可用，可以考虑修改 `core/proxy_manager.py` 中的 `free_proxy_sources` 列表，添加其他可用源。
4.  免费代理验证失败率较高属于正常现象，系统会持续尝试直到找到可用代理。

> **建议**: 生产环境推荐使用付费API模式以获得更稳定的服务质量。

---

## 📂 目录结构

*   `core/`
    *   `crawler.py`: 爬虫核心逻辑（单一职责：爬取、解析、入库）。
    *   `scheduler.py`: 调度器（获取股票列表、循环调度、IP池守护）。
    *   `proxy_manager.py`: 代理池实现（Redis交互）。
*   `config.ini`: 配置文件。

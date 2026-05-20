# Amazon Scraper v3

高性能分布式 Amazon 商品数据采集系统。Server/Worker 分离架构，支持百万级 ASIN 采集、变动检测、定时任务、截图存证、webhook 回调通知。

## 架构

```
Server (FastAPI, 1C/2GB 即可)        Worker (可部署多台)
  - Web 管理界面                       - curl_cffi TLS 指纹模拟
  - 任务分发 & 结果收集                 - AIMD 自适应并发控制
  - SQLite (WAL + FTS5)               - Session 热备轮换
  - 定时任务调度                        - Playwright 截图 (可选)
  - 全局并发配额协调                     - lease_epoch 防重复
  - Webhook 完成回调                    - variant_offset 检测
  - 锁竞争诊断仪表                       - 3 并行 batch submitter
```

## 快速开始

### 1. 环境要求

- Python 3.10+
- TPS 代理 (帐密认证，每次请求自动换 IP)
- 服务器最低 1C / 2GB / 20GB SSD

### 2. Server 部署

```bash
git clone https://github.com/ElijahRRR/amazon-scraper-v3.git
cd amazon-scraper-v3
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# 配置代理
echo 'PROXY_URL=http://user:pwd@host:port' > .env

# 启动
python3 run_server.py
```

Server 默认监听 `0.0.0.0:8899`，浏览器访问 `http://<IP>:8899`。

首次启动会自动建表 + 创建 FTS5 全文索引 + 完成数据库迁移。

### 3. Worker 启动

Worker 可以在本机或任意远程机器上运行，通过 HTTP 连接 Server。

```bash
# 基础启动（含截图）
python3 run_worker.py --server http://<SERVER_IP>:8899

# 禁用截图 + 自定义 worker_id
python3 run_worker.py --server http://<SERVER_IP>:8899 --worker-id my-worker --no-screenshot

# 定时自动重启（避免长跑内存泄漏）
python3 run_worker.py --server http://<SERVER_IP>:8899 --auto-restart-hours 6
```

| 参数 | 说明 |
|---|---|
| `--server` | Server 地址 (必填) |
| `--worker-id` | Worker 标识 (默认自动生成) |
| `--concurrency` | 初始并发数 (默认从 Server 同步) |
| `--zip-code` | 配送邮编 (默认从 Server 同步) |
| `--no-screenshot` | 禁用截图功能（只拉取非截图任务） |
| `--api-key` | ERP Server Worker API Key（也可用环境变量 `WORKER_API_KEY`） |
| `--auto-restart-hours` | 定时自动重启小时数（0 = 关闭） |

### 4. systemd 常驻服务 (Linux)

```ini
# /etc/systemd/system/amazon-scraper-v3.service
[Unit]
Description=Amazon Scraper v3 Server
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/amazon-scraper-v3
ExecStart=/opt/amazon-scraper-v3/venv/bin/python run_server.py
Restart=always
RestartSec=5
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
```

```bash
systemctl daemon-reload
systemctl enable --now amazon-scraper-v3.service
```

## 功能说明

### 任务上传

访问 **任务管理** 页面，上传包含 ASIN 的文件：

- 支持格式：`.xlsx` / `.csv` / `.txt`
- 自动提取 `B[0-9A-Z]{9}` 格式的 ASIN 并去重
- 可选：指定批次名、邮编、是否截图
- **per-ASIN 邮编**：xlsx 的 B 列填 5 位邮编可为单个 ASIN 指定邮编（同一 batch 内不同邮编自动切换 session）
- **API 调用**：可直接 `POST /api/upload`（multipart），支持以下额外字段：
  - `external_id`：调用方自己的批次 ID，原样回传，便于追踪
  - `callback_url`：采集完成时 POST 到此地址通知（详见下方 Webhook）

### 采集结果

访问 **采集结果** 页面：

- **批次筛选**：下拉选择特定批次
- **变动筛选**：全部 / 价格库存变动 / 标题描述变动 / 新增 ASIN
- **搜索**：支持 ASIN、标题、品牌模糊搜索，多个关键词用换行或逗号分隔
  - 走 SQLite FTS5 trigram 索引，**百万级数据下也只要 5-50ms**（比 LIKE 全表扫快 ~1000 倍）
  - 短查询（<3 字符）自动 fallback 到 LIKE 路径保证正确性
- **选中删除**：勾选行 checkbox，点击"删除选中"（同时删除关联截图文件）
- **清空数据**：根据当前筛选条件智能删除
  - 选了批次 → 只删该批次数据
  - 输了搜索词 → 只删匹配数据
  - 无筛选 → 清空全部数据和截图

### 导出

点击 **导出** 按钮，弹窗选择：

- 格式：Excel (.xlsx) / CSV (.csv)
- 字段：全选 / 仅价格库存 / 自定义勾选
- 范围：当前选中的批次 + 变动筛选条件
- 支持流式导出，百万级数据不 OOM
- 导出列：ASIN → 链接 → 标题 → 品牌 → 评分 → 评论数 → 卖家店铺 → 价格 → 库存 → 配送 → 描述 → 类目 → 尺寸 → 制造商 → 排名 → 站点 → 时间

### 定时自动采集

在 **系统设置** 页面的"定时自动采集"区域：

1. 点击 **新建任务**
2. 填写：
   - **任务名称**：如"每日核心商品监控"
   - **执行时间**：时:分
   - **执行间隔**：天数（输入数字，1=每天，2=每两天，7=每周...）
   - **ASIN 文件**：上传 xlsx/csv/txt（留空则使用主库全部 ASIN，主库增加时自动覆盖）
   - **需要截图**：是否截图存证
3. 创建后自动启用，到达时间点自动创建批次并开始采集
4. 支持手动 **立即执行**（播放按钮）
5. 支持 **启用/禁用** 切换和 **删除**

### Webhook 完成回调

上传任务时指定 `callback_url`，批次跑完后 server 自动 POST：

```json
{
  "event_id": "<batch_id>:<completed_at_iso>",
  "batch_name": "...",
  "external_id": "<调用方传入>",
  "status": "completed",
  "stats": {
    "total": 100,
    "done": 98,
    "failed": 2,
    "skipped_no_retry": 0
  },
  "completed_at": "2026-05-20T07:42:11Z"
}
```

特性：
- **SSRF 防御**：private IP / localhost / 非 http(s) 协议自动拒绝
- **失败重试**：30s → 5min → 30min → 2h → 终态（5 次后放弃）
- **手动重试**：`POST /api/batches/{name}/callback/retry`
- **状态查询**：`GET /api/batches/{name}/status`

### Worker 监控

访问 **Worker 监控** 页面：

- 全局并发/QPS 预算分配
- 每个 Worker 的实时指标：
  - 成功率、封锁率、延迟 p50
  - 在飞请求、本地排队、待提交
  - 采集速度、已接受、已过期（stale）
- 软重启：重建 Session（新指纹+新 Cookie），采集不中断
- 清理离线 Worker
- Dashboard "已分发"显示 Server 端 processing 总数，"活跃采集"显示实际 HTTP 在飞请求数

### 错误详情查看

任务管理页失败批次 ❗ 按钮，弹出错误明细：

- 错误类型按数量倒序展示，含占比和颜色标识
- 11 种 error_type 全部中文化（被封锁 / 验证码 / 超时 / 网络异常 / 解析失败 / Variant 偏移 / ...）
- 长 error_detail 完整展示（不再截断）
- 表格附带 Worker / 时间列，便于定位
- 点击 ASIN 直达亚马逊页面（含 `?th=1&psc=1` 强制变体）

### 系统设置

所有设置保存后 Worker 在 30 秒内自动同步，无需重启。

| 分类 | 主要参数 |
|---|---|
| 基础 | 邮编、重试次数、请求超时、Session 轮换频率 |
| 代理 | TPS 代理地址 |
| 速率 | 全局总并发/QPS 上限、单 Worker QPS、初始/最大/最小并发 |
| AIMD | 评估间隔、目标延迟、延迟上限、封锁冷却、成功率阈值 |
| 重试 | 自动重试开关、最大轮数、失败延迟 |

## 目录结构

```
amazon-scraper-v3/
  common/
    config.py          # 共享配置 (MAX_RETRIES, TASK_TIMEOUT_MINUTES, ...)
    database.py        # SQLite (WAL + FTS5 + lease_epoch + 重试机制)
    models.py          # 数据类定义
  server/
    app.py             # FastAPI 服务端 (50+ API + 后台协程)
    templates/         # Jinja2 页面模板
    static/            # 静态资源 + 截图存储
  worker/
    engine.py          # 采集引擎 (流水线 + 3 并行 submitter)
    session.py         # curl_cffi Session + variant 检测
    parser.py          # Amazon 页面解析器 (含 page_asin 提取)
    proxy.py           # TPS 代理管理
    adaptive.py        # 自适应并发控制器
    metrics.py         # 性能指标收集
    screenshot.py      # Playwright 截图子进程
  data/
    scraper.db         # SQLite 数据库文件
    scraper.db-wal     # WAL 日志（journal_size_limit=64MB）
    scraper.db-shm     # WAL 共享内存
    exports/           # 导出文件 + 临时文件（自动清理）
    schedules/         # 定时任务 ASIN 文件
  deploy/
    setup.sh           # 部署脚本
    server.service     # systemd 服务配置
  run_server.py        # Server 启动入口
  run_worker.py        # Worker 启动入口
  .env                 # 代理地址等敏感配置
```

## 数据库表

| 表 | 说明 |
|---|---|
| `batches` | 批次元数据 + callback 状态 + external_id |
| `batch_asins` | 批次-ASIN 多对多映射 |
| `asin_data` | ASIN 数据 (UNIQUE，覆盖更新) |
| `asin_data_fts` | FTS5 trigram 全文索引（external content） |
| `asin_changes` | 变动检测历史 (价格/库存/标题/新增) |
| `tasks` | 采集任务队列 (含 lease_epoch + auto_retry_count) |
| `screenshots` | 截图追踪 |

`asin_data` 包含字段：ASIN / 标题 / 品牌 / 价格 / 库存 / **评分 / 评论数 / 卖家店铺 ID / 卖家名** / 父 ASIN / 变体 / 类目 / 尺寸 / 重量 / 制造商 / 排名 / ...

## 核心机制

### 任务分发防重复 (lease_epoch)

多 Worker 并发采集的核心难题是任务重复分发。v3 通过 lease_epoch 机制解决：

- 每个任务有 `lease_epoch` 计数器（初始 0）
- 任务被回收重新入队时 `lease_epoch += 1`（所有回队路径：回收/失败重试/归还）
- Worker 提交结果时携带 `lease_epoch`，Server 原子校验：`WHERE task_id=? AND worker_id=? AND lease_epoch=? AND status='processing'`
- 校验通过才写入 `asin_data`，不通过返回 `stale=true`（迟到结果被丢弃）
- 结果写入和任务完成在同一事务内，不会出现半写状态

### 4 层重试架构

```
┌────────────────────────────────────────────────────────────────┐
│ 层 1：Worker 本地 retry 循环（MAX_RETRIES=3）                   │
│   出错 → rotate session → 再请求                                │
├────────────────────────────────────────────────────────────────┤
│ 层 2：Server fail_task / accept_results_batch                  │
│   收 success=False → retry_count++                              │
│   if retry_count >= cap[error_type]: status='failed'           │
│   else: status='pending'（重新入队）                            │
├────────────────────────────────────────────────────────────────┤
│ 层 3：auto_retry_failed_tasks（每 30s 周期任务）                │
│   扫描 status='failed' AND auto_retry_count < 2 AND 失败>5min  │
│   重置为 pending，再走 2 轮（每轮 = 层 1+2 完整循环）          │
├────────────────────────────────────────────────────────────────┤
│ 层 4：reclaim_dead_worker_tasks（每 30s 周期任务）              │
│   扫描 status='processing' AND（心跳超 2min 或 任务超 10min）  │
│   回收为 pending、lease_epoch++（迟到结果失效）                 │
└────────────────────────────────────────────────────────────────┘
```

正常错误（network / timeout / blocked / captcha / ...）最大尝试 = `MAX_RETRIES × MAX_RETRIES × (1 + auto_retry_cycles) = 3 × 3 × 3 = 27 次`。

### 按 error_type 分级的不重试策略

部分 error_type 是稳定的产品/页面层事实，重试无意义：

```python
# common/database.py
LIMITED_RETRY_ERROR_TYPES = {
    "variant_offset": 2,   # 给 1 次重试机会（cap=2）
}
NO_AUTO_RETRY_ERROR_TYPES = frozenset({"variant_offset"})
```

| 错误类型 | layer 2 cap | layer 3 自动重试 | layer 4 手动重试 |
|---|---|---|---|
| `network` / `timeout` / `parse_error` / `blocked` / `captcha` | 3 | ✓ | ✓ |
| `variant_offset` | **2**（给 1 次重试机会） | ✗ 跳过 | ⚠ 默认跳过，Shift+点击强制 |

要加新类型只需改 `LIMITED_RETRY_ERROR_TYPES` 和 `NO_AUTO_RETRY_ERROR_TYPES`，全链路自动跟上。

### Variant 偏移检测（防止数据中毒）

多属性产品（如同 parent 下的 2-100 个变体）请求 `/dp/B0XXX?th=1&psc=1` 时，Amazon 偶发返回另一个 variant 的页面（A/B test / 库存 / 缓存）。若 parser 不校验，会把错 variant 的 title 写到 B0XXX 这一行，造成数据中毒。

**parser 层防御**（worker/parser.py `_extract_page_asin`）：
- 提取 `<input id="ASIN" value="...">` 隐藏字段
- 提取 `<link rel="canonical" href=".../dp/ASIN">`
- 提取 JS 中 `"currentAsin":"..."`
- 任一信号 ≠ 请求 ASIN → 标记 `error_type='variant_offset'`，**绝不写入主表**

**worker 处理策略**：
- 不本地重试，不 rotate session（避免打爆隧道 5 QPS）
- 直接上报失败，让 server 调度其他 worker / 其他时间重试

### SQLite 性能优化

`common/database.py` 在 `connect()` 应用了一组 PRAGMA：

```python
PRAGMA journal_mode=WAL              # WAL 模式（并发读写）
PRAGMA synchronous=NORMAL            # 写延迟 -30~50%（WAL 下安全）
PRAGMA cache_size=-65536             # 64MB page cache
PRAGMA mmap_size=268435456           # 256MB mmap
PRAGMA temp_store=MEMORY             # 临时表入内存
PRAGMA wal_autocheckpoint=1000       # 自动 checkpoint
PRAGMA journal_size_limit=67108864   # WAL 上限 64MB
PRAGMA optimize=0x10002              # 启动刷新优化器统计
```

**按需 TRUNCATE checkpoint**（`_timeout_task_loop`）：
- 默认 PASSIVE（不阻塞 writer）
- 仅当 WAL > 128MB 时主动 TRUNCATE
- 消除固定周期 TRUNCATE 引起的 commit 抖动（实测 max hold 407ms → 231ms）

### FTS5 全文搜索

`asin_data_fts` 虚拟表（trigram tokenizer + external content + detail=none）：

- 配套 3 个触发器（AI / AD / AU）自动同步主表变化
- 搜索查询走 `UNION` 形态让每个 LIKE 都命中 trigram L1 索引
- 短查询（<3 字符）fallback 到主表 LIKE
- 实测：原 LIKE 全表扫描 46 秒 → 新 FTS UNION 5-50 毫秒（**~1000× 加速**）

### 锁竞争 / 阶段耗时 诊断仪表

`/api/_debug/lock-stats`（GET）返回每个 caller 的 wait/hold 时长分布：

```json
{
  "waits": {
    "pull_tasks":           {"count": 4943, "p50": 0.0, "p95": 0.85, "p99": 35.76, "max": 355.02},
    "accept_results_batch": {"count": 3238, "p50": 0.0, "p95": 12.49, "p99": 92.22, "max": 364.02}
  },
  "holds": {
    "pull_tasks":           {"count": 4943, "p50": 0.28, "p95": 5.83, "p99": 39.41, "max": 458.13},
    "accept_results_batch": {"count": 3238, "p50": 7.5, "p95": 44.06, "p99": 93.81, "max": 407.5}
  },
  "stage_timings": {
    "update_tasks_lease":   {"p50": 0.03, "p95": 0.11, "max": 38.16},
    "save_result":          {"p50": 0.45, "p95": 1.69, "max": 52.95},
    "commit":               {"p50": 0.19, "p95": 27.34, "max": 226.10}
  },
  "slow_holds_recent": [[ts, caller, ms], ...]
}
```

`POST /api/_debug/lock-stats/reset` 重置计数器，便于开启新一轮观察。

### 心跳感知任务回收

- **主机制**：后台 30s 循环检查，只回收死 Worker（无心跳 2 分钟+）的 processing 任务
- **硬超时兜底**：10 分钟（liveness safety net），防止任务永久占位
- 回收不在 `pull_tasks()` 中执行，避免每次拉取都触发误回收
- 有了 lease_epoch，即使硬超时误触发也不会写脏数据，只浪费少量代理资源

### 双口径统计

Worker 维护两组指标：

| 指标 | 计数时机 | 含义 |
|---|---|---|
| `success` / `failed` | 采集完成时 | 本地采集结果（代理+Amazon 层面） |
| `accepted` / `stale` | Server 响应后 | 服务端实际录入（`success - accepted = 重复采集量`） |

### TPS 代理模式
每次 HTTP 请求通过代理自动获取不同出口 IP，无需通道管理。代理地址格式：`http://user:pwd@host:port`

### AIMD 自适应并发
- Additive Increase：成功率高 + 延迟低 → 并发 +2
- Multiplicative Decrease：被封/超时 → 并发 x0.7 + 冷却
- Gradient2：RTT 上升趋势 → 预防性 -1
- 带宽感知：饱和时停止增长

### Session 轮换
- **热备 Session**：后台预热备用 Session，轮换瞬间切换 (<0.5s)
- **主动轮换**：每 1000 次成功请求更换
- **被动轮换**：被封/CAPTCHA/空标题时触发
- **Burst 缓解**：旧 Session 延迟 5s 关闭，轮换后 3s 宽限期
- **variant_offset 不轮换**：避免打爆隧道 QPS

### 全局并发协调
Server 根据 Worker 健康度加权分配并发和 QPS 配额，防止多 Worker 总 QPS 超出代理承载。

### 变动检测
采集结果入库时自动与 baseline（上次定时采集）对比，生成变动记录：
- `price_stock`：价格或库存变化
- `title_bullets`：标题或五点描述变化
- `new`：首次采集的新 ASIN

baseline 仅在 `is_auto=True` 的定时批次更新，手动批次不影响检测基准。

### Worker 写路径优化

```
playwright 抓取 → _result_queue (maxsize=500)
                       ↓
                 3 个并行 _batch_submitter（共用 queue）
                       ↓
                 POST /api/tasks/result/batch
                       ↓
                 server accept_results_batch
```

- **3 个并行 submitter**：单个 submitter 在 HTTP retry 时不阻塞其他
- **HTTP timeout**: 8s（原 15s），retry backoff: 0.5/1/2 秒（原 1/2/4 秒）
- **fallback 改并发**：单条 fallback 用 `asyncio.gather` 并发提交（10×10s → ~10s）
- **反压**：`_result_queue` 500 上限，提交慢时自动减速采集

## 调试与诊断

### 锁竞争快速诊断

```bash
# 重置统计
curl -X POST http://<SERVER>:8899/api/_debug/lock-stats/reset

# 跑一段采集后查看
curl http://<SERVER>:8899/api/_debug/lock-stats | python3 -m json.tool

# 关注：
# - pull_tasks.waits.p99 高 → 锁竞争严重
# - accept_results_batch.holds.max 大 → commit 抖动
# - stage_timings.commit 大 → SSD/fsync 问题
```

### 手动重试失败任务

UI：任务管理页点击"重试"按钮
- 默认跳过 `variant_offset` 类型
- 按住 Shift 点击 → 强制重试所有（含 variant_offset）

API：
```bash
# 默认跳过 NO_AUTO_RETRY_ERROR_TYPES
curl -X POST http://<SERVER>:8899/api/batches/<batch_name>/retry

# 强制重试所有失败
curl -X POST 'http://<SERVER>:8899/api/batches/<batch_name>/retry?force=true'
```

返回：
```json
{
  "ok": true,
  "retried": 1230,
  "skipped_no_retry": 45,
  "no_retry_types": ["variant_offset"],
  "forced": false
}
```

### 手动复采单个任务

```sql
-- 重置 task 为 pending（用 ASIN 或 task_id 定位）
UPDATE tasks SET status='pending', error_type=NULL, error_detail=NULL,
                 retry_count=0, auto_retry_count=0
WHERE asin = 'B0XXXXXXXX';
```

### Webhook 回调状态查询

```bash
curl http://<SERVER>:8899/api/batches/<batch_name>/status | python3 -m json.tool
```

返回完整状态：任务进度 / 截图进度 / callback 状态 / external_id / 重试次数 / 下次重试时间。

## 性能基线

实测数据（DMIT VPS 1C/2GB + 10 worker，2026-05）：

| 指标 | 数值 |
|---|---|
| 单 worker 采集速率 | 5-8 ASIN/s |
| 全局采集峰值 | 3000-5000 ASIN/min（60-83 ASIN/s）|
| Server `accept_results_batch` 持锁 p50 | 7.5 ms |
| Server `accept_results_batch` 持锁 p99 | 71-94 ms |
| Server `pull_tasks` 持锁 p50 | 0.28 ms |
| 30k ASIN 跑完总时长 | ~9 分钟 |
| `/api/results` 搜索（10 万行）| 5-50 ms |
| `/api/batches` 仪表盘加载 | 35-100 ms |
| 数据库主表 | ~2.4 GB / 10 万 ASIN |
| FTS5 索引开销 | ~20 MB / 10 万 ASIN |

## License

Private use.

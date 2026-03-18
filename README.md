# Amazon Scraper v3

高性能分布式 Amazon 商品数据采集系统。Server/Worker 分离架构，支持百万级 ASIN 采集、变动检测、定时任务、截图存证。

## 架构

```
Server (FastAPI, 1C/2GB 即可)       Worker (可部署多台)
  - Web 管理界面                      - curl_cffi TLS 指纹模拟
  - 任务分发 & 结果收集                - AIMD 自适应并发控制
  - SQLite 数据存储                   - Session 热备轮换
  - 定时任务调度                       - Playwright 截图 (可选)
  - 全局并发配额协调                    - lease_epoch 防重复
```

## 快速开始

### 1. 环境要求

- Python 3.10+
- TPS 代理 (帐密认证，每次请求自动换 IP)

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

### 3. Worker 启动

Worker 可以在本机或任意远程机器上运行，通过 HTTP 连接 Server。

```bash
# 基础启动（含截图）
python3 run_worker.py --server http://<SERVER_IP>:8899

# 禁用截图
python3 run_worker.py --server http://<SERVER_IP>:8899 --worker-id my-worker --no-screenshot
```

| 参数 | 说明 |
|---|---|
| `--server` | Server 地址 (必填) |
| `--worker-id` | Worker 标识 (默认自动生成) |
| `--concurrency` | 初始并发数 (默认从 Server 同步) |
| `--zip-code` | 配送邮编 (默认从 Server 同步) |
| `--no-screenshot` | 禁用截图功能（只拉取非截图任务） |

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

### 采集结果

访问 **采集结果** 页面：

- **批次筛选**：下拉选择特定批次
- **变动筛选**：全部 / 价格库存变动 / 标题描述变动 / 新增 ASIN
- **搜索**：支持 ASIN、标题、品牌模糊搜索，多个关键词用换行或逗号分隔
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
- 导出列顺序：ASIN → 链接 → 标题 → 品牌 → 价格 → 库存 → 配送 → 描述 → 类目 → 尺寸 → 制造商 → 排名 → 站点 → 时间

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

### 系统设置

所有设置保存后 Worker 在 30 秒内自动同步，无需重启。

| 分类 | 主要参数 |
|---|---|
| 基础 | 邮编、重试次数、请求超时、Session 轮换频率 |
| 代理 | TPS 代理地址 |
| 速率 | 全局总并发/QPS 上限、单 Worker QPS、初始/最大/最小并发 |
| AIMD | 评估间隔、目标延迟、延迟上限、封锁冷却、成功率阈值 |

## 目录结构

```
amazon-scraper-v3/
  common/
    config.py          # 共享配置
    database.py        # SQLite 数据库 (6 表 + lease_epoch)
  server/
    app.py             # FastAPI 服务端 (40+ API)
    templates/          # Jinja2 页面模板
    static/             # 静态资源 + 截图存储
  worker/
    engine.py          # 采集引擎 (流水线 + AIMD)
    session.py         # curl_cffi Session 管理
    parser.py          # Amazon 页面解析器
    proxy.py           # TPS 代理管理
    adaptive.py        # 自适应并发控制器
    metrics.py         # 性能指标收集
    screenshot.py      # Playwright 截图子进程
  data/
    scraper.db         # SQLite 数据库文件
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
| `batches` | 批次元数据 |
| `batch_asins` | 批次-ASIN 多对多映射 |
| `asin_data` | ASIN 数据 (UNIQUE，覆盖更新) |
| `asin_changes` | 变动检测历史 (价格/库存/标题/新增) |
| `tasks` | 采集任务队列 (含 lease_epoch 防重复) |
| `screenshots` | 截图追踪 |

## 核心机制

### 任务分发防重复 (lease_epoch)

多 Worker 并发采集的核心难题是任务重复分发。v3 通过 lease_epoch 机制解决：

- 每个任务有 `lease_epoch` 计数器（初始 0）
- 任务被回收重新入队时 `lease_epoch += 1`（所有回队路径：回收/失败重试/归还）
- Worker 提交结果时携带 `lease_epoch`，Server 原子校验：`WHERE task_id=? AND worker_id=? AND lease_epoch=? AND status='processing'`
- 校验通过才写入 `asin_data`，不通过返回 `stale=true`（迟到结果被丢弃）
- 结果写入和任务完成在同一事务内（`accept_success_result`），不会出现半写状态

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
- 热备 Session：后台预热备用 Session，轮换瞬间切换 (<0.5s)
- 主动轮换：每 1000 次成功请求更换
- 被动轮换：被封/CAPTCHA/空标题时触发（机制 1：独立触发 + 机制 2：累计 15 次空标题触发）
- Burst 缓解：旧 Session 延迟 5s 关闭，轮换后 3s 宽限期

### 全局并发协调
Server 根据 Worker 健康度加权分配并发和 QPS 配额，防止多 Worker 总 QPS 超出代理承载。

### 变动检测
采集结果入库时自动与上一次数据对比，生成变动记录：
- `price_stock`：价格或库存变化
- `title_bullets`：标题或五点描述变化
- `new`：首次采集的新 ASIN

### 反压机制
- `_result_queue` 有 500 上限：提交卡住时自动减速采集
- `fetch_count = 当前并发 × 1`：减少本地排队任务数，缩短暴露窗口
- Dashboard 区分"已分发"（Server processing）和"活跃采集"（Worker inflight），避免指标误导

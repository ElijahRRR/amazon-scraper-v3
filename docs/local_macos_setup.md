# macOS 本机跑起来 —— 最短路径

> 目录布局**不动**，还是现在这套（数据在 `{项目}/data`，截图在
> `server/static/screenshots`）。这份只管「在 Mac 上把它跑起来并验功能」。
>
> 服务器上那套部署不受影响：不设 `DB_BACKEND` 就还是 SQLite，行为逐字节不变。

---

## 0. 先切到有这些改动的分支（**跳过这步后面每一条命令都会 file not found**）

本次迁移的**全部产物**只在 `claude/walmart-api-db-refactor-7oergd` 上，
`main` 上没有 `requirements-dev.txt`、没有 `tools/`、没有 `docs/`、
没有 `common/pgdb/`、没有 `tests/golden/`。

```bash
cd ~/你的路径/amazon-scraper-v3
git fetch origin
git checkout claude/walmart-api-db-refactor-7oergd

# 确认（三个都要在）
ls requirements-dev.txt tools/phase5_preflight.py docs/local_macos_setup.md
```

---

## 1. PostgreSQL

```bash
brew install postgresql@16
brew services start postgresql@16

# Apple Silicon；Intel 机把 /opt/homebrew 换成 /usr/local
echo 'export PATH="/opt/homebrew/opt/postgresql@16/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
psql --version        # 应显示 16.x
```

### 建库（**`LC_COLLATE=C` 这一步别省**）

```bash
createuser -s scraper
createdb -O scraper -T template0 --lc-collate=C --lc-ctype=C -E UTF8 scraper
psql -d scraper -c "CREATE EXTENSION IF NOT EXISTS pg_trgm;"
psql -d scraper -c "ALTER DATABASE scraper SET default_toast_compression = 'lz4';"
```

**为什么必须 `LC_COLLATE=C`**：PG 默认排序规则（macOS 上通常是 `en_US.UTF-8`）
和 SQLite 的 `BINARY` 不同，`TEXT` 列的 `ORDER BY` 会给出不同顺序 —— 分页、导出、
搜索的结果都会跟旧系统对不上。**这个参数只能建库时定，事后改要重建库。**
`-T template0` 是必需的，否则不允许指定与模板不同的排序规则。

Homebrew 装的 PG 默认没有密码、走本机 socket，所以 DSN 里的密码随便写：

```bash
export PG_DSN="postgresql://scraper@127.0.0.1:5432/scraper"
```

---

## 2. Python 依赖

**用 3.12，别用 3.13/3.14。** 代码本身要 ≥ 3.10，但这次迁移的所有验证
（golden 64 步、pytest 682 条）都是在 **3.11** 上跑的；而依赖里有四个是要编 C 扩展的
——`curl_cffi`、`lxml`、`selectolax`、`asyncpg`——太新的解释器往往还没有预编译 wheel，
pip 会掉进源码编译，缺 `libxml2` / Rust 工具链就直接失败。3.12 是离验证环境最近
又肯定有 wheel 的版本。

`python3` 指向哪个版本取决于 PATH，所以下面**显式写全路径**建 venv：

```bash
brew install python@3.12

cd ~/你的路径/amazon-scraper-v3
rm -rf .venv                  # 如果之前用别的版本建过，先删掉
/opt/homebrew/opt/python@3.12/bin/python3.12 -m venv .venv
.venv/bin/python -V           # 应显示 3.12.x
.venv/bin/pip install -U pip
.venv/bin/pip install -r requirements-dev.txt   # 含 server + worker + 测试
.venv/bin/pip install asyncpg

# 只有要测截图功能才需要
.venv/bin/playwright install chromium
```

`requirements-dev.txt` 已经包含 `selectolax` / `lxml` / `dateparser` ——
**别省这三个**：`selectolax` 是生产解析引擎，不装它 worker 会走 lxml 回退路径，
采出来的数据和预期不一样，而且解析器的 24 个回归用例会有 23 个 skip。

---

## 3. 环境变量

```bash
export DB_BACKEND=postgres
export PG_DSN="postgresql://scraper@127.0.0.1:5432/scraper"
export SCRAPER_INSTANCE_ID=local-mac          # 不配则两个部署无法区分
export EXPORT_TOKEN=$(openssl rand -hex 16)   # 本机测试可以不配，见下
export SERVER_PORT=8899
```

`EXPORT_TOKEN` 不配也能跑（契约里鉴权是可选的），端点会放行并每次打 WARNING。
本机测试无所谓；**上公网前必须配**。

---

## 4. 上机体检（先跑这个，不过就别往下走）

```bash
.venv/bin/python tools/phase5_preflight.py
```

实测而非读配置：PG 版本/扩展/编码/**排序规则**、能否建分区表、advisory lock、
增量导出的路由顺序、依赖、磁盘、CPU/内存。

硬失败必须修；警告逐条确认是有意的。特别看「排序规则」那行——
不是 `C` 的话回到第 1 步重建库。

---

## 5. 跑测试（不需要 worker，不需要代理）

```bash
# 两个后端都要绿
.venv/bin/python -m tests.golden.run verify
DB_BACKEND=postgres .venv/bin/python -m tests.golden.run verify

.venv/bin/python -m pytest tests/ -q
DB_BACKEND=postgres .venv/bin/python -m pytest tests/ -q
```

预期：golden 各 **64/64**；pytest 约 **661 / 682 passed**。

---

## 6. 起服务

```bash
DB_BACKEND=postgres .venv/bin/python run_server.py
```

首次启动会建 schema、表、索引、分区。**看日志确认没有 DDL 报错。**
然后开 http://localhost:8899 ——仪表盘、任务、结果、Worker、设置五个页面。

### 冒烟（不需要真 worker，用 HTTP 模拟）

```bash
# 1) 事件流健康
curl -s localhost:8899/api/_debug/event-stream | python3 -m json.tool
#    relay_state 应为 running，outbox_depth 不应单调增长

# 2) 上传一个小批次
printf 'B0CXXXXXX1\nB0CXXXXXX2\n' > /tmp/asins.txt
curl -s -F "file=@/tmp/asins.txt" -F "batch_name=smoke" -F "zip_code=10001" \
     localhost:8899/api/upload | python3 -m json.tool

# 3) 模拟 worker 拉任务
curl -s "localhost:8899/api/tasks/pull?worker_id=w1&count=10" | python3 -m json.tool

# 4) 用上一步返回的 id / lease_epoch / batch_id 提交一条结果
curl -s -X POST localhost:8899/api/tasks/result \
  -H 'Content-Type: application/json' \
  -d '{"task_id":1,"batch_id":1,"worker_id":"w1","lease_epoch":0,"success":true,
       "asin":"B0CXXXXXX1","title":"Smoke Test","brand":"SmokeBrand",
       "category_tree":"Home > Tools","current_price":"9.99",
       "stock_status":"In Stock","stock_count":"3",
       "crawl_time":"2026-08-06T10:00:00Z","site":"US","zip_code":"10001"}'

# 5) 看结果进没进去
curl -s "localhost:8899/api/results?limit=5" | python3 -m json.tool

# 6) 等一两秒让 relay 抽干，然后验增量导出（这是新东西）
curl -s -H "X-Export-Token: $EXPORT_TOKEN" \
     "localhost:8899/api/export/incremental?cursor=0&limit=10" | python3 -m json.tool
```

第 6 步要看的：

- `records[0].source_id` 有值、`cursor` 是正整数
- `scraped_at` 形如 `2026-08-06T10:00:00Z`（**带 Z、精确到秒**）
- `slow` / `fast` 两个对象都在，`marketplace` 是 `"US"`
- 再打一次同样的 `cursor=0`，结果应**完全一致**（重复返回无害）
- 拿 `next_cursor` 再打一次，应返回空页 + **200**（不是 404）且 `next_cursor` 不推进

---

## 7. 要真采集的话

需要代理（`PROXY_URL`）和 worker：

```bash
export PROXY_URL="http://user:pwd@host:port"
.venv/bin/python run_worker.py --worker-id w-mac-01
```

worker 跑起来后回到第 6 步，这次 `parse_engine` 应该是 `selectolax`，
`zip_verify` 不再恒 `unverified`。

---

## 常见问题

| 症状 | 原因 |
|---|---|
| `Could not open requirements file: 'requirements-dev.txt'` | 还在 `main` 上。回到第 0 步切分支 |
| `can't open file '.../tools/phase5_preflight.py'` | 同上 |
| `pip install` 卡在编译 `lxml`/`curl_cffi` 然后报错 | venv 的 Python 太新（3.13/3.14）没有 wheel。按第 2 步用 3.12 重建 |
| `ConnectionRefusedError: 5432` | PG 没起。`brew services start postgresql@16` |
| `role "scraper" does not exist` | 第 1 步的 `createuser` 没跑 |
| `/api/export/incremental` 返回 404「批次不存在」 | 路由顺序坏了。跑 preflight 会直接指出来 |
| 增量导出返回 503 `event_stream_unavailable` | `DB_BACKEND` 不是 `postgres`，或事件流表没建 |
| 解析器测试大面积 skip | `selectolax` 没装 |
| 排序/分页结果和旧系统对不上 | 建库时没加 `LC_COLLATE=C`，要重建库 |

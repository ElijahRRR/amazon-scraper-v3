# Phase 5 运行手册 —— 并行验证 + 切换

> 前置：Phases 0-4、6 已完成（代码在 PR #9）。
> 相关：`.agent/MIGRATION_STATUS.md`（残留差异完整清单）、
> `docs/incremental_export_contract.md`（交付沃尔玛侧的契约）。

**Phase 5 是整条迁移里唯一还没有任何证据覆盖的环节。**
前面所有验证都在沙箱里完成——从没跑过真实采集、从没碰过生产库、目标机器还不存在。
黄金样本证明的是**存储层语义等价**（它喂合成字典、从不 import `worker.parser`、
从不真的去抓亚马逊），**证明不了**「新系统采出来的数据和旧系统一样」。
那正是这一阶段要回答的问题。

---

## 0. 三十秒版

```bash
# S1 目标机器体检（不过就别往下走）
python tools/phase5_preflight.py

# S2 起新系统（SQLite 后端），确认基本功能
DB_BACKEND=sqlite python run_server.py

# S3 切 PG 后端，建库建表
DB_BACKEND=postgres PG_DSN=... python run_server.py

# S4 两套系统各采同一批 ASIN（各自的 worker），然后比对
python tools/phase5_compare.py --old http://旧:8899 --new http://新:8899 --limit 500

# S5 事件流对账 + 契约端到端
curl "http://新:8899/api/v1/sync/counts?from_seq=0&to_seq=1000000"
curl -H "X-Export-Token: $EXPORT_TOKEN" "http://新:8899/api/export/incremental?cursor=0&limit=10"

# S6 切换（顺序承重，见 §3）
```

---

## 1. S1 前置体检

```bash
PG_DSN=postgresql://user:pass@127.0.0.1/scraper \
DB_BACKEND=postgres \
python tools/phase5_preflight.py
```

实测而非读配置：Python/CPU/内存/磁盘、依赖是否装齐、环境变量、PG 版本与扩展与
编码与排序规则、能否建分区表、advisory lock 能否取到、增量导出端点的路由顺序。

**必须处理的硬失败**；警告逐条确认是有意的。特别注意两条：

| 警告 | 为什么值得停下来看 |
|---|---|
| `排序规则` 不是 `C` | PG 默认排序规则与 SQLite 的 BINARY 不同，`TEXT` 列的 `ORDER BY` 会给出不同顺序。**建库时用 `LC_COLLATE='C' LC_CTYPE='C' TEMPLATE template0`**，事后改要重建库 |
| `import selectolax` 缺失 | 它是**生产解析引擎**。缺了 worker 走 lxml 回退，采出来的数据与预期不同——而这正是 Phase 5 要比对的东西 |

建库的正确姿势：

```sql
CREATE ROLE scraper LOGIN PASSWORD '...';
CREATE DATABASE scraper OWNER scraper
  TEMPLATE template0 LC_COLLATE 'C' LC_CTYPE 'C' ENCODING 'UTF8';
\c scraper
CREATE EXTENSION IF NOT EXISTS pg_trgm;
```

`postgresql.conf` 建议值见 `.agent/pg_migration_plan.md` §0.1（2 核 / 4GB）。
`default_toast_compression = lz4` 那条别漏——事件表的 jsonb payload 全走 TOAST，
它直接影响写入 CPU。

---

## 2. S2-S5 并行验证

### S2/S3 起服务

先用 SQLite 后端起一遍（确认新代码本身没问题），再切 PG。
两次都跑一遍冒烟：上传一个小批次 → 拉任务 → 提交结果 → 查结果 → 导出。

PG 后端首次启动会建 schema、表、索引、分区。**看日志确认没有 DDL 报错**，
并确认 relay 起来了：

```bash
curl -s http://新:8899/api/_debug/event-stream | python -m json.tool
# relay_state 应为 running；outbox_depth 不应单调增长
```

### S4 内容比对（本阶段的核心）

**两套系统各自采同一批 ASIN**（各用各的 worker、各自的代理），采完跑：

```bash
python tools/phase5_compare.py \
    --old http://旧:8899 --new http://新:8899 \
    --limit 500 --json phase5_diff.json
```

> **两边采集的时间不要差太远**（建议 1 小时内）。差太远的话慢变字段可能真的
> 变了，那是商品变了不是系统错了，会造成假阳性。

工具把每处差异分成三类：

| 类别 | 含义 | 影响判定？ |
|---|---|---|
| `EXPECTED` | 本次**有意**的变更，带 D 编号 | 否 |
| `VOLATILE` | 价格/库存这类天然会变的 | 否 |
| `UNEXPECTED` | 以上都不是 | **是** |

**为什么必须分类**：这次迁移有意改了七八个字段的值（解析器修复、时区格式、
列表排序……）。不认识这些变更的差异报告会全红，把真问题淹死。

工具还会做两件容易被忽略的事：

1. **有意变更但方向反了 → 判 UNEXPECTED。** 例如 `long_description` 的修复是
   「不再吸进容器外文本」，所以新值**应当变短**；变长了说明改错了方向。
   只按字段名放行会把真回归盖章成预期内。
2. **有意变更「该出现却没出现」→ 单独列出。** 两边 `crawl_time` 完全一样，
   说明 D-61 没生效（或者两边跑的是同一个版本）——「跑完全绿」不该被误读成
   「修复都生效了」。

**通过标准：`UNEXPECTED` 为 0，且没有只在一边出现的 ASIN。**
每一条 `UNEXPECTED` 都必须解释清楚才能往下走。

顺便人工抽查报告末尾的样本，确认这几处有意变更**改对了方向**：

- `manufacturer`：应从年龄段/尺寸之类变成真的厂商名
- `long_description`：应不再含 `$xx.xx` / `in stock` / `ratings` / CDN URL
- `seller_name`：Amazon 自营页应从 `N/A` 变成 `Amazon.com`

### S5 事件流与契约

```bash
# 对账：counts 的 count 应与直查一致
curl -s "http://新:8899/api/v1/sync/counts?from_seq=0&to_seq=100000000"
psql -c "SELECT count(*) FROM scraper.scrape_events"

# 契约端到端：分页拉到底，source_id 应无重复
curl -s -H "X-Export-Token: $EXPORT_TOKEN" \
     "http://新:8899/api/export/incremental?cursor=0&limit=500"
```

检查项：

- `source_id` 全局唯一（拉完全量后 `sort | uniq -d` 应为空）
- `cursor` 严格升序、`next_cursor` 等于最后一条的 cursor
- 空页返回 **200** 且 `next_cursor` **不推进**（不是 404）
- `scraped_at` 形如 `2026-08-05T10:00:00Z`（精确到秒、带 Z）
- `outcome != 'ok'` 的记录 `slow`/`fast` 基本为空——**这类只进 snapshots**

### 顺带定掉 N1

`site` 字段的值域（现在恒 `"US"`，而 DB 列默认是 `'amazon.com'`）留到并行期定：
**两套系统同时在跑，回滚成本最低**。见 `.agent/MIGRATION_STATUS.md` §2。

---

## 3. S6 切换（顺序是承重的）

> 两条耦合约束决定了这个顺序，**不要重排**：
>
> **(a)** worker 与 server **独立部署**，灰度期两种提交体必然同时在线；
> **(b)** D-61（worker 写 RFC3339）必须在 D-41（relay 认双格式）**之后**上线——
> 反了的话每一条记录的 `collected_at` 都会退回 `recorded_at` 兜底。

| # | 动作 | 校验点（不过就停） |
|---|---|---|
| 1 | 部署 **server**（含 relay 双格式解析） | `/api/_debug/event-stream` 的 `relay_state=running`；旧 worker 提交仍正常入库 |
| 2 | 观察 30 分钟 | `outbox_depth` 不单调增长；`collected_at_fallback` 计数不涨 |
| 3 | 部署 **worker**（RFC3339 + 质量信号） | 新提交的记录 `parse_engine` 有值、`zip_verify` 不再恒 `unverified` |
| 4 | 观察 30 分钟 | 同上；`/api/results` 抽查几条，慢变字段正常 |
| 5 | 开 catalog_sync 只读拉取（**先不 ack**） | 连续拉 1 小时，`source_id` 无冲突、无 409 |
| 6 | 接 `/ack` | 保留期下界开始跟随 `ack_seq` |

### 回滚

任一步不过就停在那一步。回滚方式：

- **第 1-2 步**：server 回滚到上一版即可。事件流表留着无害（没有消费方）。
- **第 3-4 步**：worker 回滚。relay 认双格式，所以旧 worker 的提交仍能正确解析——
  **这正是双格式兼容存在的理由**。
- **第 5-6 步**：停 catalog_sync 即可。采集侧不受影响。
- **整体退回 SQLite**：`DB_BACKEND=sqlite` 重启。SQLite 库全程没被碰过
  （`common/database.py` 一个字节没改），数据还在。

---

## 4. 上机后要盯的指标

```bash
curl -s http://新:8899/api/v1/sync/status | python -m json.tool
```

| 指标 | 正常 | 不正常意味着 |
|---|---|---|
| `relay_state` | `running` | `failed`/`refused` = 事件流停了 |
| `outbox_depth` | 波动但不单调增长 | 单调增长 = relay 抽不动或挂了 |
| `relay_lag_seconds` | < 60 | 持续偏高 = relay 有问题 |
| `forced_prune_log` | 空 | 非空 = **真的裁掉了还没被 ack 的数据** |
| `free_disk_bytes` | 充足 | 逼近下界会触发应急裁剪 |

另外盯两个**这次特意做出来的先兆信号**：

- `consec_tick_fail`：relay 连续失败次数。它会在 `relay_state` 翻转**之前**先涨——
  当初的缺陷正是「每 tick 都失败但状态一直报 running」。
- `collected_at_fallback`：`crawl_time` 解析失败退回 `recorded_at` 的计数。
  worker 灰度期它应该是 0；不是 0 说明 §3 的部署顺序反了。

---

## 5. 已知残余（不是这一阶段要解决的）

完整清单见 `.agent/MIGRATION_STATUS.md` §2/§3/§4。上机时最可能撞到的：

- `POST /api/tasks/release {"task_ids":["1"]}`：SQLite 靠类型亲和把 `'1'` 转成 `1`
  返 200，PG 抛 `DataError` 返 500。真实 worker 传 int，仅畸形客户端可达。
- `get_results` 的 `?search=<≥3字符>&cursor=<id>` → 500 是**刻意复现**的既有缺陷。
- 整机快照回滚服务端检测不到，只能靠消费侧 `max_seq` 单调性。
- 真正的写并发没做（单写连接 + 真锁保持不变），要换先得把 `app.py` 的 24 处
  裸 SQL 抽干净。

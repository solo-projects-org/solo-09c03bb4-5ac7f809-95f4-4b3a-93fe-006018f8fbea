# 海洋数仓 · 浮标观测包接收服务

浮标站每日 Parquet 观测包入湖的接收 API。支持**双实例**共享同一存储、
**内容寻址去重**、**字节配额**、**崩溃自动回收**与完整的**接收进度查询/原件下载**。

技术栈：FastAPI + PyArrow + SQLite（WAL）+ uvicorn，全部阻塞 I/O 在线程池执行，
慢上传不阻塞其他接收与查询。

---

## 1. 核心能力与需求对应

| 需求 | 实现 |
| --- | --- |
| 按接收号声明站号、UTC 日期、长度、SHA-256，再上传原包 | `PUT /receipts/{id}/declaration` → `POST /receipts/{id}/content` |
| 包内含站号、UTC 时刻、盐度三列，逐行站号/站日符合声明 | `app/validation.py` 用 PyArrow 逐行校验，保留全部原始行（只读不改写） |
| 每次接收独立留档，可查进度、下载原件 | `receipts` 表每接收一行；`GET /receipts`、`GET /receipts/{id}`、`.../download` |
| 同号同声明重试沿用原记录，异声明拒绝 | 声明幂等；四要素任一不同返回 `409 declaration_conflict` |
| 配置原包字节额度，暂存与成品共用 | `OBS_QUOTA_BYTES`；口径 = 成品 blob + 在途暂存 |
| 相同内容跨接收只存一份、不重复占额 | 内容寻址 `blobs/<sha 前2位>/<sha>` + `ref_count`，硬链接发布 |
| 空间不足明确拒收 | `507 quota_exceeded`，落盘/发布前按实际内容核算 |
| 长度、摘要、内容均通过才发布，失败无可下载成品 | 三关依次校验；失败保持 `UPLOADING`，下载返回 `409` |
| 中断后无用文件及占额自动回收，未完成可重传 | 启动 + 周期 + 手动 `reconcile`，清理孤儿暂存/成品、回退缺件记录 |
| 恢复不误删另一实例在途/已发布原包，不留缺件成功记录 | 全局 flock + 写事务对账；在途暂存按 DB 归属保留，缺件成品回退而非掩盖 |
| 慢上传期间其他包仍可接收、查询 | 网络读取异步、阻塞重活进 I/O 线程池；接收级 flock 只锁同一接收号 |
| 按站号 + UTC 日期查询已发布原件的盐度冲突 | `GET /reports/salinity-conflicts`；`app/report.py` 读取已发布 Parquet 原件 |
| 同原件多接收号重送只计一次，列冲突明细与 SHA/接收号 | 快照内按 `blob_sha256` 去重；`conflicts[].values[].sha256s/receipt_ids` |
| 缺测/非有限盐度明确标出，不能算作无冲突 | `missing` / `non_finite` + `salinity_state`；`has_conflicts=true` |
| 报告对应同一时点完整原件集合，发布/删除后再查反映变化 | 全局锁内固化集合并硬链接私有快照，逐次查询重新取快照 |
| 双实例操作与慢扫描交错不拼混杂报告，原件不可读则明确失败 | 快照隔离 + 专用扫描线程池；缺件 409 / 读取失败 500 |
| 扫描不拖住其他包接收 | 报告扫描独立线程池，与接收 I/O 池隔离 |
| 可启动的双实例 Docker Compose 与 Swagger 入口 | `docker-compose.yml`（app1/app2 + nginx），`/docs` Swagger |

---

## 2. 目录结构

```
app/
  config.py      环境变量配置（数据目录、额度、对账周期等）
  db.py          SQLite 连接、WAL、BEGIN IMMEDIATE 写事务、建表
  errors.py      领域异常与 HTTP 状态码映射
  schemas.py     Pydantic 请求/响应模型
  storage.py     flock 文件锁、内容寻址 blob、暂存、配额、崩溃对账
  validation.py  PyArrow Parquet 三列与站号/站日逐行校验
  report.py      站日盐度冲突报告：快照、原件去重、时刻比对、失败语义
  service.py     声明/上传/发布/中止/删除编排，I/O 线程池
  main.py        FastAPI 路由、Swagger、后台对账、异常处理
tests/           pytest 单元/并发/回收测试 + 自研同步 ASGI 客户端
scripts/
  run_local.sh       无 Docker 时本机双实例启动
  integration_test.py 真实双进程端到端自测
Dockerfile  docker-compose.yml  nginx.conf  requirements.txt
```

数据卷布局（双实例共享）：

```
$OBS_DATA_DIR/
  obs.db                 SQLite（WAL：obs.db-wal / obs.db-shm）
  staging/<receipt>.part 在途暂存，按接收号命名
  blobs/<xx>/<sha256>    内容寻址成品
  locks/rcpt-<id>.lock   接收级互斥锁
  locks/global.lock      发布/对账全局锁
  locks/obs-report-*/    报告快照私有目录（原件硬链接 + scan.lock）
```

---

## 3. 快速开始

### 方式 A：Docker Compose（推荐，双实例）

```bash
docker compose up -d --build
# 统一入口（nginx 轮询到 app1/app2）
# Swagger:  http://localhost:8080/docs
# OpenAPI:  http://localhost:8080/openapi.json
# 健康检查:  http://localhost:8080/health
```

调整额度：`OBS_QUOTA_BYTES=10485760 docker compose up -d`（默认 1 GiB）。

### 方式 B：本机双进程（无 Docker 环境，自测用）

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
./scripts/run_local.sh
# 实例1 http://127.0.0.1:8001/docs ；实例2 http://127.0.0.1:8002/docs
```

---

## 4. API 流程

### 第一步：声明

```bash
curl -X PUT http://localhost:8080/receipts/RCV-0001/declaration \
  -H 'Content-Type: application/json' \
  -d '{"receipt_id":"RCV-0001","station_id":"BUOY-001",
       "obs_date":"2026-11-15","size":1488,
       "sha256":"<64位十六进制>"}'
```

- 同号同声明重复调用 → 幂等返回同一记录；
- 同号异声明（站号/日期/长度/摘要任一不同）→ `409`；
- 声明长度超过总额度 → `507`。

### 第二步：上传原包（body 即 Parquet 原始字节）

```bash
curl -X POST http://localhost:8080/receipts/RCV-0001/content \
  -H 'Content-Type: application/octet-stream' \
  --data-binary @BUOY-001_2026-11-15.parquet
```

依次通过 **长度 → SHA-256 → Parquet 三列与逐行站号/站日** 三关后发布为
`PUBLISHED`；任一失败返回 `422`，接收保持 `UPLOADING` 可重传，且**不产生成品**。

### 查询与下载

```bash
curl http://localhost:8080/receipts                         # 列表，可 ?state=&station_id=
curl http://localhost:8080/receipts/RCV-0001                # 单接收进度
curl -O -J http://localhost:8080/receipts/RCV-0001/download # 下载原件（未发布 409）
curl http://localhost:8080/quota                            # 额度占用
curl -X POST http://localhost:8080/admin/reconcile          # 手动触发回收对账
```

辅助接口：`POST /receipts/{id}/abort`（中止在途并释放暂存）、
`DELETE /receipts/{id}`（删除接收，唯一引用的成品一并回收）。

### 站日盐度冲突报告（质控）

```bash
curl 'http://localhost:8080/reports/salinity-conflicts?station_id=BUOY-001&obs_date=2026-11-15'
```

- 只读取该站该 UTC 日期的**已发布**原件；同内容（原件 SHA-256 相同）被多个
  接收号重送时只算一份，报告在该原件的 `receipt_ids` 下列出全部接收号。
- `conflicts` 按 UTC 时刻列出**两个及以上不同有限盐度**：
  每个取值附 `salinity`、原件 `sha256s` 与对应 `receipt_ids`。
- 盐度为 NULL（缺测）或 NaN/±Inf 的行进入 `missing` / `non_finite` 并带
  `salinity_state`（`missing` / `nan` / `positive_infinity` /
  `negative_infinity`）。出现这类行时 `status=incomplete`、`has_conflicts=true`、
  `clean=false`，**绝不算作无冲突**。
- `status`：`ok`（无冲突且无缺测/非有限）/ `conflict` / `incomplete` /
  `no_data`（该站日没有已发布原件）。
- 每次查询取一个**一致快照**：在全局锁内固化已发布接收集合并把原件硬链接到
  请求私有目录，然后释放锁慢扫描。扫描期间的发布/删除/对账不会混入本次报告；
  下一次查询重新取快照，因此发布或删除后立即反映变化。
- 原件缺失（快照时为 `409 report_snapshot_incomplete`）或无法完整读取/摘要
  不符/不可解析（`500 report_source_unreadable`）时**明确失败**，不输出残缺
  或「无冲突」报告。
- `conflict_count` / `anomaly_count` / `missing_count` / `non_finite_count`
  始终是**完整总数**；明细列表只返回 `offset..offset+limit` 窗口（默认
  `offset=0&limit=1000`，`limit` 上限 5000），用 `conflicts_truncated` /
  `anomalies_truncated` 判断是否还有下一页。超大站日不会生成无界响应。
- 扫描在报告专用线程池执行，不占用接收的 I/O 线程池，慢扫描不阻塞其他包接收。
- 快照目录位于 `$OBS_DATA_DIR/locks/obs-report-*`，正常请求结束即删除；进程
  崩溃残留由 `reconcile` 在确认无人持有该目录的 `scan.lock` 后回收。

### Parquet 约束

- 必需三列：`station_id`（字符串）、`timestamp_utc`（timestamp/date/ISO8601 字符串）、
  `salinity`（数值）；允许携带额外列，原样保留。
- 每行 `station_id` 必须等于声明站号；每行时间戳的 **UTC 日期**必须等于声明日期。
- 空表拒绝；时区按 UTC 归一化处理。

---

## 5. 并发与崩溃安全设计

1. **SQLite 多进程共享**：WAL + `busy_timeout` + `BEGIN IMMEDIATE`，写事务跨实例串行化。
2. **接收级 flock**：同一接收号同时只允许一个上传会话，冲突返回 `409 receipt_busy`；
   不同接收号互不阻塞。进程崩溃时内核自动释放锁。
3. **全局 flock**：发布与对账互斥，保证回收不会与在途发布竞争。
4. **原子发布**：成品先写 `.tmp` 再 `rename`；内容已存在则只增 `ref_count`，不重复占额。
5. **对账（reconcile）**：启动即执行，之后每 `OBS_RECONCILE_INTERVAL` 秒一次——
   - 无 `UPLOADING` 归属的暂存（含他实例崩溃残留）→ 删除；
   - `UPLOADING` 缺暂存 → 进度清零、标记可重传；
   - `PUBLISHED` 缺成品 → 回退 `UPLOADING` 并解除引用，杜绝缺件成功记录；
   - 无引用 blob → 删除并注销；账本与磁盘不符 → 以磁盘为准修正。

---

## 6. 配额语义

- `GET /quota` 返回 `quota_bytes / used_bytes / blob_bytes / staging_bytes / available_bytes`。
- 相同内容（同 SHA-256）无论被多少接收引用，物理只存一份，`blob_bytes` 只计一次。
- 落盘暂存前按「成品已用 + 本次暂存」核算；发布新内容前按「成品已用 + 本包」核算，
  超额均返回 `507` 且不留半成品。同内容重发因发布去重，不会因暂存而被误拒。

---

## 7. 测试

```bash
pip install pytest httpx pyarrow
python -m pytest tests/                  # 55 个用例（含并发、配额、崩溃回收、质控报告）
python -W error -m pytest tests/         # 严格模式（资源告警也视为失败）

# 真实双进程端到端：先按方式 B 启动两实例，再执行
OBS_DATA_DIR=/tmp/obs-e2e python scripts/integration_test.py
```

测试覆盖：完整发布/下载往返、幂等与冲突、长度/摘要/Parquet/站号/站日五类拒绝、
去重占额、配额 507、慢上传不阻塞、同接收并发互斥、孤儿暂存/缺件成品回收、
双 Database 连接共享状态，以及站日盐度冲突报告（原件去重、冲突明细、缺测/
非有限标记、已发布集合变更反映、慢扫描与发布/删除交错不混杂、原件缺失或损坏
明确失败、慢扫描不阻塞接收、明细窗口与完整总数、快照目录回收）。

---

## 8. 配置项（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `OBS_DATA_DIR` | `/data` | 数据根目录（DB、暂存、成品、锁） |
| `OBS_QUOTA_BYTES` | `1073741824` | 原包字节总额度（暂存+成品共用） |
| `OBS_INSTANCE_ID` | `instance-0` | 实例标识，仅用于健康检查/排查 |
| `OBS_RECONCILE_INTERVAL` | `30` | 周期对账间隔秒 |
| `OBS_CHUNK_SIZE` | `1048576` | 流式分块大小 |

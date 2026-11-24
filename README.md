# 质检影像缺帧核对 API

质检影像缺档时，用于判断缺档该找**采集端**补登记，还是找**影像库**补归档的核对服务。
FastAPI + SQLite，全部以**闭区间号段**为单位存储与计算，帧号可跨十亿且不逐帧展开。

## 业务模型

- **批次**：登记每批影像应有的帧号范围 `[frame_start, frame_end]`，首尾包含。
- **清单**：采集端（`collector`）与影像库（`library`）各凭全局唯一的清单号，
  对一个**连续号段**做三种操作：
  - 登记（POST `/manifests`）
  - 更正（PUT `/manifests/{manifest_no}`），只允许改号段，批次/侧别不变
  - 撤回（POST `/manifests/withdraw`），撤回后同侧其他清单覆盖的帧仍算有记录
- **核对**（GET `/batches/{id}/reconcile`）返回四类**最大连续段**：
  `both_present`（双方齐全）、`only_collector`（仅采集端有 → 找影像库补归档）、
  `only_library`（仅影像库有 → 找采集端补登记）、`neither_present`（双方皆缺）。
- **按帧追查**（GET `/batches/{id}/frames/{frame_no}`）：返回该帧状态及两侧命中的清单号。

判定口径：`only_collector` 说明采集端已登记、影像库缺档 → 找影像库补归档；
`only_library` 相反 → 找采集端补登记；`neither_present` 两侧都要补。

## 关键设计

| 需求 | 实现 |
| --- | --- |
| 帧号跨十亿、不逐帧展开 | 帧号以 64 位整数存号段（`seg_start/seg_end` 闭区间），算法复杂度只与号段数有关 |
| 改撤只更新受影响号段 | `coverage` 表缓存每侧已合并覆盖段；写操作只对新旧号段涉及的区间做局部重建（`refresh_region`），不重扫整批清单 |
| 按帧追查不扫批次 | `frame_index` 用覆盖整个 int64 域（65 层）的区段树做规范分解：一张清单最多 128 个索引项；查帧沿根到叶 65 个节点键点查，读量只随命中清单数增长 |
| 清单、覆盖段、索引同时生效 | 每次写操作在单个 `BEGIN IMMEDIATE` 事务内同时更新清单表、覆盖段表与帧索引，任何一步异常整笔回滚 |
| 越界/入库失败保留原状 | 越界在事务内先校验抛 422；任何错误回滚，已提交数据不变；帧号超 int64 在入参层拦截 |
| 撤回不影响同侧重叠清单 | 撤回只删该清单的索引项与覆盖重算；同侧其他清单的索引项不变，查帧仍列出其余来源 |
| 旧库升级可直接追查 | `init_db` 以 `PRAGMA user_version` 检测旧库，从清单表回填帧索引并重建覆盖段，原有清单无需重录 |
| 一次查帧同一份已提交数据 | 批次范围与两侧命中清单由同一条 SQL 取回，SQLite 语句级读快照保证判定与列表一致 |
| 最大连续段 | 号段合并时首尾相邻（`start <= prev_end + 1`）即合并，核对结果天然为最大连续段 |

### 四张表

- `batches`：批次号 + 应有帧号范围
- `manifests`：清单号、批次、侧别、状态（active/withdrawn）、号段
- `coverage`：按批次+侧别缓存的合并覆盖段（增量维护）
- `frame_index`：按帧追查的倒排索引，`(batch_id, node_key, side, manifest_no)`，
  每张有效清单的号段分解为至多 128 个树节点

### 按帧追查的索引方案

区段树第 0 层覆盖 `[INT64_MIN, INT64_MAX]`，每层二分，第 64 层为单帧。
清单号段被规范分解为「完全落在号段内」的树节点（Kraft 不等式保证
至多 `2 × 64 = 128` 个，实测最坏 84 个），长号段只按号段写入节点，
不会逐帧展开。查某帧时取其根到叶的 65 个节点键做等值点查：命中该帧的
清单，其分解节点必然包含路径上的某一个键；反过来路径上每个键只见于
完全覆盖该帧的清单，因此没有误命中，无需回表判断区间。

`node_key` 用 BLOB（1 字节层号 + 8 字节无符号序号），因为第 64 层的节点
序号可达 `2^64-1`，超出 SQLite 有符号 64 位整数。主键前缀
`(batch_id, node_key)` 让一次查帧用 65 次主键点查取回两侧索引项。

号段运算见 `app/intervals.py`（合并 / 交集 / 差集，纯函数，零第三方依赖）。

## 查帧读量实测

`scripts/measure_trace.py` 用 SQLite progress handler 统计一次
`trace_frame` 实际执行的 VDBE 指令数（确定性、可复现，不依赖机器性能）：

| 场景 | 指令步数 |
| --- | --- |
| 2 条命中清单（基线） | 803 |
| 再登记 5000 条无关清单 | 803（+0） |
| 命中 12 条 | 1113 |
| 命中 112 条 | 4213 |
| 命中 1112 条 | 35213 |
| 无命中帧 + 5000 条无关清单 | 773 |
| 更正把一张清单移出该帧后（其余来源保留） | 35182 |
| 同侧重叠清单撤回一张后（其余来源仍在） | 35151 |

结论：无关清单从 2 条涨到 5002 条，查帧读量一步不增；读量只随同帧
命中清单数近似线性增长。作为对比，同一数据量下旧的区间相交扫描在
「大量短清单 + 一张大号段」场景需要 6207 步，且随清单总数继续增长。

## 启动方式

### Docker Compose（推荐）

```bash
docker compose up --build
# 服务： http://localhost:8000
# 文档： http://localhost:8000/docs   （Swagger UI，可直接演示缺帧核对）
```

SQLite 数据文件在命名卷 `qc-data`（容器内 `/data/qc.db`），WAL 模式。

### 本地直接运行

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/batches` | 登记批次应有帧号范围 |
| GET | `/batches/{batch_id}` | 查看批次 |
| POST | `/manifests` | 登记清单号段（自动越界校验） |
| PUT | `/manifests/{manifest_no}` | 更正号段（局部增量更新） |
| POST | `/manifests/withdraw` | 撤回清单（幂等冲突返回 409） |
| GET | `/manifests/{manifest_no}` | 查看清单 |
| GET | `/batches/{batch_id}/manifests` | 批次清单列表（可按 side 过滤） |
| GET | `/batches/{batch_id}/reconcile` | 四类最大连续段核对结果 |
| GET | `/batches/{batch_id}/frames/{frame_no}` | 按帧追查两侧清单号 |
| GET | `/health` | 健康检查 |

### 快速演示

```bash
python scripts/demo.py            # 默认打 http://127.0.0.1:8000
python scripts/measure_trace.py   # 查帧读量自测（不依赖服务与网络）
pytest -q                         # 37 个测试
```

## 错误约定

- `404`：批次/清单不存在
- `409`：批次号/清单号重复、撤回已撤回清单、更正已撤回清单
- `422`：号段非法（起点大于终点）、越出批次范围、帧号超 64 位整数
- 所有写错误都在提交前拦截，库内状态保持调用前原状

## 目录结构

```
app/
  intervals.py   # 号段合并/交集/差集纯函数
  db.py          # SQLite 表结构、事务、覆盖段增量维护、帧索引、核对/查帧
  main.py        # FastAPI 路由与入参校验
tests/           # 号段算法 + 帧索引 + 查帧语义 + API 端到端测试
scripts/demo.py  # 标准库演示脚本（容器内可直接跑）
scripts/measure_trace.py  # 查帧读量自测
Dockerfile, docker-compose.yml, requirements.txt
```

## 数据库升级

`frame_index` 是 1.1 起新增的第四张表，旧库（只有三张表）首次启动时
由 `app.lifespan` → `db.init_db` 自动升级：写入 `PRAGMA user_version=2`，
从 `manifests` 中所有 `active` 清单回填索引项，并按清单重建 `coverage`。
升级后原有清单可直接 `GET /batches/{id}/frames/{frame_no}` 追查，无需
重新登记；升级失败整笔事务回滚，旧数据保持原状。

## 待完成事项

- **容器未验证**：本环境没有 Docker，`Dockerfile` / `docker-compose.yml` 按原样保留并只做了静态核对；服务用同样的 uvicorn + SQLite 在本机启动，`/health`、`/docs`、`/openapi.json` 与 `scripts/demo.py` 全流程通过。请在装有 Docker 的机器上执行一次 `docker compose up --build` 确认镜像构建与命名卷。
- **迁移只在单测里验证**：旧库升级覆盖「三表无索引」→ 回填 → 查帧，也测了 8 线程并发首次启动；尚未在真实生产库副本上跑过，升级前建议备份 `qc.db`（WAL 模式需连同 `-wal`/`-shm` 一起）。
- **索引写放大**：每张清单固定至多 128 个索引项（实测最坏 84），登记/更正会写这些行；这是用写入量换查帧读量的取舍，若清单量极大需评估写放大。
- **读量指标口径**：实测用的是 VDBE 指令数（确定性、与机器无关），不是墙钟耗时；真实延迟还受页缓存与磁盘影响。
- **未做的优化**：`frame_index` 没有按 `side` 再拆表，查帧一次取回两侧；若将来单侧清单量级差异极大，可再评估。

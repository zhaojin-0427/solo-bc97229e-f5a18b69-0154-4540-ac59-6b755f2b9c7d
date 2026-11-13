# 自然博物馆标本登记库同步系统

把本地 SQLite(WAL) 标本登记库持续复制到一个**独立的** SQLite 副本，供分布研究
查询；鉴定员仍直接在源库登记、更正、撤回。提供 Flask 网页启动/停止同步、查看
进度与错误、按标本号查询副本，并附带可持续修改的示例源库。

## 需求与实现对照

| 需求 | 实现方式 |
| --- | --- |
| 源库可并发提交增删改，首次复制期间也不停写 | 同步器只用只读短事务打开源库；WAL 允许读写并发，源写入不等待任何复制/同步完成 |
| 不得漏更 | 源库 `_change_log` 触发器记录全部行变更；首次复制 = 分页快照 + 重放快照窗口内全部日志；增量按序号续读 |
| 不得恢复已删标本 | 删除事件 `D` 按序号重放为 `DELETE`；删除后不会再被任何快照/重放路径插回 |
| 不得以旧鉴定覆盖新值 | 日志按 `seq` 升序应用，`INSERT OR REPLACE` 永远以更大序号携带的最新值为准 |
| 一次源事务的改动一起可见，回滚不传播 | 触发器与 DML 同事务：COMMIT 则行与日志原子可见，ROLLBACK 则一起消失，同步器永远看不到回滚内容 |
| 就绪前不供查询 | 副本仅在 `phase=RUNNING` **且** 持久标记 `bootstrap_completed=1` 时才接受查询，否则 HTTP 503；该标记只在首拷重放完成、与数据/进度同一事务发布时置 1 |
| 增量同步不反复扫描全表 | 只执行 `WHERE seq > ? AND seq <= ?` 的主键索引区间查询（`EXPLAIN QUERY PLAN` 测试断言为 SEARCH 而非 SCAN） |
| 副本数据、进度、就绪状态共同提交 | 每批数据变更、审计流水、`sync_meta` 进度在副本的**同一个事务**中 COMMIT；首拷发布时 `bootstrap_completed=1`、`phase=RUNNING`、最终序号也在同一事务落盘 |
| 初次复制中断（停止/异常/进程退出）可重做且全程不可查 | 中断时 `bootstrap_completed` 始终为 0（哪怕 `last_applied_seq` 已非零）；重启以该标记而非“进度是否非零”为准，清空半成品重做，期间副本一律 503 |
| 就绪后重启按持久进度增量续跑 | 仅当 `bootstrap_completed=1`（含空库首拷）才跳过快照，从持久序号续跑；不重做、不清空 |
| 旧副本无法证明首拷完成时保守重做 | 旧库迁移自动补 `bootstrap_completed` 列并默认 0：不能仅凭非零进度/ RUNNING 放行，先重做首拷 |
| 源为本地 SQLite WAL | 源库 `PRAGMA journal_mode=WAL`；副本为独立 SQLite 文件，独立挂载卷 |
| 允许增加辅助表及触发器 | 源库增加 `_change_log` 表与 3 个触发器；副本增加 `sync_meta`、`_applied_changes` |

## 架构

```
鉴定员/演示写入器 ──写──> 源库 specimens.db (SQLite WAL)
                            │ specimens 表
                            │ + _change_log (AFTER I/U/D 触发器, AUTOINCREMENT seq)
                            │
              只读短事务 SELECT（不阻塞写入）
                            ▼
                    SyncEngine 后台线程
              ┌─────────────────────────────┐
              │ bootstrap: 主键分页快照       │
              │   → 重放日志 [1, w1] → RUNNING│
              │ incremental: seq > 已应用序号 │
              └─────────────────────────────┘
                            │ 每批一个事务：数据 + 进度一起 COMMIT
                            ▼
                    副本 specimens_replica.db (独立 SQLite)
              specimens / sync_meta(持久进度) / _applied_changes(审计)
                            ▲
              Flask 网页（仅 RUNNING 可查，否则 503）
```

### 首次复制为什么正确（并发窗口）

1. 记录源端当前最大序号 `w0`；
2. 按主键分页把 `specimens` 快照写入副本（每页独立短事务，无长读锁）；
3. 快照结束再读一次最大序号 `w1`；
4. 重放日志区间 `[1, w1]`（从 1 开始对快照行是幂等覆盖）。

快照期间发生的任何并发提交，其日志序号都落在 `(w0, w1]`，必然被第 4 步覆盖：
插入被补进、删除被删掉（不复活）、更正以新值覆盖旧值。

### 首拷发布与崩溃恢复（关键不变量）

`sync_meta.bootstrap_completed` 是“首拷已完整完成”的**唯一权威凭证**，
与 `phase`、`last_applied_seq` 一样持久化在副本里：

* 重放途中每批日志都会把 `last_applied_seq` 推进到非零，但只要首拷没跑完，
  `bootstrap_completed` 始终为 0。因此**非零进度绝不能当作就绪证据**——
  这正是“回放 500 条历史日志后停止、重启却读到已撤回标本”的根因；
* 只有快照 + 区间 `[1, w1]` 全部重放完成，才在**同一个事务**里写入
  `bootstrap_completed=1`、`phase=RUNNING`、最终序号并 COMMIT。事务提交前
  任意时刻进程被停止、异常或直接退出，标记都保持 0；
* 查询门 `is_ready` 同时要求 `phase=RUNNING` 与 `bootstrap_completed=1`，
  所以未发布的半成品副本（哪怕物理行已落盘）对外始终 503；
* 重启决策只看该标记：为 0 则 `reset` 后重做 bootstrap（幂等，重做期间源库
  仍可继续增删改）；为 1（含空库）则按持久序号增量续跑，绝不清空重建；
* 旧版本副本没有这一列，初始化时 `ALTER TABLE` 补列并默认 0 ——
  无法证明首拷完成就保守重做。

## 目录结构

```
app/
  source_db.py     源库：WAL、specimens、CDC 表与触发器、演示写入 API
  replica_db.py    副本：表结构、持久进度 meta、就绪判定、查询
  sync.py          同步引擎：bootstrap + 增量循环，后台线程生命周期
  web.py           Flask 应用与 REST API
  templates/index.html  控制台页面
scripts/
  seed_source.py   播种示例源库（容器入口调用）
  demo_mutator.py  持续演示写入器（周期增/改/撤）
tests/             pytest 测试（CDC 正确性、并发、崩溃、发布/恢复、空库、Web E2E）
Dockerfile, docker-compose.yml, entrypoint.sh, wsgi.py
```

## 用 Docker Compose 启动（推荐）

```bash
# 构建并启动 Web + 后台同步（源库自动播种 8 条示例标本）
docker compose up -d --build

# 浏览器打开
#   http://localhost:8000/
```

打开网页后可以：

* 查看同步阶段（INIT/BOOTSTRAP/RUNNING/STOPPED/ERROR）、已应用序号、源端水位、错误；
* 启动/停止同步；按标本号查询副本（未就绪时返回 503）；
* 在页面上直接对**源库**新增 / 更正 / 撤回，观察副本自动追上。

附带一个持续写入器，模拟鉴定员不停操作（可选 profile，不影响主服务）：

```bash
docker compose --profile mutator up -d
docker compose logs -f mutator
```

数据持久化在两个独立命名卷 `source_data`、`replica_data`。删除容器再
`docker compose up -d` 会自动按副本里的持久进度接续；清空 `replica_data` 卷
则会重新做首次复制。

## 本地（无 Docker）运行

```bash
pip install -r requirements.txt
export SOURCE_DB=./data/source/specimens.db
export REPLICA_DB=./data/replica/specimens_replica.db
python -m scripts.seed_source          # 播种示例源库（可选，Web 启动也会自动播种）
python -m flask --app wsgi:app run --port 8000
# 或：gunicorn --bind 0.0.0.0:8000 --workers 1 --threads 8 wsgi:app
```

## REST API

| 方法 路径 | 说明 |
| --- | --- |
| `POST /api/sync/start` | 启动后台同步（已在运行则返回 `started:false`） |
| `POST /api/sync/stop` | 请求停止（当前批次完成后退出，phase=STOPPED） |
| `GET  /api/status` | 阶段、`last_applied_seq`、源端水位、已拷贝行数、错误、线程状态 |
| `GET  /api/specimen/<id>` | 按标本号查副本；未就绪 503，不存在/已撤回 404 |
| `POST /api/demo/insert` | 对源库新增 `{id,taxon,locality}` |
| `POST /api/demo/update` | 对源库更正 `{id,taxon,locality}` |
| `POST /api/demo/delete` | 撤回源记录 `{id}` |
| `GET  /api/source/specimens` | 源库当前标本（演示辅助） |
| `GET  /healthz` | 健康检查 |

## 测试

```bash
pip install pytest
pytest tests/ -v
```

覆盖要点：触发器捕获全部 DML、回滚不传播、已提交序号单调、单事务多行原子可见、
首次复制期间并发增删改的最终一致性、删除不复活、旧值不覆盖新值、中断重做、
**首拷重放中途停止/进程硬退出（os._exit）后副本全程 503、重启重做后撤回标本不复活、
重做期间源库持续增删改、非零进度但无就绪标记不放行、旧副本缺列迁移后保守重做、
空库首拷发布与重启增量续跑不清空、** 重启按持久进度接续且不重做快照、
提交点崩溃后数据与进度均不半生效、增量走索引不全表扫描、同步期间源写入不被阻塞、
就绪前 503、Web 端到端增改撤链路。

## 关键环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `SOURCE_DB` | `/data/source/specimens.db` | 源库文件路径 |
| `REPLICA_DB` | `/data/replica/specimens_replica.db` | 副本文件路径 |
| `AUTO_START` | `1` | 容器/应用启动时是否自动开始同步 |
| `POLL_INTERVAL` | `1.0` | 无新变更时的轮询秒数（stop 可立即唤醒） |
| `SNAPSHOT_PAGE_SIZE` | `500` | 首次复制快照每页行数 |
| `CHANGE_BATCH_SIZE` | `500` | 增量每批应用的日志条数 |
| `MUTATOR_INTERVAL` | `3` | 演示写入器操作间隔秒数 |

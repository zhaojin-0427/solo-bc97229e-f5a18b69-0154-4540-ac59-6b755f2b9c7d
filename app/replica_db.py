"""副本库（独立 SQLite）。

副本只由同步器写入，结构刻意与源库解耦：
  * specimens        — 标本数据副本；
  * sync_meta        — 单行表，保存阶段、已应用日志序号等持久进度；
  * _applied_changes — 已应用变更的序号流水（附带便于排查的审计信息）。

同步器保证：数据行的修改与 sync_meta 进度更新在 *同一个事务* 中提交，
因此“副本数据与进度共同提交”，崩溃后要么整批生效、要么整批回滚。

就绪判定（关键）
================
``sync_meta.bootstrap_completed`` 是“首次复制已经完整完成”的唯一权威凭证：
  * 它只在 bootstrap 日志重放全部完成、``phase`` 置为 RUNNING 的 *同一个
    事务* 里翻成 1；此前无论快照拷了多少行、重放提交了多少批日志（此时
    ``last_applied_seq`` 可能早已非零），该标记始终为 0；
  * 重启续跑以该标记、而非“进度是否非零”为准：标记为 0 一律保守重做
    bootstrap，绝不仅凭非零进度放行查询；
  * 旧版本副本没有这一列，迁移时默认补 0 —— 无法证明首拷完成就重做。
"""
from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager

# 同步阶段
PHASE_INIT = "INIT"          # 尚未开始 / 初次复制未完成
PHASE_BOOTSTRAP = "BOOTSTRAP"  # 正在做首次后台复制
PHASE_RUNNING = "RUNNING"    # 已就绪，持续增量同步
PHASE_STOPPED = "STOPPED"    # 被用户停止
PHASE_ERROR = "ERROR"        # 同步出错


class _ReplicaConnection(sqlite3.Connection):
    """带身份标记的连接，便于与源库连接区分（例如测试中注入故障时）。"""

    _is_replica = True


def connect(path: str, *, timeout: float = 30.0) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(
        path, timeout=timeout, isolation_level=None, factory=_ReplicaConnection
    )
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout={int(timeout * 1000)}")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    # 外键关闭：副本是独立的派生库，不依赖源库对象
    conn.execute("PRAGMA foreign_keys=OFF")
    return conn


@contextmanager
def open_replica(path: str):
    conn = connect(path)
    try:
        yield conn
    finally:
        conn.close()


def init_replica(path: str) -> None:
    """创建副本结构（幂等）。"""
    with open_replica(path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS specimens (
                id       INTEGER PRIMARY KEY,
                taxon    TEXT NOT NULL,
                locality TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sync_meta (
                id                 INTEGER PRIMARY KEY CHECK (id = 1),
                phase              TEXT NOT NULL,
                last_applied_seq   INTEGER NOT NULL DEFAULT 0,
                source_max_seq     INTEGER NOT NULL DEFAULT 0,
                bootstrap_copied   INTEGER NOT NULL DEFAULT 0,
                bootstrap_completed INTEGER NOT NULL DEFAULT 0,
                last_error         TEXT,
                updated_at         TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )
        # 旧副本（没有 bootstrap_completed 列）的在线迁移：默认补 0，
        # 即“无法证明首拷完成 -> 保守重做 bootstrap”，绝不凭非零进度放行。
        cols = {
            r["name"]
            for r in conn.execute("PRAGMA table_info(sync_meta)")
        }
        if "bootstrap_completed" not in cols:
            conn.execute(
                "ALTER TABLE sync_meta"
                " ADD COLUMN bootstrap_completed INTEGER NOT NULL DEFAULT 0"
            )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS _applied_changes (
                seq        INTEGER PRIMARY KEY,
                op         TEXT NOT NULL,
                specimen_id INTEGER NOT NULL,
                applied_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )
        conn.execute(
            "INSERT OR IGNORE INTO sync_meta(id, phase) VALUES (1, ?)",
            (PHASE_INIT,),
        )


def get_meta(path: str) -> dict:
    fallback = {
        "phase": PHASE_INIT,
        "last_applied_seq": 0,
        "source_max_seq": 0,
        "bootstrap_copied": 0,
        "bootstrap_completed": 0,
        "last_error": None,
    }
    if not os.path.exists(path):
        return fallback
    with open_replica(path) as conn:
        try:
            row = conn.execute("SELECT * FROM sync_meta WHERE id = 1").fetchone()
        except sqlite3.OperationalError:
            # 副本文件存在但表结构尚未初始化
            return fallback
        if row is None:
            return fallback
        return dict(row)


def is_ready(path: str) -> bool:
    """就绪前不供查询。

    必须同时满足：处于 RUNNING 阶段 *且* 持久标记 bootstrap_completed=1。
    首拷中断（含日志重放已提交若干批、last_applied_seq 非零但标记仍为 0）
    的副本永远不可查，重启后先保守重做。
    """
    if not os.path.exists(path):
        return False
    meta = get_meta(path)
    return (
        meta["phase"] == PHASE_RUNNING
        and int(meta.get("bootstrap_completed", 0)) == 1
    )


def get_specimen(path: str, specimen_id: int):
    with open_replica(path) as conn:
        row = conn.execute(
            "SELECT id, taxon, locality FROM specimens WHERE id = ?",
            (specimen_id,),
        ).fetchone()
        return dict(row) if row is not None else None


def reset_for_bootstrap(path: str) -> None:
    """初次复制（重新）开始：清空一切派生数据，回到 INIT 进度。

    初次复制中断后重做时调用，保证半成品快照不会残留；同时把
    bootstrap_completed 明确清 0 —— 在新一次首拷完整跑完之前，
    副本始终不就绪、不可查。
    """
    with open_replica(path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DELETE FROM specimens")
        conn.execute("DELETE FROM _applied_changes")
        conn.execute(
            """
            UPDATE sync_meta
               SET phase = ?, last_applied_seq = 0, source_max_seq = 0,
                   bootstrap_copied = 0, bootstrap_completed = 0,
                   last_error = NULL,
                   updated_at = datetime('now')
             WHERE id = 1
            """,
            (PHASE_BOOTSTRAP,),
        )
        conn.execute("COMMIT")


def mark_bootstrap_running(path: str, target_seq: int) -> None:
    """首拷正式发布：数据、进度、就绪标记在 *同一个事务* 原子生效。

    只有走到这里（快照 + 日志区间 [1, w1] 全部重放完毕）才允许：
      * bootstrap_completed -> 1（首拷完成的唯一权威凭证）；
      * phase -> RUNNING（开始接受查询）；
      * last_applied_seq/source_max_seq -> target_seq。
    进程在本事务提交前的任意时刻退出，标记都保持 0，重启即保守重做。
    """
    with open_replica(path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """
            UPDATE sync_meta
               SET phase = ?, last_applied_seq = ?,
                   source_max_seq = ?, bootstrap_completed = 1,
                   last_error = NULL,
                   updated_at = datetime('now')
             WHERE id = 1
            """,
            (PHASE_RUNNING, target_seq, target_seq),
        )
        conn.execute("COMMIT")

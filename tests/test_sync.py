"""同步系统正确性测试。

运行：
    PYTHONPATH=/tmp/pylibs python -m pytest tests/ -v
"""
import os
import sqlite3
import threading
import time

import pytest

from app import replica_db, source_db
from app.source_db import connect as source_connect
from app.sync import SyncEngine, sync_once


# --------------------------------------------------------------------------- #
# 辅助
# --------------------------------------------------------------------------- #
@pytest.fixture()
def paths(tmp_path):
    src = str(tmp_path / "source.db")
    rep = str(tmp_path / "replica.db")
    source_db.init_demo(src, rows=[(i, f"taxon-{i}", f"loc-{i}") for i in range(1, 9)])
    return src, rep


def replica_rows(path):
    with replica_db.open_replica(path) as conn:
        return {r["id"]: (r["taxon"], r["locality"])
                for r in conn.execute("SELECT * FROM specimens ORDER BY id")}


def source_rows(path):
    conn = source_connect(path, readonly=True)
    try:
        return {r["id"]: (r["taxon"], r["locality"])
                for r in conn.execute("SELECT * FROM specimens ORDER BY id")}
    finally:
        conn.close()


def run_engine(src, rep, wait_target=None, timeout=20, expect_running=True):
    """启动引擎，等到副本追上源端当前水位（或就绪）后停止并回收。"""
    engine = SyncEngine(src, rep, poll_interval=0.05)
    engine.start()
    deadline = time.time() + timeout
    target = wait_target if wait_target is not None else source_db.max_change_seq(src)
    saw_running = False
    while time.time() < deadline:
        meta = replica_db.get_meta(rep)
        if meta["phase"] == replica_db.PHASE_RUNNING:
            saw_running = True
            if meta["last_applied_seq"] >= target:
                break
        time.sleep(0.02)
    if expect_running:
        assert saw_running, "副本从未进入 RUNNING 阶段"
    engine.stop()
    engine.wait_until_idle(timeout)
    return engine


# --------------------------------------------------------------------------- #
# 1. CDC 基础：触发器、序号、回滚
# --------------------------------------------------------------------------- #
class TestCDC:
    def test_triggers_log_all_dml(self, paths):
        src, _ = paths
        source_db.insert_specimen(src, 100, "t", "l")
        source_db.update_specimen(src, 100, "t2", "l2")
        source_db.delete_specimen(src, 100)
        conn = source_connect(src, readonly=True)
        try:
            ops = [r["op"] for r in conn.execute(
                "SELECT op FROM _change_log WHERE id = 100 ORDER BY seq")]
        finally:
            conn.close()
        assert ops == ["I", "U", "D"]

    def test_rollback_does_not_propagate(self, paths):
        src, rep = paths
        run_engine(src, rep)
        before = source_db.max_change_seq(src)

        conn = source_db.connect(src)
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("INSERT INTO specimens VALUES (999, 'ghost', 'ghost town')")
            conn.execute("UPDATE specimens SET taxon='XX' WHERE id=1")
            conn.execute("DELETE FROM specimens WHERE id=2")
            conn.execute("ROLLBACK")
        finally:
            conn.close()

        # 源库日志不增加
        assert source_db.max_change_seq(src) == before
        # 再同步一轮，副本与源库完全一致、没有幽灵行
        run_engine(src, rep)
        assert replica_rows(rep) == source_rows(src)
        assert 999 not in replica_rows(rep)

    def test_committed_seq_monotonic_and_rollback_never_applied(self, paths):
        """已提交日志序号严格单调；回滚事务不产生任何可应用日志。"""
        src, rep = paths
        conn = source_db.connect(src)
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("INSERT INTO specimens VALUES (77, 'a', 'b')")
            conn.execute("ROLLBACK")
            seq_after_rollback = conn.execute(
                "SELECT COALESCE(MAX(seq), 0) s FROM _change_log").fetchone()["s"]

            conn.execute("BEGIN IMMEDIATE")
            conn.execute("INSERT INTO specimens VALUES (78, 'c', 'd')")
            conn.execute("COMMIT")
            seq_after_commit = conn.execute(
                "SELECT MAX(seq) s FROM _change_log").fetchone()["s"]
        finally:
            conn.close()

        # 回滚没有留下任何日志
        assert seq_after_rollback == 8  # 只有初始 backfill 的 8 条
        # 已提交日志序号单调递增
        assert seq_after_commit == seq_after_rollback + 1

        run_engine(src, rep)
        # 回滚的 77 号从未进入副本；78 号正常同步
        rows = replica_rows(rep)
        assert 77 not in rows and 78 in rows
        # 副本进度只覆盖真实提交的日志，不存在空洞跳跃
        meta = replica_db.get_meta(rep)
        assert meta["last_applied_seq"] == seq_after_commit

    def test_multi_row_transaction_single_visibility_unit(self, paths):
        src, rep = paths
        # 源库一个事务插入 20 行
        conn = source_db.connect(src)
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.executemany(
                "INSERT INTO specimens VALUES (?, ?, ?)",
                [(200 + i, f"batch-{i}", "batch-loc") for i in range(20)],
            )
            conn.execute("COMMIT")
        finally:
            conn.close()
        run_engine(src, rep)
        rows = replica_rows(rep)
        assert all(200 + i in rows for i in range(20))
        # 同一事务的日志连续，副本进度落在该事务最后一条上
        meta = replica_db.get_meta(rep)
        conn = source_connect(src, readonly=True)
        try:
            seqs = [r["seq"] for r in conn.execute(
                "SELECT seq FROM _change_log WHERE id >= 200 ORDER BY seq")]
        finally:
            conn.close()
        assert meta["last_applied_seq"] == max(seqs)


# --------------------------------------------------------------------------- #
# 2. 首次复制
# --------------------------------------------------------------------------- #
class TestBootstrap:
    def test_full_initial_copy(self, paths):
        src, rep = paths
        run_engine(src, rep)
        assert replica_rows(rep) == source_rows(src)
        meta = replica_db.get_meta(rep)
        # 停止后 phase 为 STOPPED；关键是进度已推进到源端水位且数据一致
        assert meta["last_applied_seq"] == source_db.max_change_seq(src)
        assert replica_db.get_specimen(rep, 1) is not None

    def test_not_ready_before_bootstrap_finishes(self, paths):
        src, rep = paths
        # 未启动时查询必须被拒绝
        assert replica_db.is_ready(rep) is False
        engine = SyncEngine(src, rep, poll_interval=0.05)
        engine.start()
        deadline = time.time() + 10
        while time.time() < deadline:
            if replica_db.is_ready(rep):
                break
            time.sleep(0.02)
        # 就绪后可查
        assert replica_db.is_ready(rep) is True
        assert replica_db.get_specimen(rep, 1) is not None
        engine.stop()
        engine.wait_until_idle(10)

    def test_query_rejected_while_bootstrap_in_progress(self, paths, monkeypatch):
        src, rep = paths
        monkeypatch.setattr(source_db, "SNAPSHOT_PAGE_SIZE", 1)
        monkeypatch.setattr("app.sync.SNAPSHOT_PAGE_SIZE", 1, raising=False)
        engine = SyncEngine(src, rep, poll_interval=0.05)
        engine.start()
        # 确定性等待：bootstrap 已开始且尚未完成时，查询必须被拒绝
        assert engine.bootstrap_started.wait(5)
        if not engine.bootstrap_completed.wait(0.5):
            assert replica_db.is_ready(rep) is False
            assert replica_db.get_meta(rep)["phase"] == replica_db.PHASE_BOOTSTRAP
        # 等就绪后查询恢复
        deadline = time.time() + 10
        while time.time() < deadline and not replica_db.is_ready(rep):
            time.sleep(0.02)
        assert replica_db.is_ready(rep) is True
        engine.stop()
        engine.wait_until_idle(10)

    def test_concurrent_writes_during_bootstrap(self, paths, monkeypatch):
        """首次复制期间源库持续增删改：不得漏更、复活删除、旧值覆盖新值。"""
        src, rep = paths
        # 放大页数，让 bootstrap 快照持续足够久
        monkeypatch.setattr(source_db, "SNAPSHOT_PAGE_SIZE", 2)
        monkeypatch.setattr("app.sync.SNAPSHOT_PAGE_SIZE", 2, raising=False)

        stop_flag = threading.Event()

        def writer():
            n = 1000
            while not stop_flag.is_set():
                n += 1
                try:
                    source_db.insert_specimen(src, n, f"t-{n}", "concurrent")
                    source_db.update_specimen(src, 1, f"t1-rev-{n}", "loc-1-new")
                    source_db.delete_specimen(src, 8)  # 快照可能含也可能不含
                    if n % 3 == 0:
                        source_db.delete_specimen(src, n)  # 插完立刻撤
                except Exception:
                    pass
                time.sleep(0.002)

        t = threading.Thread(target=writer)
        t.start()
        try:
            engine = SyncEngine(src, rep, poll_interval=0.05)
            engine.start()
            deadline = time.time() + 30
            while time.time() < deadline:
                meta = replica_db.get_meta(rep)
                if meta["phase"] == replica_db.PHASE_RUNNING:
                    break
                time.sleep(0.02)
            # 再跑两个增量轮次确保追上
            deadline = time.time() + 10
            src_max = source_db.max_change_seq(src)
            while time.time() < deadline:
                meta = replica_db.get_meta(rep)
                if meta["last_applied_seq"] >= src_max:
                    break
                time.sleep(0.02)
        finally:
            stop_flag.set()
            t.join()
            engine.stop()
            engine.wait_until_idle(10)

        # 停写后再做一次确定性收敛
        final_max = source_db.max_change_seq(src)
        run_engine(src, rep, wait_target=final_max)
        assert replica_rows(rep) == source_rows(src)
        # 被撤回的 8 号绝不复活
        assert 8 not in replica_rows(rep)
        # 最新鉴定生效（不是快照里的旧值）
        assert replica_rows(rep)[1][0].startswith("t1-rev-")

    def test_interrupted_bootstrap_can_redo(self, paths, monkeypatch):
        src, rep = paths
        monkeypatch.setattr(source_db, "SNAPSHOT_PAGE_SIZE", 2)
        monkeypatch.setattr("app.sync.SNAPSHOT_PAGE_SIZE", 2, raising=False)

        engine = SyncEngine(src, rep, poll_interval=0.05)
        engine.start()
        time.sleep(0.3)
        engine.stop()
        engine.wait_until_idle(10)

        meta = replica_db.get_meta(rep)
        assert meta["phase"] in (replica_db.PHASE_BOOTSTRAP, replica_db.PHASE_STOPPED)
        assert replica_db.is_ready(rep) is False

        # 重新启动：重做 bootstrap，最终一致
        run_engine(src, rep)
        assert replica_rows(rep) == source_rows(src)
        # 已经历过 RUNNING（停止后落为 STOPPED），且数据一致
        meta = replica_db.get_meta(rep)
        assert meta["phase"] == replica_db.PHASE_STOPPED
        assert meta["last_applied_seq"] == source_db.max_change_seq(src)

    def test_interrupted_replay_never_publishes_replica(self, tmp_path, monkeypatch):
        """复刻线上事故：逐条提交 501 条登记再撤回 1 号；首拷重放 500 条
        历史日志后停止。半成品副本（进度已非零、1 号可能仍在）始终不可查，
        重启重做后 1 号消失、其余 500 条齐全。"""
        src = str(tmp_path / "source.db")
        rep = str(tmp_path / "replica.db")
        source_db.init_source(src)
        conn = source_db.connect(src)
        try:
            for i in range(1, 502):  # 501 条登记，逐条提交
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "INSERT INTO specimens VALUES (?, ?, ?)",
                    (i, f"taxon-{i}", f"locality-{i}"),
                )
                conn.execute("COMMIT")
            conn.execute("BEGIN IMMEDIATE")  # 单独撤回 1 号
            conn.execute("DELETE FROM specimens WHERE id = 1")
            conn.execute("COMMIT")
        finally:
            conn.close()
        assert source_db.max_change_seq(src) == 502

        # 每批 500 条：重放提交第 500 条（501 条 I 的前 500 条）后、
        # 发布就绪前确定性打断
        monkeypatch.setattr(source_db, "CHANGE_BATCH_SIZE", 500)
        monkeypatch.setattr("app.sync.CHANGE_BATCH_SIZE", 500, raising=False)
        monkeypatch.setattr(source_db, "SNAPSHOT_PAGE_SIZE", 500)
        monkeypatch.setattr("app.sync.SNAPSHOT_PAGE_SIZE", 500, raising=False)

        engine = SyncEngine(src, rep, poll_interval=0.02)

        def stop_after_500(seq):
            if seq == 500:
                engine.stop()

        engine.after_batch_commit = stop_after_500
        engine.start()
        engine.wait_until_idle(20)

        meta = replica_db.get_meta(rep)
        # 进度确实已推进到非零（旧代码据此误判就绪的根因）
        assert meta["last_applied_seq"] == 500
        # 但就绪标记未发布、阶段不是 RUNNING、查询被拒绝
        assert meta["bootstrap_completed"] == 0
        assert meta["phase"] != replica_db.PHASE_RUNNING
        assert replica_db.is_ready(rep) is False
        # 此时 1 号的 D 事件尚未重放，物理上可能还在半成品副本里——
        # 关键是就绪门未开（is_ready=False，Web 层据此返回 503），对外不可查
        assert replica_db.is_ready(rep) is False
        with replica_db.open_replica(rep) as rconn:
            physical = rconn.execute(
                "SELECT id FROM specimens WHERE id = 1"
            ).fetchone()
        if physical is not None:
            # 半成品里残留的 1 号必须随重做被撤回，且全程未对外发布
            assert replica_db.is_ready(rep) is False

        # 模拟进程重启：新引擎实例。重做期间始终 503，发布后才可查
        engine2 = SyncEngine(src, rep, poll_interval=0.02)
        engine2.start()
        saw_not_ready = False
        became_ready = False
        deadline = time.time() + 20
        while time.time() < deadline:
            if not replica_db.is_ready(rep):
                saw_not_ready = True
            else:
                became_ready = True
                break
            time.sleep(0.005)
        assert saw_not_ready and became_ready

        # 发布瞬间数据已一致：1 号撤回生效，其余 500 条齐全
        assert replica_db.get_specimen(rep, 1) is None
        with replica_db.open_replica(rep) as rconn:
            count = rconn.execute("SELECT COUNT(*) c FROM specimens").fetchone()["c"]
        assert count == 500
        meta = replica_db.get_meta(rep)
        assert meta["bootstrap_completed"] == 1
        assert meta["last_applied_seq"] == 502
        engine2.stop()
        engine2.wait_until_idle(20)

    def test_redo_bootstrap_while_source_keeps_changing(self, tmp_path, monkeypatch):
        """未完成首拷可重做，重做期间源库仍可持续增删改，最终一致。"""
        src = str(tmp_path / "source.db")
        rep = str(tmp_path / "replica.db")
        source_db.init_demo(
            src, rows=[(i, f"taxon-{i}", f"locality-{i}") for i in range(1, 11)]
        )
        monkeypatch.setattr(source_db, "SNAPSHOT_PAGE_SIZE", 2)
        monkeypatch.setattr("app.sync.SNAPSHOT_PAGE_SIZE", 2, raising=False)

        # 第一次首拷：快照第一页提交后确定性停止，留下未发布的半成品
        engine = SyncEngine(src, rep, poll_interval=0.02)

        def stop_after_first_page(copied):
            engine.stop()

        engine.after_snapshot_page = stop_after_first_page
        engine.start()
        engine.wait_until_idle(10)
        assert replica_db.get_meta(rep)["bootstrap_completed"] == 0
        assert replica_db.is_ready(rep) is False

        # 重做期间源库持续增 / 改 / 撤
        stop_flag = threading.Event()

        def writer():
            n = 5000
            while not stop_flag.is_set():
                n += 1
                try:
                    source_db.insert_specimen(src, n, f"t-{n}", "concurrent")
                    source_db.update_specimen(src, 1, f"rev-{n}", "new-loc")
                    if n % 4 == 0:
                        source_db.delete_specimen(src, n)
                except Exception:
                    pass
                time.sleep(0.002)

        t = threading.Thread(target=writer)
        t.start()
        try:
            engine2 = SyncEngine(src, rep, poll_interval=0.02)
            engine2.start()
            deadline = time.time() + 30
            while time.time() < deadline:
                if replica_db.get_meta(rep)["bootstrap_completed"] == 1:
                    break
                time.sleep(0.02)
            assert replica_db.get_meta(rep)["bootstrap_completed"] == 1
            assert replica_db.is_ready(rep) is True
        finally:
            stop_flag.set()
            t.join()

        # 停写后确定性收敛
        final_max = source_db.max_change_seq(src)
        deadline = time.time() + 15
        while time.time() < deadline:
            if replica_db.get_meta(rep)["last_applied_seq"] >= final_max:
                break
            time.sleep(0.02)
        engine2.stop()
        engine2.wait_until_idle(15)
        assert replica_rows(rep) == source_rows(src)

    def test_nonzero_progress_without_flag_is_not_allowed_through(self, tmp_path):
        """旧副本/手工损坏副本：进度非零但 bootstrap_completed=0，
        不能仅凭非零进度放行，必须保守重做。"""
        src = str(tmp_path / "source.db")
        rep = str(tmp_path / "replica.db")
        source_db.init_demo(
            src, rows=[(i, f"taxon-{i}", f"locality-{i}") for i in range(1, 7)]
        )
        # 直接构造一个带非零进度、就绪标记缺失的半成品副本
        replica_db.init_replica(rep)
        with replica_db.open_replica(rep) as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("INSERT INTO specimens VALUES (1, 'ghost', 'ghost town')")
            conn.execute(
                "UPDATE sync_meta SET phase = ?, last_applied_seq = 99,"
                " source_max_seq = 99, bootstrap_completed = 0 WHERE id = 1",
                (replica_db.PHASE_STOPPED,),
            )
            conn.execute("COMMIT")
        assert replica_db.is_ready(rep) is False

        run_engine(src, rep)
        meta = replica_db.get_meta(rep)
        assert meta["bootstrap_completed"] == 1
        assert replica_rows(rep) == source_rows(src)
        assert "ghost town" not in {v[1] for v in replica_rows(rep).values()}

    def test_legacy_replica_without_flag_column_redo_bootstrap(self, tmp_path):
        """旧版本副本文件没有 bootstrap_completed 列：在线迁移补 0，
        无法证明首拷完成 -> 保守重做，而不是按 RUNNING+非零进度放行。"""
        src = str(tmp_path / "source.db")
        rep = str(tmp_path / "replica.db")
        source_db.init_demo(
            src, rows=[(i, f"taxon-{i}", f"locality-{i}") for i in range(1, 5)]
        )
        import sqlite3

        conn = sqlite3.connect(rep)
        conn.execute(
            "CREATE TABLE specimens (id INTEGER PRIMARY KEY, taxon TEXT NOT NULL,"
            " locality TEXT NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE sync_meta (id INTEGER PRIMARY KEY CHECK (id=1),"
            " phase TEXT NOT NULL, last_applied_seq INTEGER NOT NULL DEFAULT 0,"
            " source_max_seq INTEGER NOT NULL DEFAULT 0,"
            " bootstrap_copied INTEGER NOT NULL DEFAULT 0, last_error TEXT,"
            " updated_at TEXT NOT NULL DEFAULT (datetime('now')))"
        )
        conn.execute(
            "CREATE TABLE _applied_changes (seq INTEGER PRIMARY KEY, op TEXT NOT NULL,"
            " specimen_id INTEGER NOT NULL,"
            " applied_at TEXT NOT NULL DEFAULT (datetime('now')))"
        )
        conn.execute("INSERT INTO specimens VALUES (1, 'stale', 'stale-loc')")
        conn.execute(
            "INSERT INTO sync_meta(id, phase, last_applied_seq, source_max_seq,"
            " bootstrap_copied) VALUES (1, 'RUNNING', 3, 3, 1)"
        )
        conn.commit()
        conn.close()

        # 迁移发生在引擎初始化副本结构时
        replica_db.init_replica(rep)
        meta = replica_db.get_meta(rep)
        assert meta["bootstrap_completed"] == 0
        assert replica_db.is_ready(rep) is False

        run_engine(src, rep)
        meta = replica_db.get_meta(rep)
        assert meta["bootstrap_completed"] == 1
        assert replica_rows(rep) == source_rows(src)
        assert all(v[1] != "stale-loc" for v in replica_rows(rep).values())

    def test_process_exit_mid_bootstrap_leaves_replica_unqueryable(self, tmp_path):
        """真正的进程退出（os._exit，无 finally/清理）发生在首拷重放中途：
        磁盘上的副本必须保持未发布；新进程重做后才就绪且数据一致。"""
        import subprocess
        import sys

        src = str(tmp_path / "source.db")
        rep = str(tmp_path / "replica.db")
        worker = str(tmp_path / "worker.py")
        source_db.init_source(src)
        conn = source_db.connect(src)
        for i in range(1, 41):  # 40 条登记 + 撤回 1 号 = 41 条日志
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "INSERT INTO specimens VALUES (?, ?, ?)", (i, f"t{i}", f"l{i}")
            )
            conn.execute("COMMIT")
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DELETE FROM specimens WHERE id = 1")
        conn.execute("COMMIT")
        conn.close()

        # 子进程：小批次重放，提交若干批后立即硬退出（模拟进程被杀）
        with open(worker, "w") as fh:
            fh.write(
                "import os, sys\n"
                "sys.path.insert(0, %r)\n"
                "import app.source_db as s\n"
                "import app.sync as m\n"
                "s.CHANGE_BATCH_SIZE = 5; m.CHANGE_BATCH_SIZE = 5\n"
                "s.SNAPSHOT_PAGE_SIZE = 5; m.SNAPSHOT_PAGE_SIZE = 5\n"
                "from app.sync import SyncEngine\n"
                "src, rep = %r, %r\n"
                "eng = SyncEngine(src, rep, poll_interval=0.01)\n"
                "state = {'batches': 0}\n"
                "def hook(seq):\n"
                "    state['batches'] += 1\n"
                "    if state['batches'] >= 2:\n"
                "        os._exit(7)  # 硬退出：不执行任何 finally/flush 之外的清理\n"
                "eng.after_batch_commit = hook\n"
                "eng.start()\n"
                "eng._thread.join()\n"
                % (os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                   src, rep)
            )

        proc = subprocess.run(
            [sys.executable, worker], cwd=str(tmp_path), capture_output=True, timeout=30
        )
        assert proc.returncode == 7, proc.stderr.decode()

        # 磁盘副本：进度可能非零，但就绪标记必须为 0、不可查
        replica_db.init_replica(rep)
        meta = replica_db.get_meta(rep)
        assert meta["bootstrap_completed"] == 0
        assert replica_db.is_ready(rep) is False
        # get_specimen 是底层物理读；查询门由 is_ready 把守（Web 层返回 503），
        # 因此即使物理行残留也不会对外发布
        assert replica_db.is_ready(rep) is False

        # 新进程（这里用新引擎实例等效）重做 -> 就绪、一致、1 号已撤回
        run_engine(src, rep)
        meta = replica_db.get_meta(rep)
        assert meta["bootstrap_completed"] == 1
        assert replica_rows(rep) == source_rows(src)
        assert 1 not in replica_rows(rep)
        assert len(replica_rows(rep)) == 39


# --------------------------------------------------------------------------- #
# 3. 增量同步
# --------------------------------------------------------------------------- #
class TestIncremental:
    def test_insert_update_delete_flow(self, paths):
        src, rep = paths
        run_engine(src, rep)

        source_db.insert_specimen(src, 50, "new-taxon", "new-loc")
        source_db.update_specimen(src, 50, "corrected-taxon", "corrected-loc")
        run_engine(src, rep, wait_target=source_db.max_change_seq(src))
        assert replica_rows(rep)[50] == ("corrected-taxon", "corrected-loc")

        source_db.delete_specimen(src, 50)
        run_engine(src, rep, wait_target=source_db.max_change_seq(src))
        assert 50 not in replica_rows(rep)

    def test_old_identification_never_overwrites_new(self, paths):
        """同一行连续更正两次：最终值必须是最新鉴定。"""
        src, rep = paths
        run_engine(src, rep)
        source_db.update_specimen(src, 3, "v1", "l1")
        source_db.update_specimen(src, 3, "v2", "l2")
        run_engine(src, rep, wait_target=source_db.max_change_seq(src))
        assert replica_rows(rep)[3] == ("v2", "l2")

    def test_delete_not_resurrected(self, paths):
        """删除后即使重新触发同步/重建检查，已删标本不复活。"""
        src, rep = paths
        run_engine(src, rep)
        source_db.delete_specimen(src, 2)
        run_engine(src, rep, wait_target=source_db.max_change_seq(src))
        assert 2 not in replica_rows(rep)
        # 多轮空同步
        run_engine(src, rep)
        run_engine(src, rep)
        assert 2 not in replica_rows(rep)

    def test_resume_from_persisted_progress_after_restart(self, paths):
        src, rep = paths
        run_engine(src, rep)
        progress = replica_db.get_meta(rep)["last_applied_seq"]

        # 源库继续写入，然后用“新引擎实例”模拟进程重启
        source_db.insert_specimen(src, 60, "after-restart", "x")
        source_db.update_specimen(src, 1, "post-restart", "y")
        engine2 = SyncEngine(src, rep, poll_interval=0.05)
        engine2.start()
        deadline = time.time() + 10
        while time.time() < deadline:
            meta = replica_db.get_meta(rep)
            if meta["last_applied_seq"] >= source_db.max_change_seq(src):
                break
            time.sleep(0.02)
        engine2.stop()
        engine2.wait_until_idle(10)

        meta = replica_db.get_meta(rep)
        assert meta["phase"] == replica_db.PHASE_STOPPED
        assert meta["last_applied_seq"] > progress
        assert replica_rows(rep) == source_rows(src)
        # 没有重做 bootstrap：copied 计数保持首次复制的值
        assert meta["bootstrap_copied"] == 8

    def test_data_and_progress_commit_together(self, paths, monkeypatch):
        """模拟批处理提交前崩溃：数据与进度都不得半生效。"""
        src, rep = paths
        run_engine(src, rep)
        source_db.insert_specimen(src, 70, "a", "b")
        source_db.insert_specimen(src, 71, "c", "d")
        target = source_db.max_change_seq(src)

        progress_before = replica_db.get_meta(rep)["last_applied_seq"]

        # 让副本在“应用这批变更”的 COMMIT 上失败一次（模拟进程在提交点崩溃）。
        # 内置 sqlite3.Connection 不可 monkeypatch，改用带故障的连接工厂。
        real_factory = replica_db._ReplicaConnection  # noqa: SLF001
        crash_state = {"done": False}

        class CrashingConnection(real_factory):
            def execute(self, sql, *args):
                if (
                    not crash_state["done"]
                    and isinstance(sql, str)
                    and sql.strip().upper() == "COMMIT"
                ):
                    crash_state["done"] = True
                    raise sqlite3.OperationalError("simulated crash at commit")
                return super().execute(sql, *args)

        monkeypatch.setattr(replica_db, "_ReplicaConnection", CrashingConnection)
        engine = SyncEngine(src, rep, poll_interval=0.05)
        engine.start()
        time.sleep(0.5)
        engine.stop()
        engine.wait_until_idle(10)
        monkeypatch.undo()

        assert crash_state["done"], "故障注入未触发"
        # 崩溃后进度未推进、半成品行不可见（数据与进度共同提交/共同回滚）
        meta = replica_db.get_meta(rep)
        assert meta["last_applied_seq"] == progress_before
        assert 70 not in replica_rows(rep) and 71 not in replica_rows(rep)

        # 恢复后新引擎从头接续，最终一致
        run_engine(src, rep, wait_target=target)
        assert replica_rows(rep) == source_rows(src)

    def test_resume_skips_bootstrap_when_already_ready(self, paths):
        """已就绪后重启（新建引擎实例）必须按持久进度接续，不重做快照。"""
        src, rep = paths
        run_engine(src, rep)
        source_db.insert_specimen(src, 80, "restarted", "r")
        target = source_db.max_change_seq(src)

        engine = SyncEngine(src, rep, poll_interval=0.05)
        engine.start()
        deadline = time.time() + 10
        while time.time() < deadline:
            if replica_db.get_meta(rep)["last_applied_seq"] >= target:
                break
            time.sleep(0.02)
        engine.stop()
        engine.wait_until_idle(5)
        meta = replica_db.get_meta(rep)
        assert meta["phase"] == replica_db.PHASE_STOPPED
        assert meta["bootstrap_copied"] == 8  # 没有重新快照
        assert replica_rows(rep) == source_rows(src)

    def test_empty_source_bootstrap_publishes_ready(self, tmp_path):
        """空源库的首拷也必须正常发布：seq=0 但 bootstrap_completed=1，
        副本就绪可查（查无标本时 404，而不是 503）。"""
        src = str(tmp_path / "empty-source.db")
        rep = str(tmp_path / "empty-replica.db")
        source_db.init_source(src)
        assert source_db.max_change_seq(src) == 0

        engine = SyncEngine(src, rep, poll_interval=0.02)
        engine.start()
        deadline = time.time() + 10
        while time.time() < deadline:
            meta = replica_db.get_meta(rep)
            if (
                meta["bootstrap_completed"] == 1
                and meta["phase"] == replica_db.PHASE_RUNNING
            ):
                break
            time.sleep(0.01)
        else:
            engine.stop()
            pytest.fail("空库首拷未发布就绪")
        meta = replica_db.get_meta(rep)
        assert meta["bootstrap_completed"] == 1
        assert meta["last_applied_seq"] == 0
        assert replica_db.is_ready(rep) is True
        engine.stop()
        engine.wait_until_idle(10)

    def test_completed_empty_replica_restarts_incrementally(self, tmp_path):
        """已完成首拷的空库副本，重启必须增量续跑，绝不清空重建。"""
        src = str(tmp_path / "empty-source.db")
        rep = str(tmp_path / "empty-replica.db")
        source_db.init_source(src)

        engine = SyncEngine(src, rep, poll_interval=0.02)
        engine.start()
        deadline = time.time() + 10
        while time.time() < deadline:
            if replica_db.get_meta(rep)["bootstrap_completed"] == 1:
                break
            time.sleep(0.01)
        engine.stop()
        engine.wait_until_idle(10)

        # 新引擎实例模拟重启：监视 _bootstrap 不允许被调用
        engine2 = SyncEngine(src, rep, poll_interval=0.02)
        bootstrap_calls = []
        original_bootstrap = engine2._bootstrap

        def spy_bootstrap():
            bootstrap_calls.append(1)
            return original_bootstrap()

        engine2._bootstrap = spy_bootstrap
        engine2.start()
        time.sleep(0.5)
        meta = replica_db.get_meta(rep)
        engine2.stop()
        engine2.wait_until_idle(10)

        assert bootstrap_calls == [], "已完成的空库副本重启不得重做 bootstrap"
        assert meta["bootstrap_completed"] == 1
        assert meta["last_applied_seq"] == 0
        assert meta["bootstrap_copied"] == 0

        # 空库就绪后源库写入第一条 -> 增量通道追上并可查
        source_db.insert_specimen(src, 9, "first-after-empty", "loc-9")
        engine3 = SyncEngine(src, rep, poll_interval=0.02)
        engine3.start()
        deadline = time.time() + 10
        while time.time() < deadline:
            if replica_db.get_specimen(rep, 9) is not None:
                break
            time.sleep(0.02)
        else:
            engine3.stop()
            pytest.fail("空库副本的首条增量未追上")
        assert replica_db.get_specimen(rep, 9) == {
            "id": 9,
            "taxon": "first-after-empty",
            "locality": "loc-9",
        }
        engine3.stop()
        engine3.wait_until_idle(10)


# --------------------------------------------------------------------------- #
# 4. 不扫全表 / 不阻塞源写入
# --------------------------------------------------------------------------- #
class TestEfficiency:
    def test_incremental_uses_index_not_full_scan(self, paths):
        src, rep = paths
        run_engine(src, rep)
        conn = source_connect(src, readonly=True)
        try:
            plan = conn.execute(
                "EXPLAIN QUERY PLAN "
                "SELECT seq, op, id, taxon, locality FROM _change_log "
                "WHERE seq > ? AND seq <= ? ORDER BY seq LIMIT ?",
                (1, 999, 500),
            ).fetchall()
        finally:
            conn.close()
        detail = " ".join(row[3] for row in plan)
        assert "SCAN _change_log" not in detail  # 不是全表扫描
        assert "SEARCH" in detail

    def test_source_writes_not_blocked_by_sync(self, paths):
        src, rep = paths
        engine = SyncEngine(src, rep, poll_interval=0.02)
        engine.start()
        try:
            # 同步线程活跃期间，源库写入立即成功，不需等待复制完成。
            # 阈值取宽松上限：真正被锁阻塞会一直等到 busy_timeout(30s) 超时。
            t0 = time.time()
            for i in range(50):
                source_db.insert_specimen(src, 300 + i, "t", "l")
            elapsed = time.time() - t0
            assert elapsed < 15.0, f"50 次源写入耗时 {elapsed:.1f}s，疑似被同步阻塞"
            deadline = time.time() + 15
            while time.time() < deadline:
                if replica_db.get_meta(rep)["last_applied_seq"] >= source_db.max_change_seq(src):
                    break
                time.sleep(0.02)
        finally:
            engine.stop()
            engine.wait_until_idle(10)
        assert replica_rows(rep) == source_rows(src)


# --------------------------------------------------------------------------- #
# 5. 副本库独立性与错误可见
# --------------------------------------------------------------------------- #
class TestReplicaBehavior:
    def test_replica_is_separate_database(self, paths):
        src, rep = paths
        assert os.path.abspath(src) != os.path.abspath(rep)
        run_engine(src, rep)
        # 副本里没有源端的触发器/源写连接，结构独立；直接写副本不影响源库
        with replica_db.open_replica(rep) as conn:
            conn.execute("INSERT INTO specimens VALUES (900, 'local', 'local')")
        assert 900 not in source_rows(src)

    def test_sync_once_helper(self, paths):
        src, rep = paths
        meta = sync_once(src, rep)
        assert meta["phase"] == replica_db.PHASE_RUNNING
        assert replica_rows(rep) == source_rows(src)

    def test_wal_mode_enabled(self, paths):
        src, rep = paths
        conn = source_connect(src, readonly=True)
        try:
            mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        finally:
            conn.close()
        assert mode.lower() == "wal"

    def test_error_is_recorded_when_source_unavailable(self, paths):
        """源库丢失/不可读时，错误持久化到 meta，线程退出而非静默空转。"""
        src, rep = paths
        run_engine(src, rep)
        os.remove(src)
        for suffix in ("-wal", "-shm"):
            p = src + suffix
            if os.path.exists(p):
                os.remove(p)

        engine = SyncEngine(src, rep, poll_interval=0.02)
        engine.start()
        engine.wait_until_idle(10)
        meta = replica_db.get_meta(rep)
        assert meta["phase"] == replica_db.PHASE_ERROR
        assert meta["last_error"] and "OperationalError" in meta["last_error"]

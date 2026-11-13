"""Web 控制台端到端测试（Flask test client + 真实同步线程）。"""
import os
import time

import pytest

# 必须在导入 app.web 前指向临时目录，避免模块级 app 使用默认 /data 路径
os.environ.setdefault("SOURCE_DB", "/tmp/museum-test-webdb/source.db")
os.environ.setdefault("REPLICA_DB", "/tmp/museum-test-webdb/replica.db")

from app import replica_db, source_db  # noqa: E402
from app.sync import SyncEngine  # noqa: E402
from app.web import create_app  # noqa: E402


@pytest.fixture()
def app_paths(tmp_path):
    src = str(tmp_path / "source.db")
    rep = str(tmp_path / "replica.db")
    source_db.init_demo(src, rows=[(i, f"taxon-{i}", f"loc-{i}") for i in range(1, 6)])
    return src, rep


@pytest.fixture()
def client(app_paths):
    src, rep = app_paths
    app = create_app(src, rep, poll_interval=0.05, auto_start=False)
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c, app.config["SYNC_ENGINE"]


def wait_phase(engine, predicate, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate(replica_db.get_meta(engine.replica_path)):
            return True
        time.sleep(0.02)
    return False


class TestWebAPI:
    def test_healthz(self, client):
        c, _ = client
        assert c.get("/healthz").status_code == 200

    def test_query_503_before_ready(self, client):
        c, _ = client
        r = c.get("/api/specimen/1")
        assert r.status_code == 503
        assert r.get_json()["error"] == "REPLICA_NOT_READY"

    def test_full_flow_start_query_and_demo_writes(self, client):
        c, engine = client

        # 启动同步并等待就绪
        r = c.post("/api/sync/start")
        assert r.status_code == 200 and r.get_json()["started"] is True
        assert wait_phase(engine, lambda m: m["phase"] == replica_db.PHASE_RUNNING)

        # 就绪后查询副本
        r = c.get("/api/specimen/3")
        assert r.status_code == 200
        body = r.get_json()
        assert body == {"id": 3, "taxon": "taxon-3", "locality": "loc-3"}

        # 不存在的标本 -> 404
        assert c.get("/api/specimen/9999").status_code == 404

        # 通过网页在源库新增 -> 副本自动追上
        r = c.post("/api/demo/insert", json={
            "id": 100, "taxon": "Web taxon", "locality": "Web locality"})
        assert r.status_code == 200
        deadline = time.time() + 10
        while time.time() < deadline:
            got = c.get("/api/specimen/100")
            if got.status_code == 200:
                assert got.get_json()["taxon"] == "Web taxon"
                break
            time.sleep(0.05)
        else:
            pytest.fail("新增标本未同步到副本")

        # 通过网页更正 -> 副本以新鉴定覆盖
        r = c.post("/api/demo/update", json={
            "id": 100, "taxon": "Corrected taxon", "locality": "Corrected loc"})
        assert r.status_code == 200
        deadline = time.time() + 10
        while time.time() < deadline:
            got = c.get("/api/specimen/100").get_json()
            if got["taxon"] == "Corrected taxon":
                break
            time.sleep(0.05)
        else:
            pytest.fail("更正未同步到副本")

        # 通过网页撤回 -> 副本删除且 404
        assert c.post("/api/demo/delete", json={"id": 100}).status_code == 200
        deadline = time.time() + 10
        while time.time() < deadline:
            if c.get("/api/specimen/100").status_code == 404:
                break
            time.sleep(0.05)
        else:
            pytest.fail("撤回未传播到副本")

        # 停止
        r = c.post("/api/sync/stop")
        assert r.status_code == 200
        engine.wait_until_idle(10)

    def test_status_reports_progress_and_errors(self, client):
        c, engine = client
        c.post("/api/sync/start")
        assert wait_phase(engine, lambda m: m["phase"] == replica_db.PHASE_RUNNING)
        r = c.get("/api/status")
        assert r.status_code == 200
        st = r.get_json()
        assert st["phase"] == replica_db.PHASE_RUNNING
        assert st["last_applied_seq"] >= 5
        assert st["source"]["specimen_count"] == 5
        assert st["worker_alive"] is True
        c.post("/api/sync/stop")
        engine.wait_until_idle(10)

    def test_start_twice_is_idempotent(self, client):
        c, engine = client
        assert c.post("/api/sync/start").get_json()["started"] is True
        assert c.post("/api/sync/start").get_json()["started"] is False
        c.post("/api/sync/stop")
        engine.wait_until_idle(10)

    def test_source_listing_and_index_page(self, client):
        c, _ = client
        rows = c.get("/api/source/specimens").get_json()
        assert len(rows) == 5 and rows[0]["id"] == 1
        page = c.get("/")
        assert page.status_code == 200
        assert "标本登记库".encode() in page.data

    def test_demo_errors(self, client):
        c, engine = client
        c.post("/api/sync/start")
        wait_phase(engine, lambda m: m["phase"] == replica_db.PHASE_RUNNING)
        # 主键冲突
        r = c.post("/api/demo/insert", json={
            "id": 1, "taxon": "dup", "locality": "dup"})
        assert r.status_code == 409
        # 更正不存在的标本
        r = c.post("/api/demo/update", json={
            "id": 8888, "taxon": "x", "locality": "y"})
        assert r.status_code == 404
        # 撤回不存在的标本
        r = c.post("/api/demo/delete", json={"id": 8888})
        assert r.status_code == 404
        c.post("/api/sync/stop")
        engine.wait_until_idle(10)


class TestBootstrapRecoveryOverHTTP:
    """覆盖事故链路：首拷重放中断 -> 重启期间查询始终 503 -> 重做后恢复。"""

    def test_interrupted_bootstrap_replica_stays_503_across_restart(
        self, app_paths, monkeypatch
    ):
        src, rep = app_paths
        # 足够小的批次，保证首拷重放可以被确定性地拦在发布之前
        monkeypatch.setattr("app.sync.CHANGE_BATCH_SIZE", 2, raising=False)
        monkeypatch.setattr("app.sync.SNAPSHOT_PAGE_SIZE", 2, raising=False)

        app = create_app(src, rep, poll_interval=0.02, auto_start=False)
        app.config["TESTING"] = True
        with app.test_client() as c:
            engine = app.config["SYNC_ENGINE"]

            def stop_on_first_batch(seq):
                engine.stop()

            engine.after_batch_commit = stop_on_first_batch
            r = c.post("/api/sync/start")
            assert r.status_code == 200
            engine.wait_until_idle(10)

            meta = replica_db.get_meta(rep)
            assert meta["bootstrap_completed"] == 0
            assert replica_db.is_ready(rep) is False
            # 中断后查询接口必须 503，而不是把半成品副本暴露出去
            r = c.get("/api/specimen/1")
            assert r.status_code == 503
            assert r.get_json()["error"] == "REPLICA_NOT_READY"

            # 模拟“重启便开放查询”：再次启动同步。重做完成前持续 503，
            # 绝不允许中途出现 200
            engine.after_batch_commit = None
            c.post("/api/sync/start")
            saw_503 = False
            became_ready = False
            deadline = time.time() + 15
            while time.time() < deadline:
                r = c.get("/api/specimen/1")
                if r.status_code == 503:
                    saw_503 = True
                elif r.status_code == 200:
                    became_ready = True
                    break
                else:
                    pytest.fail(f"重做期间出现意外状态码 {r.status_code}")
                time.sleep(0.005)
            assert saw_503 and became_ready
            assert replica_db.get_meta(rep)["bootstrap_completed"] == 1
            c.post("/api/sync/stop")
            engine.wait_until_idle(10)

    def test_withdrawn_specimen_not_queryable_after_recovery(
        self, app_paths, monkeypatch
    ):
        """完整事故复刻：撤回 1 号 -> 首拷重放中断 -> 重启重做 ->
        1 号在整个过程中从未 200，最终 404。"""
        src, rep = app_paths
        monkeypatch.setattr("app.sync.CHANGE_BATCH_SIZE", 2, raising=False)
        monkeypatch.setattr("app.sync.SNAPSHOT_PAGE_SIZE", 2, raising=False)

        # 先通过源库撤回 1 号（源库初始有 1..5）
        source_db.delete_specimen(src, 1)

        app = create_app(src, rep, poll_interval=0.02, auto_start=False)
        app.config["TESTING"] = True
        with app.test_client() as c:
            engine = app.config["SYNC_ENGINE"]

            def stop_on_first_batch(seq):
                engine.stop()

            engine.after_batch_commit = stop_on_first_batch
            c.post("/api/sync/start")
            engine.wait_until_idle(10)
            assert replica_db.get_meta(rep)["bootstrap_completed"] == 0

            # 中断态：1 号绝不返回 200
            assert c.get("/api/specimen/1").status_code == 503

            engine.after_batch_commit = None
            c.post("/api/sync/start")
            deadline = time.time() + 15
            finalized = False
            while time.time() < deadline:
                # 单一事实来源：只看 HTTP 状态码，避免 is_ready() 与接口分别
                # 读取产生 TOCTOU。未就绪只能 503；就绪后撤回的 1 号必须 404。
                code = c.get("/api/specimen/1").status_code
                assert code in (503, 404), f"重做期间意外状态码 {code}"
                if code == 404:
                    finalized = True
                    break
                time.sleep(0.005)
            assert finalized, "副本就绪后 1 号未返回 404"
            assert replica_db.is_ready(rep)

            # 发布后：1 号已撤回 -> 404；其他标本正常 200
            assert c.get("/api/specimen/1").status_code == 404
            assert c.get("/api/specimen/2").status_code == 200
            c.post("/api/sync/stop")
            engine.wait_until_idle(10)

    def test_completed_replica_restart_keeps_ready_flag(self, app_paths):
        """已完成首拷的副本重启后沿用持久就绪标记，增量续跑、不重做。"""
        src, rep = app_paths
        app = create_app(src, rep, poll_interval=0.02, auto_start=False)
        app.config["TESTING"] = True
        with app.test_client() as c:
            engine = app.config["SYNC_ENGINE"]
            c.post("/api/sync/start")
            assert wait_phase(engine, lambda m: m["bootstrap_completed"] == 1)
            copied = replica_db.get_meta(rep)["bootstrap_copied"]
            c.post("/api/sync/stop")
            engine.wait_until_idle(10)

        # 新 app / 新引擎实例模拟进程重启
        app2 = create_app(src, rep, poll_interval=0.02, auto_start=False)
        app2.config["TESTING"] = True
        with app2.test_client() as c2:
            engine2 = app2.config["SYNC_ENGINE"]
            bootstrap_calls = []
            original = engine2._bootstrap

            def spy():
                bootstrap_calls.append(1)
                return original()

            engine2._bootstrap = spy
            c2.post("/api/sync/start")
            assert wait_phase(
                engine2, lambda m: m["phase"] == replica_db.PHASE_RUNNING
            )
            # 没有重做快照，持久标记与拷贝计数保留
            assert bootstrap_calls == []
            meta = replica_db.get_meta(rep)
            assert meta["bootstrap_completed"] == 1
            assert meta["bootstrap_copied"] == copied
            assert c2.get("/api/specimen/3").status_code == 200
            c2.post("/api/sync/stop")
            engine2.wait_until_idle(10)

    def test_empty_source_replica_publishes_and_survives_restart(self, tmp_path):
        """空库首拷发布就绪；重启增量续跑不清空重建，查询不再 503。"""
        src = str(tmp_path / "empty-source.db")
        rep = str(tmp_path / "empty-replica.db")
        source_db.init_source(src)  # 0 行

        app = create_app(src, rep, poll_interval=0.02, auto_start=False)
        app.config["TESTING"] = True
        with app.test_client() as c:
            engine = app.config["SYNC_ENGINE"]
            # 未启动时 503
            assert c.get("/api/specimen/1").status_code == 503
            c.post("/api/sync/start")
            deadline = time.time() + 10
            while time.time() < deadline:
                if replica_db.get_meta(rep)["bootstrap_completed"] == 1:
                    break
                time.sleep(0.01)
            # 空库就绪：查无标本是 404，而不是 503
            assert c.get("/api/specimen/1").status_code == 404
            c.post("/api/sync/stop")
            engine.wait_until_idle(10)

        app2 = create_app(src, rep, poll_interval=0.02, auto_start=False)
        app2.config["TESTING"] = True
        with app2.test_client() as c2:
            engine2 = app2.config["SYNC_ENGINE"]
            bootstrap_calls = []
            original = engine2._bootstrap

            def spy():
                bootstrap_calls.append(1)
                return original()

            engine2._bootstrap = spy
            c2.post("/api/sync/start")
            time.sleep(0.4)
            assert bootstrap_calls == []
            assert replica_db.get_meta(rep)["bootstrap_completed"] == 1
            assert c2.get("/api/specimen/1").status_code == 404
            c2.post("/api/sync/stop")
            engine2.wait_until_idle(10)

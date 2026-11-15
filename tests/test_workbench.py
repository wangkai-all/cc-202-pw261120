"""工作台核心逻辑测试（标准库 unittest，每个用例使用独立临时数据库）。

运行：python3 -I -m unittest discover -s tests -v
"""
import json
import os
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
import urllib.request
from http.client import RemoteDisconnected

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import db as dbmod
from app import preflight as preflight_mod
from app import service
from app.parser import STRUCTURED_KEYS, parse_address
from app.server import SEED_SAMPLES, build_server


class WorkbenchTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "test.db")
        self.conn = dbmod.get_conn(self.db_path)
        self._runners = []

    def tearDown(self):
        # 必须先停掉可能仍在运行的回填线程，再关闭连接与清理
        for runner in self._runners:
            runner.stop()
            runner.join(timeout=10)
        self.conn.close()
        dbmod._initialized_paths.discard(self.db_path)
        self.tmp.cleanup()

    def seed(self, n=None):
        samples = SEED_SAMPLES[:n] if n else SEED_SAMPLES
        return service.seed_legacy_only(self.conn, samples)

    def run_backfill_sync(self, delay=0.0):
        runner = service.BackfillRunner(self.db_path, delay=delay)
        self._runners.append(runner)
        runner.start()
        runner.join(timeout=30)
        self.assertFalse(runner.is_running())
        return runner


class TestParserDeterminism(unittest.TestCase):
    def test_deterministic_same_input_same_output(self):
        for raw in [s["address_raw"] for s in SEED_SAMPLES]:
            self.assertEqual(parse_address(raw), parse_address(raw))

    def test_never_fabricates_verbatim_fields(self):
        # 除直辖市同城复制外，所有字段值都必须是原文的逐字子串
        for raw in [s["address_raw"] for s in SEED_SAMPLES]:
            r = parse_address(raw)
            flat = raw.replace("，", " ").replace(",", " ")
            for k in STRUCTURED_KEYS:
                if r[k]:
                    self.assertIn(r[k], flat, f"{k}={r[k]} 不是原文片段：{raw}")

    def test_three_states(self):
        self.assertEqual(parse_address("广东省广州市越秀区中山五路33号")["parse_status"], "parsed")
        self.assertEqual(parse_address("浙江杭州西湖区")["parse_status"], "partial")
        self.assertEqual(parse_address("")["parse_status"], "pending")

    def test_free_text_is_pending_or_partial_not_fabricated(self):
        r = parse_address("老地方见，小区门口便利店旁边")
        self.assertIn(r["parse_status"], ("pending", "partial"))
        self.assertIsNone(r["province"])
        self.assertIsNone(r["city"])


class TestDualWrite(WorkbenchTestBase):
    def test_single_transaction_writes_both_projections_once(self):
        out = service.write_customer(
            self.conn, customer_name="测试客户",
            address_raw="江苏省南京市鼓楼区中山北路200号")
        cid = out["id"]
        legacy = self.conn.execute("SELECT * FROM customers WHERE id=?", (cid,)).fetchone()
        new = self.conn.execute("SELECT * FROM customer_addresses WHERE id=?", (cid,)).fetchone()
        self.assertEqual(legacy["version"], 1)
        self.assertEqual(new["source_version"], 1)
        self.assertEqual(new["parse_status"], "parsed")
        # 同一次写入只有一条 journal，且两投影内容都能由同一次解析推出
        journal = self.conn.execute(
            "SELECT COUNT(*) n FROM write_journal WHERE customer_id=?", (cid,)).fetchone()
        self.assertEqual(journal["n"], 1)
        fresh = parse_address(legacy["address_raw"])
        for k in STRUCTURED_KEYS:
            self.assertEqual(new[k], fresh[k])

    def test_no_triggers_in_database(self):
        # 不能依赖触发器顺序：库中不允许存在任何 trigger
        triggers = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger'").fetchall()
        self.assertEqual(triggers, [])

    def test_revision_increments_version_and_reparses(self):
        out = service.write_customer(
            self.conn, customer_name="C", address_raw="123号")
        cid = out["id"]
        out2 = service.write_customer(
            self.conn, customer_name="C", address_raw="上海市浦东新区博云路2号",
            customer_id=cid, expected_version=1)
        self.assertEqual(out2["version"], 2)
        row = service.get_customer(self.conn, cid)
        self.assertEqual(row["version"], 2)
        self.assertEqual(row["source_version"], 2)
        self.assertEqual(row["city"], "上海市")
        self.assertEqual(row["parse_status"], "parsed")

    def test_optimistic_concurrency_one_winner(self):
        out = service.write_customer(
            self.conn, customer_name="C", address_raw="123号")
        cid, base = out["id"], out["version"]
        results = []

        def attempt(addr, delay):
            time.sleep(delay)
            try:
                service.write_customer(
                    dbmod.get_conn(self.db_path), customer_name="C",
                    address_raw=addr, customer_id=cid, expected_version=base)
                results.append(("ok", addr))
            except service.ConflictError:
                results.append(("conflict", addr))

        t1 = threading.Thread(target=attempt, args=("北京市海淀区中关村大街27号", 0.0))
        t2 = threading.Thread(target=attempt, args=("天津市和平区南京路189号", 0.05))
        t1.start(); t2.start(); t1.join(); t2.join()
        statuses = sorted(r[0] for r in results)
        self.assertEqual(statuses, ["conflict", "ok"])
        # 获胜事务之后两投影仍严格同源
        cur = service.get_customer(self.conn, cid)
        self.assertEqual(cur["source_version"], cur["version"])
        cmp = service.compare_customer(self.conn, cid)
        self.assertTrue(cmp["determinism_ok"])

    def test_stale_backfill_snapshot_rejected(self):
        out = service.write_customer(
            self.conn, customer_name="C",
            address_raw="广东省广州市越秀区中山五路33号")
        cid = out["id"]  # v1
        service.write_customer(
            self.conn, customer_name="C",
            address_raw="北京市朝阳区建国路88号", customer_id=cid, expected_version=1)  # v2
        with self.assertRaises(service.ConflictError):
            service.backfill_stale_snapshot(self.conn, cid, stale_version=1)
        cur = service.get_customer(self.conn, cid)
        self.assertEqual(cur["version"], 2)
        self.assertEqual(cur["source_version"], 2)
        self.assertEqual(cur["city"], "北京市")  # 旧回填没有覆盖新修改


class TestBackfill(WorkbenchTestBase):
    def test_checkpoint_progress_and_resume(self):
        self.seed()
        total = self.conn.execute("SELECT COUNT(*) n FROM customers").fetchone()["n"]
        runner = service.BackfillRunner(self.db_path, delay=0.02)
        self._runners.append(runner)
        runner.start()
        # 等处理若干行后优雅中断
        deadline = time.time() + 5
        while time.time() < deadline:
            cp = service.checkpoint(self.conn)
            if cp["processed"] >= 4:
                break
            time.sleep(0.02)
        runner.stop()
        runner.join(timeout=10)
        cp = service.checkpoint(self.conn)
        self.assertEqual(cp["status"], "interrupted")
        mid = cp["last_id"]
        self.assertGreater(mid, 0)
        self.assertLess(mid, total)
        # 续跑直到完成
        runner2 = service.BackfillRunner(self.db_path)
        runner2.start()
        runner2.join(timeout=30)
        cp2 = service.checkpoint(self.conn)
        self.assertEqual(cp2["status"], "done")
        self.assertEqual(cp2["last_id"],
                         self.conn.execute("SELECT MAX(id) m FROM customers").fetchone()["m"])
        # 每条旧记录都有了投影
        self.assertEqual(service.stats(self.conn)["missing_projection"], 0)

    def test_backfill_skips_record_modified_online(self):
        self.seed(n=6)
        runner = service.BackfillRunner(self.db_path, delay=0.15)
        self._runners.append(runner)
        runner.start()
        time.sleep(0.1)  # 让回填先处理 id=1
        # 在回填到达 id=5 之前在线修订它
        c5 = self.conn.execute(
            "SELECT id, version FROM customers WHERE id=5").fetchone()
        service.write_customer(
            self.conn, customer_name="在线抢先修订",
            address_raw="北京市海淀区中关村大街27号 100080",
            customer_id=c5["id"], expected_version=c5["version"])
        runner.join(timeout=30)
        cp = service.checkpoint(self.conn)
        self.assertEqual(cp["status"], "done")
        self.assertGreaterEqual(cp["skipped_newer"], 1)
        rec = service.get_customer(self.conn, 5)
        self.assertEqual(rec["source_version"], 2)
        self.assertEqual(rec["city"], "北京市")  # 回填没有用旧值盖回去
        # 全局不允许出现投影落后于业务版本
        self.assertEqual(service.stats(self.conn)["stale_projection"], 0)

    def test_pending_rows_kept_not_fabricated(self):
        self.seed()
        self.run_backfill_sync()
        bad = self.conn.execute(
            "SELECT c.id, c.address_raw, ca.* FROM customers c"
            " JOIN customer_addresses ca ON ca.id=c.id"
            " WHERE ca.parse_status IN ('pending','partial')").fetchall()
        self.assertGreaterEqual(len(bad), 3)
        for r in bad:
            # 待处理行的结构化字段必须仍能在原文中找到，或为空
            # （detail 只做标点到空格的归一化，比较时同样归一化原文）
            flat = r["address_raw"].replace("，", " ").replace(",", " ")
            for k in STRUCTURED_KEYS:
                self.assertTrue(r[k] is None or r[k] in flat, (r["id"], k, r[k]))

    def test_manual_resolve_does_not_autofill(self):
        self.seed(n=11)  # 第 11 条是 "123号"（partial）
        self.run_backfill_sync()
        target = self.conn.execute(
            "SELECT id FROM customers ORDER BY id LIMIT 1 OFFSET 10").fetchone()["id"]
        service.manual_resolve(self.conn, target, {"city": "测试市", "district": "样本区"})
        rec = service.get_customer(self.conn, target)
        self.assertEqual(rec["parse_status"], "manual")
        self.assertEqual(rec["city"], "测试市")
        self.assertIsNone(rec["province"])  # 未提供，绝不自动补
        self.assertIsNone(rec["street"])


class TestPreflightAndCutover(WorkbenchTestBase):
    def test_preflight_blocks_cutover_until_complete(self):
        self.seed()
        report = preflight_mod.run_preflight(self.conn)
        self.assertFalse(report["ready_to_cutover"])
        fails = {c["name"] for c in report["checks"] if c["status"] == "fail"}
        self.assertIn("投影覆盖率", fails)

        self.run_backfill_sync()
        report2 = preflight_mod.run_preflight(self.conn)
        # 覆盖率/版本/确定性全部通过；不完整地址只产生 warn
        hard = {c["name"]: c for c in report2["checks"] if c["status"] == "fail"}
        self.assertEqual(hard, {})
        warn_names = {c["name"] for c in report2["checks"] if c["status"] == "warn"}
        self.assertIn("待处理地址", warn_names)
        self.assertTrue(report2["ready_to_cutover"])

    def test_key_query_parity(self):
        self.seed()
        self.run_backfill_sync()
        report = preflight_mod.run_preflight(self.conn)
        query_checks = [c for c in report["checks"] if c["name"].startswith("关键查询")]
        self.assertTrue(query_checks)
        for c in query_checks:
            self.assertEqual(c["status"], "pass", c["detail"])

    def test_read_mode_transitions_and_withdraw(self):
        self.seed()
        self.run_backfill_sync()
        self.assertEqual(service.get_read_mode(self.conn), "legacy")
        service.set_read_mode(self.conn, "dual")
        self.assertEqual(service.get_read_mode(self.conn), "dual")
        service.set_read_mode(self.conn, "structured")
        service.set_read_mode(self.conn, "legacy")  # 随时撤回
        self.assertEqual(service.get_read_mode(self.conn), "legacy")
        with self.assertRaises(ValueError):
            service.set_read_mode(self.conn, "unknown")


class TestHTTPEndToEnd(unittest.TestCase):
    """通过真实 HTTP 端口验证页面 API 全链路。"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "http.db")
        self.server = build_server("127.0.0.1", 0, self.db_path)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        # 等待服务内可能仍存活的回填线程结束，避免它在临时目录删除后写库
        from app.server import get_runner
        runner = get_runner(self.db_path)
        runner.stop()
        runner.join(timeout=10)
        self.tmp.cleanup()

    def call(self, path, body=None, method=None):
        if method is None:
            method = "POST" if body is not None else "GET"
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}",
            data=json.dumps(body).encode() if body is not None else None,
            headers={"Content-Type": "application/json"},
            method=method)
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read().decode())

    def test_full_workflow_over_http(self):
        status, body = self.call("/api/seed", {}, method="POST")
        self.assertEqual(status, 200)
        self.assertEqual(body["seeded"], len(SEED_SAMPLES))

        status, _ = self.call("/api/backfill/start", {"delay": 0})
        self.assertEqual(status, 200)
        deadline = time.time() + 15
        state = None
        while time.time() < deadline:
            try:
                _, state = self.call("/api/state")
                if state["checkpoint"]["status"] == "done":
                    break
            except Exception:
                pass
            time.sleep(0.1)
        self.assertIsNotNone(state)
        self.assertEqual(state["checkpoint"]["status"], "done")
        self.assertEqual(state["stats"]["missing_projection"], 0)

        # 并发修订：必有一笔 409
        _, conc = self.call("/api/demo/concurrent", {
            "customer_id": 1, "base_version": 1,
            "first_addr": "北京市海淀区中关村大街27号 100080",
            "second_addr": "天津市和平区南京路189号 300041"})
        self.assertEqual(len(conc["attempts"]), 2)
        self.assertEqual(sum(1 for a in conc["attempts"] if a["won"]), 1)

        # 过期回填被拒
        status, body = self.call("/api/demo/backfill-conflict",
                                 {"customer_id": 1, "stale_version": 1})
        self.assertEqual(status, 409)
        self.assertIn("拒绝", body["error"])

        # 比较 + 核对 + 切换
        status, cmp = self.call("/api/compare?id=1")
        self.assertEqual(status, 200)
        self.assertIn("v1_output", cmp)
        status, pf = self.call("/api/preflight")
        self.assertEqual(status, 200)
        self.assertTrue(pf["ready_to_cutover"])
        status, body = self.call("/api/cutover", {"mode": "structured"})
        self.assertEqual(status, 200)
        status, body = self.call("/api/cutover", {"mode": "legacy"})  # 撤回
        self.assertEqual(body["read_mode"], "legacy")

        # 页面静态资源可访问
        with urllib.request.urlopen(f"http://127.0.0.1:{self.port}/", timeout=5) as r:
            self.assertIn("迁移工作台", r.read().decode())


if __name__ == "__main__":
    unittest.main()

"""本地 Web 工作台 HTTP 服务（仅依赖 Python 标准库）。

启动：python -m app.server
页面：http://127.0.0.1:8080/
"""
from __future__ import annotations

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from . import db as dbmod
from . import preflight as preflight_mod
from . import service

WEB_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "web")

# 迁移开始前的历史样例：包含格式完整与格式不完整的地址
SEED_SAMPLES = [
    {"customer_name": "北京晨光文具厂", "address_raw": "中国北京市朝阳区建国路88号101室 100022"},
    {"customer_name": "上海浦远贸易有限公司", "address_raw": "上海市浦东新区张江高科技园区博云路2号5幢"},
    {"customer_name": "广州越秀餐饮管理", "address_raw": "广东省广州市越秀区中山五路33号"},
    {"customer_name": "杭州西湖茶馆", "address_raw": "浙江杭州西湖区龙井路1号"},
    {"customer_name": "深圳南山科技", "address_raw": "广东省深圳市南山区科技园南区高新南一道9号3单元1202室 518057"},
    {"customer_name": "成都锦江小吃", "address_raw": "四川省成都市锦江区春熙路步行街18号"},
    {"customer_name": "武汉光谷光电子", "address_raw": "湖北省武汉市洪山区珞喻路1037号"},
    {"customer_name": "西安城墙文旅", "address_raw": "陕西省西安市碑林区南大街12号"},
    {"customer_name": "南京金陵书屋", "address_raw": "江苏省南京市鼓楼区中山北路200号"},
    {"customer_name": "重庆山城物流", "address_raw": "重庆市渝中区解放碑民族路188号"},
    {"customer_name": "不完整样例·缺省市区", "address_raw": "123号"},
    {"customer_name": "不完整样例·无街道编号", "address_raw": "浙江杭州西湖区"},
    {"customer_name": "不完整样例·自由文本", "address_raw": "老地方见，小区门口便利店旁边"},
    {"customer_name": "不完整样例·空格式", "address_raw": "N/A"},
    {"customer_name": "不完整样例·只有邮编", "address_raw": "邮编 310000"},
]

_runners: dict[str, service.BackfillRunner] = {}
_runners_lock = threading.Lock()


def get_runner(db_path: str) -> service.BackfillRunner:
    with _runners_lock:
        runner = _runners.get(db_path)
        if runner is None:
            runner = service.BackfillRunner(db_path)
            _runners[db_path] = runner
    return runner


class Handler(BaseHTTPRequestHandler):
    server_version = "MigrationWorkbench/1.0"

    # ---------------------------------------------------------- 工具方法
    def _db_path(self) -> str:
        return self.server.db_path  # type: ignore[attr-defined]

    def _send(self, payload, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if not length:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            raise ValueError("请求体不是合法 JSON")
        if not isinstance(data, dict):
            raise ValueError("请求体必须是 JSON 对象")
        return data

    def log_message(self, fmt, *args):  # 静默默认访问日志，控制台保持干净
        pass

    # ---------------------------------------------------------- 路由
    def do_GET(self):  # noqa: N802
        parsed = urlparse(self.path)
        path, qs = parsed.path, parse_qs(parsed.query)
        conn = dbmod.get_conn(self._db_path())
        try:
            if path in ("/", "/index.html"):
                return self._serve_static("index.html", "text/html; charset=utf-8")
            if path == "/api/state":
                return self._send({
                    "stats": service.stats(conn),
                    "checkpoint": service.checkpoint(conn),
                    "read_mode": service.get_read_mode(conn),
                    "backfill_running": get_runner(self._db_path()).is_running(),
                })
            if path == "/api/customers":
                return self._send({"customers": service.list_customers(conn)})
            if path == "/api/compare":
                if "id" not in qs:
                    return self._send({"error": "缺少 id 参数"}, 400)
                return self._send(service.compare_customer(conn, int(qs["id"][0])))
            if path == "/api/pending":
                rows = conn.execute(
                    "SELECT c.id, c.customer_name, c.address_raw, c.version, ca.parse_status,"
                    " ca.parse_note FROM customers c JOIN customer_addresses ca ON ca.id=c.id"
                    " WHERE ca.parse_status IN ('pending','partial') ORDER BY c.id"
                ).fetchall()
                return self._send({"items": [dict(r) for r in rows]})
            if path == "/api/logs":
                return self._send({
                    "logs": service.recent_logs(conn),
                    "journal": service.recent_journal(conn),
                })
            if path == "/api/preflight":
                return self._send(preflight_mod.run_preflight(conn))
            return self._send({"error": "not found"}, 404)
        except service.NotFoundError as exc:
            return self._send({"error": str(exc)}, 404)
        except Exception as exc:  # noqa: BLE001
            return self._send({"error": f"{type(exc).__name__}: {exc}"}, 500)

    def do_POST(self):  # noqa: N802
        path = urlparse(self.path).path
        conn = dbmod.get_conn(self._db_path())
        runner = get_runner(self._db_path())
        try:
            if path == "/api/reset":
                if runner.is_running():
                    return self._send({"error": "回填运行中，请先停止再重置"}, 409)
                dbmod.reset_database(self._db_path())
                return self._send({"ok": True, "message": "数据库已重置为空"})

            if path == "/api/seed":
                n = service.seed_legacy_only(conn, SEED_SAMPLES)
                return self._send({"ok": True, "seeded": n})

            if path == "/api/backfill/start":
                body = self._body()
                runner.delay = float(body.get("delay", 0.0))
                runner.start()
                return self._send({"ok": True, "message": "回填已启动"})

            if path == "/api/backfill/pause":
                runner.pause()
                return self._send({"ok": True, "message": "已请求暂停（当前行完成后生效）"})

            if path == "/api/backfill/resume":
                runner.resume()
                return self._send({"ok": True, "message": "已继续"})

            if path == "/api/backfill/stop":
                runner.stop()
                runner.join(timeout=5)
                return self._send({"ok": True, "message": "已中断，检查点保留，可继续"})

            if path == "/api/backfill/reset-checkpoint":
                if runner.is_running():
                    return self._send({"error": "回填运行中，不能重置检查点"}, 409)
                service.reset_checkpoint(conn)
                return self._send({"ok": True})

            if path == "/api/customers":
                body = self._body()
                if not body.get("customer_name") or not body.get("address_raw"):
                    return self._send({"error": "customer_name 与 address_raw 必填"}, 400)
                out = service.write_customer(
                    conn,
                    customer_name=str(body["customer_name"]),
                    address_raw=str(body["address_raw"]),
                    actor="online")
                return self._send({"ok": True, **out}, 201)

            if path.startswith("/api/customers/") and path.endswith("/update"):
                cid = int(path.split("/")[3])
                body = self._body()
                if "expected_version" not in body:
                    return self._send({"error": "expected_version 必填（乐观并发）"}, 400)
                out = service.write_customer(
                    conn,
                    customer_name=str(body.get("customer_name", "")),
                    address_raw=str(body.get("address_raw", "")),
                    actor="online",
                    customer_id=cid,
                    expected_version=int(body["expected_version"]))
                return self._send({"ok": True, **out})

            if path.startswith("/api/customers/") and path.endswith("/manual"):
                cid = int(path.split("/")[3])
                body = self._body()
                out = service.manual_resolve(conn, cid, body.get("fields", {}))
                return self._send({"ok": True, **out})

            if path == "/api/demo/concurrent":
                # 并发修订演示：同一记录上两笔基于同一基线版本的修订，只有一笔能赢
                body = self._body()
                cid = int(body["customer_id"])
                base = int(body["base_version"])
                first, second = body["first_addr"], body["second_addr"]
                results = []
                threads = []

                def attempt(label: str, addr: str, order_delay: float):
                    import time as _t
                    _t.sleep(order_delay)
                    try:
                        out = service.write_customer(
                            dbmod.get_conn(self._db_path()),
                            customer_name=body.get("customer_name", "并发修订客户"),
                            address_raw=addr, actor="online",
                            customer_id=cid, expected_version=base)
                        results.append({"label": label, "won": True, "new_version": out["version"]})
                    except service.ConflictError as exc:
                        results.append({"label": label, "won": False, "error": str(exc)})

                threads.append(threading.Thread(target=attempt, args=("修订A", first, 0.0)))
                threads.append(threading.Thread(target=attempt, args=("修订B", second, 0.05)))
                for t in threads:
                    t.start()
                for t in threads:
                    t.join()
                current = service.get_customer(conn, cid)
                return self._send({"attempts": results, "current": current})

            if path == "/api/demo/backfill-conflict":
                # 回填撞在线修改：拿过期快照回填，必须被拒绝
                body = self._body()
                cid = int(body["customer_id"])
                stale = int(body["stale_version"])
                try:
                    out = service.backfill_stale_snapshot(conn, cid, stale)
                    return self._send({"ok": True, "accepted": True, **out})
                except service.ConflictError as exc:
                    return self._send({"ok": True, "accepted": False, "error": str(exc)}, 409)

            if path == "/api/cutover":
                body = self._body()
                mode = body.get("mode", "")
                if mode == "structured":
                    report = preflight_mod.run_preflight(conn)
                    if not report["ready_to_cutover"]:
                        return self._send({
                            "error": "发布前核对存在 fail 项，禁止切换到结构化模式",
                            "preflight": report}, 409)
                service.set_read_mode(conn, mode)
                return self._send({"ok": True, "read_mode": service.get_read_mode(conn)})

            return self._send({"error": "not found"}, 404)
        except service.NotFoundError as exc:
            return self._send({"error": str(exc)}, 404)
        except service.ConflictError as exc:
            return self._send({"error": str(exc)}, 409)
        except ValueError as exc:
            return self._send({"error": str(exc)}, 400)
        except Exception as exc:  # noqa: BLE001
            return self._send({"error": f"{type(exc).__name__}: {exc}"}, 500)

    def _serve_static(self, name: str, content_type: str) -> None:
        target = os.path.join(WEB_DIR, name)
        if not os.path.isfile(target):
            self.send_error(404)
            return
        with open(target, "rb") as fh:
            body = fh.read()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def build_server(host: str = "127.0.0.1", port: int = 8080,
                 db_path: str | None = None) -> ThreadingHTTPServer:
    path = db_path or dbmod.DEFAULT_DB_PATH
    dbmod.get_conn(path)  # 提前建表
    server = ThreadingHTTPServer((host, port), Handler)
    server.db_path = path  # type: ignore[attr-defined]
    return server


def main() -> None:
    host = os.environ.get("HOST", "127.0.0.1")
    port = int(os.environ.get("PORT", "8080"))
    server = build_server(host, port)
    print(f"迁移工作台已启动: http://{host}:{port}/  (数据库 {server.db_path})")  # type: ignore[attr-defined]
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n收到中断信号，关闭服务")
        server.shutdown()


if __name__ == "__main__":
    main()

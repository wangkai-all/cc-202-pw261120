"""领域服务：双投影写入、回填、冲突仲裁、查询与发布开关。

关键不变式：
* 任何写入都在 *一个* 显式事务内同时更新 customers（旧投影）与
  customer_addresses（新投影），地址只解析一次；数据库无触发器，
  因此两套投影同源、同序，不存在触发顺序差异。
* 冲突只认业务版本号 customers.version：回填持有的快照版本落后于当前版本时
  必须跳过，旧回填永远不能覆盖新修改。
"""
from __future__ import annotations

import hashlib
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from . import db as dbmod
from .parser import STRUCTURED_KEYS, parse_address


class NotFoundError(Exception):
    pass


class ConflictError(Exception):
    """乐观版本冲突：期望版本与当前业务版本不一致。"""


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


_write_lock = threading.RLock()


# ---------------------------------------------------------------- 基础工具

def _log(conn, event: str, customer_id: Optional[int], detail: str) -> None:
    conn.execute(
        "INSERT INTO migration_log(ts, event, customer_id, detail) VALUES(?,?,?,?)",
        (now_iso(), event, customer_id, detail),
    )


def _md5(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


# ------------------------------------------------------------ 在线双写

def write_customer(
    conn,
    *,
    customer_name: str,
    address_raw: str,
    actor: str = "online",
    customer_id: Optional[int] = None,
    expected_version: Optional[int] = None,
) -> Dict[str, Any]:
    """新增或修订客户。单事务、解析一次、两投影同写。返回新版本与解析结果。"""
    with _write_lock:
        conn.execute("BEGIN IMMEDIATE")
        try:
            ts = now_iso()
            if customer_id is None:
                cur = conn.execute(
                    "INSERT INTO customers(customer_name, address_raw, version, updated_at)"
                    " VALUES(?,?,1,?)",
                    (customer_name, address_raw, ts),
                )
                cid, version = cur.lastrowid, 1
            else:
                row = conn.execute(
                    "SELECT version FROM customers WHERE id=?", (customer_id,)
                ).fetchone()
                if row is None:
                    raise NotFoundError(f"customer {customer_id} 不存在")
                if expected_version is not None and row["version"] != expected_version:
                    raise ConflictError(
                        f"版本冲突：记录当前为 v{row['version']}，提交基于 v{expected_version}"
                    )
                version = row["version"] + 1
                conn.execute(
                    "UPDATE customers SET customer_name=?, address_raw=?, version=?, updated_at=?"
                    " WHERE id=?",
                    (customer_name, address_raw, version, ts, customer_id),
                )
                cid = customer_id

            parsed = parse_address(address_raw)  # 整次写入只解析这一次
            conn.execute(
                """
                INSERT INTO customer_addresses
                    (id, country, province, city, district, street, house_no,
                     postal_code, detail, parse_status, parse_note, source_version, parsed_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET
                    country=excluded.country, province=excluded.province,
                    city=excluded.city, district=excluded.district,
                    street=excluded.street, house_no=excluded.house_no,
                    postal_code=excluded.postal_code, detail=excluded.detail,
                    parse_status=excluded.parse_status, parse_note=excluded.parse_note,
                    source_version=excluded.source_version, parsed_at=excluded.parsed_at
                """,
                (cid, parsed["country"], parsed["province"], parsed["city"],
                 parsed["district"], parsed["street"], parsed["house_no"],
                 parsed["postal_code"], parsed["detail"], parsed["parse_status"],
                 parsed["parse_note"], version, ts),
            )
            conn.execute(
                "INSERT INTO write_journal(ts, customer_id, version, actor, raw_md5)"
                " VALUES(?,?,?,?,?)",
                (ts, cid, version, actor, _md5(address_raw)),
            )
            _log(conn, "dual_write", cid,
                 f"{actor} 写入 v{version}，解析状态={parsed['parse_status']}")
            conn.execute("COMMIT")
            return {"id": cid, "version": version, "parsed": parsed}
        except Exception:
            conn.execute("ROLLBACK")
            raise


def seed_legacy_only(conn, samples: List[Dict[str, str]]) -> int:
    """模拟迁移开始前的旧库：只写旧投影，新投影留空等待回填。"""
    with _write_lock:
        conn.execute("BEGIN IMMEDIATE")
        try:
            ts = now_iso()
            n = 0
            for s in samples:
                conn.execute(
                    "INSERT INTO customers(customer_name, address_raw, version, updated_at)"
                    " VALUES(?,?,1,?)",
                    (s["customer_name"], s["address_raw"], ts),
                )
                n += 1
            _log(conn, "seed", None, f"导入 {n} 条仅含旧投影的历史样例")
            conn.execute("COMMIT")
            return n
        except Exception:
            conn.execute("ROLLBACK")
            raise


# ------------------------------------------------------------ 历史回填

class BackfillRunner:
    """后台回填线程：逐行提交、检查点随行更新，可暂停/停止，崩溃后可续跑。"""

    def __init__(self, db_path: str, delay: float = 0.0):
        self.db_path = db_path
        self.delay = delay  # 每行处理后的停顿，用于演示与在线写入并发交错
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._pause = threading.Event()

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        conn = dbmod.get_conn(self.db_path)
        cp = conn.execute("SELECT status FROM migration_checkpoint WHERE id=1").fetchone()
        if cp["status"] == "running":
            raise RuntimeError("回填已在运行中")
        if cp["status"] == "done":
            raise RuntimeError("回填已完成，如需重跑请重置检查点")
        self._stop.clear()
        self._pause.clear()
        conn.execute(
            "UPDATE migration_checkpoint SET status='running', started_at=COALESCE(started_at, ?),"
            " updated_at=? WHERE id=1",
            (now_iso(), now_iso()),
        )
        _log(conn, "backfill_start", None, f"回填启动（行间隔 {self.delay}s）")
        self._thread = threading.Thread(target=self._run, name="backfill", daemon=True)
        self._thread.start()

    def pause(self) -> None:
        self._pause.set()

    def resume(self) -> None:
        self._pause.clear()

    def stop(self) -> None:
        """请求优雅中断：当前行事务结束后停下，检查点保留，下次继续。"""
        self._stop.set()
        self._pause.clear()

    def join(self, timeout: Optional[float] = None) -> None:
        if self._thread:
            self._thread.join(timeout)

    def _run(self) -> None:
        conn = dbmod.get_conn(self.db_path)
        try:
            while not self._stop.is_set():
                if self._pause.is_set():
                    conn.execute(
                        "UPDATE migration_checkpoint SET status='paused', updated_at=? WHERE id=1",
                        (now_iso(),))
                    while self._pause.is_set() and not self._stop.is_set():
                        time.sleep(0.1)
                    if self._stop.is_set():
                        break
                    conn.execute(
                        "UPDATE migration_checkpoint SET status='running', updated_at=? WHERE id=1",
                        (now_iso(),))

                with _write_lock:
                    conn.execute("BEGIN IMMEDIATE")
                    try:
                        cp = conn.execute(
                            "SELECT * FROM migration_checkpoint WHERE id=1").fetchone()
                        row = conn.execute(
                            "SELECT id, address_raw, version FROM customers"
                            " WHERE id > ? ORDER BY id LIMIT 1",
                            (cp["last_id"],)).fetchone()
                        if row is None:
                            conn.execute(
                                "UPDATE migration_checkpoint SET status='done', updated_at=? WHERE id=1",
                                (now_iso(),))
                            _log(conn, "backfill_done", None,
                                 f"回填完成，共处理 {cp['processed']} 行，"
                                 f"跳过更新版本 {cp['skipped_newer']} 行")
                            conn.execute("COMMIT")
                            return

                        cid, raw, version = row["id"], row["address_raw"], row["version"]
                        existing = conn.execute(
                            "SELECT source_version FROM customer_addresses WHERE id=?",
                            (cid,)).fetchone()

                        # 冲突仲裁：以业务版本为准，旧回填不得覆盖新修改
                        if existing is not None and existing["source_version"] >= version:
                            conn.execute(
                                "UPDATE migration_checkpoint SET last_id=?, skipped_newer=skipped_newer+1,"
                                " processed=processed+1, updated_at=? WHERE id=1",
                                (cid, now_iso()))
                            _log(conn, "backfill_skip", cid,
                                 f"跳过：新投影已对应 v{existing['source_version']}，"
                                 f"回填快照 v{version} 不更新")
                            conn.execute("COMMIT")
                        else:
                            parsed = parse_address(raw)
                            ts = now_iso()
                            conn.execute(
                                """INSERT INTO customer_addresses
                                    (id, country, province, city, district, street, house_no,
                                     postal_code, detail, parse_status, parse_note, source_version, parsed_at)
                                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                                   ON CONFLICT(id) DO UPDATE SET
                                    country=excluded.country, province=excluded.province,
                                    city=excluded.city, district=excluded.district,
                                    street=excluded.street, house_no=excluded.house_no,
                                    postal_code=excluded.postal_code, detail=excluded.detail,
                                    parse_status=excluded.parse_status, parse_note=excluded.parse_note,
                                    source_version=excluded.source_version, parsed_at=excluded.parsed_at""",
                                (cid, parsed["country"], parsed["province"], parsed["city"],
                                 parsed["district"], parsed["street"], parsed["house_no"],
                                 parsed["postal_code"], parsed["detail"], parsed["parse_status"],
                                 parsed["parse_note"], version, ts))
                            conn.execute(
                                "INSERT INTO write_journal(ts, customer_id, version, actor, raw_md5)"
                                " VALUES(?,?,?, 'backfill', ?)",
                                (ts, cid, version, _md5(raw)))
                            bump = {
                                "parsed": "parsed_cnt=parsed_cnt+1",
                                "partial": "partial_cnt=partial_cnt+1",
                                "pending": "pending_cnt=pending_cnt+1",
                            }[parsed["parse_status"]]
                            conn.execute(
                                f"UPDATE migration_checkpoint SET last_id=?, processed=processed+1,"
                                f" {bump}, updated_at=? WHERE id=1",
                                (cid, now_iso()))
                            _log(conn, "backfill_apply", cid,
                                 f"回填 v{version}，解析状态={parsed['parse_status']}")
                            conn.execute("COMMIT")
                    except Exception:
                        conn.execute("ROLLBACK")
                        raise

                if self.delay:
                    time.sleep(self.delay)
        except Exception as exc:  # 工作线程异常：标记中断，等待续跑
            try:
                conn.execute(
                    "UPDATE migration_checkpoint SET status='interrupted', updated_at=? WHERE id=1",
                    (now_iso(),))
                _log(conn, "backfill_error", None, f"回填中断：{exc!r}")
            except Exception:
                pass
            return

        # 主动停止：检查点原样保留
        conn.execute(
            "UPDATE migration_checkpoint SET status='interrupted', updated_at=? WHERE id=1",
            (now_iso(),))
        _log(conn, "backfill_interrupt", None, "回填被手动中断，可从检查点继续")


def backfill_stale_snapshot(conn, customer_id: int, stale_version: int) -> Dict[str, Any]:
    """显式演示冲突规则：回填方拿着旧快照版本去写，必须被拒绝。"""
    with _write_lock:
        row = conn.execute(
            "SELECT version FROM customers WHERE id=?", (customer_id,)).fetchone()
        if row is None:
            raise NotFoundError(f"customer {customer_id} 不存在")
        if row["version"] != stale_version:
            raise ConflictError(
                f"回填快照 v{stale_version} 已过期，当前业务版本 v{row['version']}，拒绝覆盖")
        # 快照仍然新鲜才允许执行（与逐行回填相同的 upsert）
        raw = conn.execute(
            "SELECT address_raw FROM customers WHERE id=?", (customer_id,)).fetchone()["address_raw"]
        parsed = parse_address(raw)
        conn.execute("BEGIN IMMEDIATE")
        try:
            ts = now_iso()
            conn.execute(
                """INSERT INTO customer_addresses
                    (id, country, province, city, district, street, house_no, postal_code,
                     detail, parse_status, parse_note, source_version, parsed_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(id) DO UPDATE SET
                    country=excluded.country, province=excluded.province,
                    city=excluded.city, district=excluded.district, street=excluded.street,
                    house_no=excluded.house_no, postal_code=excluded.postal_code,
                    detail=excluded.detail, parse_status=excluded.parse_status,
                    parse_note=excluded.parse_note, source_version=excluded.source_version,
                    parsed_at=excluded.parsed_at""",
                (customer_id, parsed["country"], parsed["province"], parsed["city"],
                 parsed["district"], parsed["street"], parsed["house_no"],
                 parsed["postal_code"], parsed["detail"], parsed["parse_status"],
                 parsed["parse_note"], stale_version, ts))
            conn.execute(
                "INSERT INTO write_journal(ts, customer_id, version, actor, raw_md5)"
                " VALUES(?,?,?, 'backfill', ?)",
                (ts, customer_id, stale_version, _md5(raw)))
            conn.execute("COMMIT")
            return {"id": customer_id, "version": stale_version, "parsed": parsed}
        except Exception:
            conn.execute("ROLLBACK")
            raise


# ------------------------------------------------------------ 人工处理待解析

def manual_resolve(conn, customer_id: int, fields: Dict[str, Optional[str]]) -> Dict[str, Any]:
    """人工确认待处理地址：只接受操作员显式提供的字段，不做任何自动补全。"""
    clean = {k: (fields.get(k) or None) for k in STRUCTURED_KEYS}
    with _write_lock:
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT version FROM customers WHERE id=?", (customer_id,)).fetchone()
            if row is None:
                raise NotFoundError(f"customer {customer_id} 不存在")
            existing = conn.execute(
                "SELECT * FROM customer_addresses WHERE id=?", (customer_id,)).fetchone()
            merged = {k: clean[k] if clean[k] is not None
                      else (existing[k] if existing else None)
                      for k in STRUCTURED_KEYS}
            ts = now_iso()
            conn.execute(
                """INSERT INTO customer_addresses
                    (id, country, province, city, district, street, house_no, postal_code,
                     detail, parse_status, parse_note, source_version, parsed_at)
                   VALUES (?,?,?,?,?,?,?,?,?, 'manual', ?, ?, ?)
                   ON CONFLICT(id) DO UPDATE SET
                    country=excluded.country, province=excluded.province,
                    city=excluded.city, district=excluded.district, street=excluded.street,
                    house_no=excluded.house_no, postal_code=excluded.postal_code,
                    detail=excluded.detail, parse_status='manual',
                    parse_note=excluded.parse_note, source_version=excluded.source_version,
                    parsed_at=excluded.parsed_at""",
                (customer_id, merged["country"], merged["province"], merged["city"],
                 merged["district"], merged["street"], merged["house_no"],
                 merged["postal_code"], merged["detail"],
                 "人工确认拆分，未提供的字段保持为空", row["version"], ts))
            conn.execute(
                "INSERT INTO write_journal(ts, customer_id, version, actor, raw_md5)"
                " SELECT ?, ?, version, 'manual', ? FROM customers WHERE id=?",
                (ts, customer_id, _md5(
                    conn.execute("SELECT address_raw FROM customers WHERE id=?",
                                 (customer_id,)).fetchone()["address_raw"]),
                 customer_id))
            _log(conn, "manual_resolve", customer_id, "人工确认结构化地址")
            conn.execute("COMMIT")
            return {"id": customer_id, "fields": merged}
        except Exception:
            conn.execute("ROLLBACK")
            raise


# ------------------------------------------------------------ 查询

def list_customers(conn) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """SELECT c.id, c.customer_name, c.address_raw, c.version, c.updated_at,
                  ca.country, ca.province, ca.city, ca.district, ca.street,
                  ca.house_no, ca.postal_code, ca.detail, ca.parse_status,
                  ca.parse_note, ca.source_version
           FROM customers c
           LEFT JOIN customer_addresses ca ON ca.id = c.id
           ORDER BY c.id""").fetchall()
    return [dict(r) for r in rows]


def get_customer(conn, customer_id: int) -> Dict[str, Any]:
    row = conn.execute(
        """SELECT c.id, c.customer_name, c.address_raw, c.version, c.updated_at,
                  ca.country, ca.province, ca.city, ca.district, ca.street,
                  ca.house_no, ca.postal_code, ca.detail, ca.parse_status,
                  ca.parse_note, ca.source_version
           FROM customers c LEFT JOIN customer_addresses ca ON ca.id=c.id
           WHERE c.id=?""", (customer_id,)).fetchone()
    if row is None:
        raise NotFoundError(f"customer {customer_id} 不存在")
    return dict(row)


def compare_customer(conn, customer_id: int) -> Dict[str, Any]:
    """比较两版输出：v1 原始值 vs v2 结构化投影，并校验投影与规则重算一致。"""
    rec = get_customer(conn, customer_id)
    fresh = parse_address(rec["address_raw"])
    stored = {k: rec.get(k) for k in STRUCTURED_KEYS}
    field_match = all(stored[k] == fresh[k] for k in STRUCTURED_KEYS)
    if rec.get("parse_status") == "manual":
        determinism_ok = None  # 人工行不参与机器规则一致性比较
    elif rec.get("parse_status") is None:
        determinism_ok = False  # 新投影缺失
    else:
        determinism_ok = field_match and rec.get("parse_status") == fresh["parse_status"]
    return {
        "id": customer_id,
        "v1_output": {"address_raw": rec["address_raw"], "version": rec["version"]},
        "v2_output": {
            "structured": stored,
            "parse_status": rec.get("parse_status"),
            "parse_note": rec.get("parse_note"),
            "source_version": rec.get("source_version"),
            "reconstructed": reconstruct(rec),
        },
        "fresh_parse": fresh,
        "determinism_ok": determinism_ok,
        "source_version_current": rec.get("source_version") == rec["version"],
    }


def reconstruct(rec: Dict[str, Any]) -> str:
    """把结构化投影按固定顺序拼回可读地址（仅用于展示比较，不写回任何表）。"""
    parts = [rec.get(k) for k in ("country", "province", "city", "district",
                                  "street", "house_no", "postal_code", "detail")]
    return " ".join(p for p in parts if p)


def stats(conn) -> Dict[str, Any]:
    total = conn.execute("SELECT COUNT(*) n FROM customers").fetchone()["n"]
    covered = conn.execute(
        "SELECT COUNT(*) n FROM customer_addresses").fetchone()["n"]
    by_status = {r["parse_status"]: r["n"] for r in conn.execute(
        "SELECT parse_status, COUNT(*) n FROM customer_addresses GROUP BY parse_status")}
    stale = conn.execute(
        "SELECT COUNT(*) n FROM customers c JOIN customer_addresses ca ON ca.id=c.id"
        " WHERE ca.source_version <> c.version").fetchone()["n"]
    mismatch = conn.execute(
        "SELECT COUNT(*) n FROM customers c LEFT JOIN customer_addresses ca ON ca.id=c.id"
        " WHERE ca.id IS NULL").fetchone()["n"]
    return {
        "total": total,
        "structured_covered": covered,
        "missing_projection": mismatch,
        "stale_projection": stale,
        "by_status": by_status,
    }


def checkpoint(conn) -> Dict[str, Any]:
    return dict(conn.execute(
        "SELECT * FROM migration_checkpoint WHERE id=1").fetchone())


def recent_logs(conn, limit: int = 30) -> List[Dict[str, Any]]:
    return [dict(r) for r in conn.execute(
        "SELECT * FROM migration_log ORDER BY id DESC LIMIT ?", (limit,)).fetchall()]


def recent_journal(conn, limit: int = 20) -> List[Dict[str, Any]]:
    return [dict(r) for r in conn.execute(
        "SELECT * FROM write_journal ORDER BY id DESC LIMIT ?", (limit,)).fetchall()]


# ------------------------------------------------------------ 发布开关

def get_read_mode(conn) -> str:
    return conn.execute(
        "SELECT value FROM settings WHERE key='read_mode'").fetchone()["value"]


def set_read_mode(conn, mode: str) -> None:
    if mode not in ("legacy", "dual", "structured"):
        raise ValueError("read_mode 必须是 legacy / dual / structured")
    conn.execute("UPDATE settings SET value=? WHERE key='read_mode'", (mode,))
    _log(conn, "cutover", None, f"读取开关切换为 {mode}")


def reset_checkpoint(conn) -> None:
    with _write_lock:
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                "UPDATE migration_checkpoint SET last_id=0, status='idle', processed=0,"
                " parsed_cnt=0, partial_cnt=0, pending_cnt=0, skipped_newer=0,"
                " started_at=NULL, updated_at=? WHERE id=1", (now_iso(),))
            _log(conn, "checkpoint_reset", None, "回填检查点已重置")
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

"""SQLite 持久层：旧投影、新投影、回填检查点、审计日志。

两套投影（legacy / structured）都在同一套显式服务函数里、同一个事务中写入，
数据库中**不创建任何触发器**——避免触发顺序导致两投影差异。
"""
from __future__ import annotations

import os
import sqlite3
import threading
from typing import Optional

DEFAULT_DB_PATH = os.environ.get(
    "MIGRATION_DB", os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "workbench.db")
)

SCHEMA = """
PRAGMA journal_mode=WAL;

-- 旧投影：消费者 v1 仍在读取的单一地址字段
CREATE TABLE IF NOT EXISTS customers (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_name   TEXT NOT NULL,
    address_raw     TEXT NOT NULL,                 -- 原始值，永久保留
    version         INTEGER NOT NULL DEFAULT 1,    -- 业务版本，冲突仲裁的唯一依据
    updated_at      TEXT NOT NULL
);

-- 新投影：结构化地址 + 解析状态（v2 消费者读取）
CREATE TABLE IF NOT EXISTS customer_addresses (
    id              INTEGER PRIMARY KEY
                    REFERENCES customers(id) ON DELETE CASCADE,
    country         TEXT,
    province        TEXT,
    city            TEXT,
    district        TEXT,
    street          TEXT,
    house_no        TEXT,
    postal_code     TEXT,
    detail          TEXT,
    parse_status    TEXT NOT NULL DEFAULT 'pending'
                    CHECK (parse_status IN ('parsed','partial','pending','manual')),
    parse_note      TEXT,
    source_version  INTEGER NOT NULL,               -- 投影对应的 customers.version
    parsed_at       TEXT NOT NULL
);

-- 回填检查点：单行，重启/中断后据此续跑
CREATE TABLE IF NOT EXISTS migration_checkpoint (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    last_id         INTEGER NOT NULL DEFAULT 0,     -- 已处理到的 customers.id
    status          TEXT NOT NULL DEFAULT 'idle'
                    CHECK (status IN ('idle','running','paused','done','interrupted')),
    processed       INTEGER NOT NULL DEFAULT 0,
    parsed_cnt      INTEGER NOT NULL DEFAULT 0,
    partial_cnt     INTEGER NOT NULL DEFAULT 0,
    pending_cnt     INTEGER NOT NULL DEFAULT 0,
    skipped_newer   INTEGER NOT NULL DEFAULT 0,     -- 撞到更新业务版本而跳过的行数
    started_at      TEXT,
    updated_at      TEXT NOT NULL
);
INSERT OR IGNORE INTO migration_checkpoint
    (id, last_id, status, processed, updated_at)
VALUES (1, 0, 'idle', 0, '');

-- 迁移/写入审计日志（工作台时间线使用）
CREATE TABLE IF NOT EXISTS migration_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT NOT NULL,
    event       TEXT NOT NULL,
    customer_id INTEGER,
    detail      TEXT
);

-- 双写流水：每次逻辑写入落一条有序记录，证明同一次写入只解析一次、两投影同源
CREATE TABLE IF NOT EXISTS write_journal (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT NOT NULL,
    customer_id INTEGER NOT NULL,
    version     INTEGER NOT NULL,
    actor       TEXT NOT NULL,          -- online(在线双写) / backfill(回填) / manual(人工)
    raw_md5     TEXT NOT NULL
);

-- 全局设置（含发布开关 read_mode）
CREATE TABLE IF NOT EXISTS settings (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL
);
INSERT OR IGNORE INTO settings(key, value) VALUES ('read_mode', 'legacy');

-- 关键查询索引（发布前核对会确认这些查询走索引而非全表扫描）
CREATE INDEX IF NOT EXISTS idx_addr_city       ON customer_addresses(city);
CREATE INDEX IF NOT EXISTS idx_addr_postal     ON customer_addresses(postal_code);
CREATE INDEX IF NOT EXISTS idx_addr_status     ON customer_addresses(parse_status);
CREATE INDEX IF NOT EXISTS idx_customers_raw   ON customers(address_raw);
"""

_local = threading.local()
_init_lock = threading.Lock()
_initialized_paths: set[str] = set()


def get_conn(db_path: Optional[str] = None) -> sqlite3.Connection:
    """每个线程一个连接（check_same_thread 安全），WAL 允许后台回填与页面读并发。"""
    path = db_path or DEFAULT_DB_PATH
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    if getattr(_local, "path", None) != path or not hasattr(_local, "conn"):
        conn = sqlite3.connect(path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA journal_mode=WAL")
        _local.conn = conn
        _local.path = path
    with _init_lock:
        if path not in _initialized_paths:
            from datetime import datetime, timezone
            ts = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
            _local.conn.executescript(SCHEMA)
            # 崩溃恢复：上次进程被杀死时检查点可能停在 running/paused，
            # 本进程内没有对应工作线程，统一转为 interrupted 等待续跑。
            stale = _local.conn.execute(
                "SELECT processed FROM migration_checkpoint WHERE id=1"
                " AND status IN ('running','paused')").fetchone()
            if stale is not None:
                _local.conn.execute(
                    "UPDATE migration_checkpoint SET status='interrupted', updated_at=?"
                    " WHERE id=1", (ts,))
                _local.conn.execute(
                    "INSERT INTO migration_log(ts, event, customer_id, detail)"
                    " VALUES(?, 'startup_recovery', NULL, ?)",
                    (ts, f"检测到上次进程异常退出，检查点停在已处理 {stale['processed']} 行，"
                         "标记为 interrupted 等待续跑"))
            _initialized_paths.add(path)
    return _local.conn


def reset_database(db_path: str) -> None:
    """删除并重建数据库（仅供测试与"重置演示数据"使用）。"""
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(db_path + suffix)
        except FileNotFoundError:
            pass
    with _init_lock:
        _initialized_paths.discard(db_path)
    if hasattr(_local, "conn") and getattr(_local, "path", None) == db_path:
        del _local.conn
        del _local.path
    get_conn(db_path)

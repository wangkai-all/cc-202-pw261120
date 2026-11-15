"""发布前核对：主键、引用、投影完整性、确定性与关键查询。

返回一组检查项，每项 status 为 pass / warn / fail。
cutover 到 structured 只允许在没有 fail 时进行（pending/partial 记为 warn）。
"""
from __future__ import annotations

from typing import Any, Dict, List

from .parser import STRUCTURED_KEYS, parse_address
from .service import get_read_mode


def _item(name: str, status: str, detail: str) -> Dict[str, str]:
    return {"name": name, "status": status, "detail": detail}


def run_preflight(conn) -> Dict[str, Any]:
    checks: List[Dict[str, str]] = []

    # 1. 主键：customers / customer_addresses 均以 id 为主键，且一一对应
    dup_c = conn.execute(
        "SELECT id, COUNT(*) n FROM customers GROUP BY id HAVING n > 1").fetchall()
    dup_a = conn.execute(
        "SELECT id, COUNT(*) n FROM customer_addresses GROUP BY id HAVING n > 1").fetchall()
    if dup_c or dup_a:
        checks.append(_item("主键唯一性", "fail",
                            f"customers 重复 {len(dup_c)} 组，addresses 重复 {len(dup_a)} 组"))
    else:
        checks.append(_item("主键唯一性", "pass",
                            "两表均以 id 为主键且无重复；新投影 id 同时为外键引用 customers.id"))

    # 2. 引用完整性
    fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
    orphan = conn.execute(
        "SELECT ca.id FROM customer_addresses ca LEFT JOIN customers c ON c.id=ca.id"
        " WHERE c.id IS NULL").fetchall()
    if fk_violations or orphan:
        checks.append(_item("外键引用完整性", "fail",
                            f"PRAGMA foreign_key_check 违规 {len(fk_violations)} 条，"
                            f"孤儿投影 {len(orphan)} 条"))
    else:
        checks.append(_item("外键引用完整性", "pass", "无外键违规、无孤儿结构化投影"))

    # 3. 投影覆盖：每条旧记录都必须有新投影
    missing = conn.execute(
        "SELECT c.id FROM customers c LEFT JOIN customer_addresses ca ON ca.id=c.id"
        " WHERE ca.id IS NULL ORDER BY c.id").fetchall()
    if missing:
        ids = ", ".join(str(r["id"]) for r in missing[:20])
        checks.append(_item("投影覆盖率", "fail",
                            f"{len(missing)} 条记录缺少新投影（如 id {ids}），回填未完成"))
    else:
        total = conn.execute("SELECT COUNT(*) n FROM customers").fetchone()["n"]
        checks.append(_item("投影覆盖率", "pass", f"全部 {total} 条记录均有结构化投影"))

    # 4. 版本一致：新投影必须对应最新业务版本（旧回填覆盖新修改会在这里暴露）
    stale = conn.execute(
        "SELECT c.id, c.version, ca.source_version FROM customers c"
        " JOIN customer_addresses ca ON ca.id=c.id"
        " WHERE ca.source_version <> c.version").fetchall()
    if stale:
        detail = "；".join(
            f"id {r['id']} 业务 v{r['version']} / 投影 v{r['source_version']}"
            for r in stale[:5])
        checks.append(_item("投影版本一致", "fail",
                            f"{len(stale)} 条投影落后于业务版本：{detail}"))
    else:
        checks.append(_item("投影版本一致", "pass", "所有投影的 source_version 等于当前业务版本"))

    # 5. 确定性：用同一规则重算每条原始值，与存量投影逐字段比较（人工行豁免）
    rows = conn.execute(
        "SELECT c.id, c.address_raw, ca.* FROM customers c"
        " JOIN customer_addresses ca ON ca.id=c.id ORDER BY c.id").fetchall()
    drift, manual_cnt = [], 0
    for r in rows:
        if r["parse_status"] == "manual":
            manual_cnt += 1
            continue
        fresh = parse_address(r["address_raw"])
        diffs = [k for k in STRUCTURED_KEYS if r[k] != fresh[k]]
        if diffs or r["parse_status"] != fresh["parse_status"]:
            drift.append((r["id"], diffs))
    if drift:
        detail = "；".join(f"id {i} 字段 {d}" for i, d in drift[:5])
        checks.append(_item("规则确定性", "fail",
                            f"{len(drift)} 条投影与规则重算结果不一致：{detail}"))
    else:
        checks.append(_item(
            "规则确定性", "pass",
            f"全部机器解析行与 parse_address 重算一致（{manual_cnt} 条人工行豁免），"
            "证明双写与回填同源"))

    # 6. 待处理项：不能编造字段，pending/partial 必须显式保留
    pending = conn.execute(
        "SELECT id FROM customer_addresses WHERE parse_status IN ('pending','partial')"
        " ORDER BY id").fetchall()
    if pending:
        checks.append(_item(
            "待处理地址", "warn",
            f"{len(pending)} 条为 pending/partial，保留原文与状态，需人工确认后才能完全切换"))
    else:
        checks.append(_item("待处理地址", "pass", "无待处理地址"))

    # 7. 关键查询：新投影查询结果必须与"对原始值现算"的结果集一致，且走索引
    def query_parity(label: str, column: str, value: str):
        plan = conn.execute(
            f"EXPLAIN QUERY PLAN SELECT id FROM customer_addresses WHERE {column}=?",
            (value,)).fetchall()
        uses_index = any("USING" in p["detail"] and "INDEX" in p["detail"] for p in plan)
        got = {r["id"] for r in conn.execute(
            f"SELECT id FROM customer_addresses WHERE {column}=? AND parse_status != 'manual'",
            (value,)).fetchall()}
        want = {r["id"] for r in rows
                if r["parse_status"] != "manual"
                and parse_address(r["address_raw"])[column] == value}
        ok = got == want
        checks.append(_item(
            f"关键查询·{label}",
            "pass" if ok and uses_index else ("fail" if not ok else "warn"),
            f"结果集{'一致' if ok else '不一致'}（{len(got)} 行），"
            f"索引: {'命中' if uses_index else '未命中（全表扫描）'}"))

    sample_city = conn.execute(
        "SELECT city FROM customer_addresses WHERE city IS NOT NULL LIMIT 1").fetchone()
    if sample_city:
        query_parity("按城市检索", "city", sample_city["city"])
    else:
        checks.append(_item("关键查询·按城市检索", "warn", "暂无可检索的城市值"))

    sample_postal = conn.execute(
        "SELECT postal_code FROM customer_addresses WHERE postal_code IS NOT NULL LIMIT 1"
    ).fetchone()
    if sample_postal:
        query_parity("按邮编检索", "postal_code", sample_postal["postal_code"])
    else:
        checks.append(_item("关键查询·按邮编检索", "warn", "暂无可检索的邮编值"))

    hard_fail = [c for c in checks if c["status"] == "fail"]
    return {
        "checks": checks,
        "hard_fail_count": len(hard_fail),
        "ready_to_cutover": not hard_fail,
        "current_read_mode": get_read_mode(conn),
    }

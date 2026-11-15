"""端到端：真实进程被 kill -9 后，检查点仍可续跑。

步骤：
1. 子进程启动 HTTP 服务（独立数据库目录），灌入历史样例；
2. 启动慢速回填，处理若干行后直接 SIGKILL 杀掉进程（模拟宕机/断电）；
3. 重新启动服务，验证检查点状态、续跑完成、两投影一致。

运行：python3 -I tests/test_process_crash.py
"""
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def request(base, path, body=None, expect=None):
    req = urllib.request.Request(
        base + path,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"},
        method="POST" if body is not None else "GET")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            payload = json.loads(resp.read().decode())
            code = resp.status
    except urllib.error.HTTPError as e:
        code, payload = e.code, json.loads(e.read().decode())
    if expect is not None and code != expect:
        raise AssertionError(f"{path} 返回 {code}，期望 {expect}：{payload}")
    return code, payload


def wait_ready(base, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            request(base, "/api/state")
            return
        except Exception:
            time.sleep(0.2)
    raise TimeoutError("服务未在预期时间内就绪")


def main() -> int:
    tmp = tempfile.mkdtemp(prefix="crash-demo-")
    db_path = os.path.join(tmp, "crash.db")
    env = dict(os.environ, MIGRATION_DB=db_path, PORT="8099", HOST="127.0.0.1")
    base = "http://127.0.0.1:8099"

    # ---- 第一个进程：种子 + 慢速回填，然后 SIGKILL ----
    proc = subprocess.Popen(
        [sys.executable, "-I", os.path.join(ROOT, "run_server.py")],
        cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        wait_ready(base)
        request(base, "/api/seed", {}, expect=200)
        request(base, "/api/backfill/start", {"delay": 0.25}, expect=200)

        processed = 0
        deadline = time.time() + 10
        while time.time() < deadline:
            _, state = request(base, "/api/state")
            processed = state["checkpoint"]["processed"]
            if processed >= 4:
                break
            time.sleep(0.1)
        assert processed >= 4, "回填在崩溃前没有取得进展"
        print(f"[1] 回填进行到 {processed} 行，发送 SIGKILL 模拟进程中断…")
        proc.send_signal(signal.SIGKILL)
        proc.wait(timeout=10)
        assert proc.returncode and proc.returncode < 0, "进程应被信号杀死"
    finally:
        if proc.poll() is None:
            proc.kill()

    # ---- 数据库文件在进程外仍然完好，检查点已落盘 ----
    import sqlite3
    with sqlite3.connect(db_path) as raw:
        cp = raw.execute("SELECT last_id, processed, status FROM migration_checkpoint").fetchone()
    print(f"[2] 磁盘检查点：last_id={cp[0]} processed={cp[1]} status={cp[2]}")
    assert cp[1] >= 4 and cp[1] < 15, "检查点应停在中途"

    # ---- 第二个进程：从检查点续跑直到完成 ----
    proc2 = subprocess.Popen(
        [sys.executable, "-I", os.path.join(ROOT, "run_server.py")],
        cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        wait_ready(base)
        _, state_before = request(base, "/api/state")
        assert state_before["checkpoint"]["processed"] == cp[1], "重启后检查点必须保留"
        print(f"[3] 新进程识别检查点，从 processed={cp[1]} 继续…")
        request(base, "/api/backfill/start", {"delay": 0.0}, expect=200)

        deadline = time.time() + 15
        while time.time() < deadline:
            _, state = request(base, "/api/state")
            if state["checkpoint"]["status"] == "done":
                break
            time.sleep(0.2)
        _, state = request(base, "/api/state")
        assert state["checkpoint"]["status"] == "done", state["checkpoint"]
        assert state["stats"]["missing_projection"] == 0
        assert state["stats"]["stale_projection"] == 0
        print(f"[4] 续跑完成：processed={state['checkpoint']['processed']}，"
              f"覆盖率={state['stats']['structured_covered']}/{state['stats']['total']}")

        _, pf = request(base, "/api/preflight")
        assert pf["ready_to_cutover"], "崩溃续跑后发布核对必须通过"
        print("[5] 发布前核对通过，无 fail 项：")
        for c in pf["checks"]:
            print(f"    [{c['status'].upper():7}] {c['name']} — {c['detail']}")
    finally:
        proc2.terminate()
        proc2.wait(timeout=10)

    print("\n进程中断续跑测试通过 ✓")
    return 0


if __name__ == "__main__":
    sys.exit(main())

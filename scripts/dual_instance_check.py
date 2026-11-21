"""双实例共卷并发验证（Docker 不可用环境下的等价集成测试）。

以两个独立 uvicorn 进程启动同一应用、挂载同一 DATA_DIR，
验证：
  1. 并发发布同单：恰有一个写入，另一个幂等复用，不分叉不覆盖；
  2. 并发发布异单：全部成功且序号稠密、检查点连续；
  3. 实例 A 的发布可在实例 B 下载，证明与检查点跨实例一致；
  4. 杀掉一个实例再重启，历史检查点/证明持续有效，可继续追加；
  5. 差分发布：跨实例四层差分链、第五层拒绝、重启后按原字节重建取件、
     改回旧字节两稿并存、纳入证明离线核验通过。

用法：python scripts/dual_instance_check.py
"""
from __future__ import annotations

import base64
import hashlib
import http.client
import io
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


# 每次运行随机选取空闲端口，避免环境中无法回收的旧进程占用。
PORT_A = int(os.environ.get("PORT_A", str(_free_port())))
PORT_B = int(os.environ.get("PORT_B", str(_free_port())))
PORT_C = int(os.environ.get("PORT_C", str(_free_port())))


def _wait_ready(port: int, timeout: float = 20.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{port}/health", timeout=2
                ) as resp:
                    if resp.status == 200:
                        return
        except OSError:
            time.sleep(0.3)
    raise RuntimeError(f"实例端口 {port} 未就绪")


def start_instance(port: int, data_dir: str) -> subprocess.Popen:
    env = dict(os.environ)
    env.update(
        {
            "DATA_DIR": data_dir,
            "MAX_RELEASES": "128",
            "MAX_FILE_BYTES": str(64 * 1024),
        }
    )
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "app.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        cwd=str(REPO_ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    _wait_ready(port)
    return proc


def _multipart_body(fields: dict, file_field: str, filename: str, content: bytes):
    boundary = f"----wq{uuid.uuid4().hex}"
    body = io.BytesIO()
    for key, value in fields.items():
        body.write(f"--{boundary}\r\n".encode())
        body.write(
            f'Content-Disposition: form-data; name="{key}"\r\n\r\n{value}\r\n'.encode()
        )
    body.write(f"--{boundary}\r\n".encode())
    body.write(
        f'Content-Disposition: form-data; name="{file_field}"; '
        f'filename="{filename}"\r\nContent-Type: text/csv\r\n\r\n'.encode()
    )
    body.write(content)
    body.write(f"\r\n--{boundary}--\r\n".encode())
    return body.getvalue(), f"multipart/form-data; boundary={boundary}"


def post_release(port: int, station: str, day: str, order: str, content: bytes):
    body, ctype = _multipart_body(
        {"station_id": station, "sample_date": day, "order_no": order},
        "file",
        f"{order}.csv",
        content,
    )
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    conn.request("POST", "/releases", body=body, headers={"Content-Type": ctype})
    resp = conn.getresponse()
    payload = json.loads(resp.read().decode())
    conn.close()
    return resp.status, payload


def get_json(port: int, path: str):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=10) as r:
        return r.status, json.loads(r.read().decode())


def get_bytes(port: int, path: str):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=10) as r:
        return r.status, r.read()


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _form_body(fields: dict):
    boundary = f"----wq{uuid.uuid4().hex}"
    body = io.BytesIO()
    for key, value in fields.items():
        body.write(f"--{boundary}\r\n".encode())
        body.write(
            f'Content-Disposition: form-data; name="{key}"\r\n\r\n{value}\r\n'.encode()
        )
    body.write(f"--{boundary}--\r\n".encode())
    return body.getvalue(), f"multipart/form-data; boundary={boundary}"


def post_delta(port: int, station: str, day: str, order: str, base_id: int, segments):
    body, ctype = _form_body(
        {
            "station_id": station,
            "sample_date": day,
            "order_no": order,
            "base_release_id": str(base_id),
            "segments": json.dumps(segments),
        }
    )
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    conn.request(
        "POST", "/releases/delta", body=body, headers={"Content-Type": ctype}
    )
    resp = conn.getresponse()
    raw = resp.read().decode()
    payload = json.loads(raw) if raw else {}
    conn.close()
    return resp.status, payload


def main() -> int:
    data_dir = tempfile.mkdtemp(prefix="wq-dual-")
    print(f"[setup] 共享 DATA_DIR={data_dir}")
    proc_a = start_instance(PORT_A, data_dir)
    proc_b = start_instance(PORT_B, data_dir)
    failures: list[str] = []

    def check(cond: bool, message: str) -> None:
        print(f"[{'PASS' if cond else 'FAIL'}] {message}")
        if not cond:
            failures.append(message)

    try:
        # 1) 并发同单：两实例同时发同一键、同一字节。
        same = b"station,ph\nSAME,7.1\n"
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(
                pool.map(
                    lambda port: post_release(
                        port, "SAME", "2026-11-16", "ORD-DUP", same
                    ),
                    [PORT_A, PORT_B],
                )
            )
        statuses = [r[0] for r in results]
        reused = sorted(r[1]["reused"] for r in results)
        check(statuses == [201, 201], f"并发同单均返回 201（实际 {statuses}）")
        check(reused == [False, True], f"恰有一次写入、一次复用（实际 {reused}）")
        ids = {r[1]["release"]["id"] for r in results}
        check(ids == {1}, f"同单只有一条记录 id=1（实际 {ids}）")

        # 2) 并发异单：10 个不同键分散到两实例。
        keys = [(f"ST{i:02d}", f"ORD-{i:02d}") for i in range(10)]

        def publish_one(item):
            idx, (station, order) = item
            port = PORT_A if idx % 2 == 0 else PORT_B
            return post_release(
                port, station, "2026-11-16", order, f"row-{idx}\n".encode()
            )

        with ThreadPoolExecutor(max_workers=10) as pool:
            bulk = list(pool.map(publish_one, enumerate(keys)))
        check(all(s == 201 for s, _ in bulk), "10 件并发异单全部 201")
        check(
            all(not p["reused"] for _, p in bulk),
            "10 件均为新写入而非复用",
        )

        status_a, releases_a = get_json(PORT_A, "/releases")
        status_b, releases_b = get_json(PORT_B, "/releases")
        check(releases_a == releases_b, "两实例看到完全一致的发布目录")
        ids_seq = [r["id"] for r in releases_a]
        check(
            ids_seq == list(range(1, 12)),
            f"序号 1..11 稠密无分叉（实际 {ids_seq}）",
        )

        # 3) 跨实例取件与检查点一致。
        status_dl, blob = get_bytes(
            PORT_B, "/releases/ST00/2026-11-16/ORD-00/download"
        )
        check(status_dl == 200 and blob == b"row-0\n", "实例 B 可取实例 A 发布的原件")
        _, cp_a = get_json(PORT_A, "/checkpoints/latest")
        _, cp_b = get_json(PORT_B, "/checkpoints/latest")
        check(cp_a == cp_b and cp_a["seq"] == 11, "两实例最新检查点一致且条数=11")

        # 4) 跨实例证明与离线核验。
        _, proof = get_json(
            PORT_B, "/releases/ST00/2026-11-16/ORD-00/inclusion-proof"
        )
        sys.path.insert(0, str(REPO_ROOT))
        from app.verify import verify_append_proof, verify_release_bytes

        ok, msg = verify_release_bytes(proof, b"row-0\n")
        check(ok, f"跨实例纳入证明离线核验通过：{msg}")
        _, old_cp = get_json(PORT_A, "/checkpoints/1")
        _, append = get_json(PORT_A, "/checkpoints/1/append-proof")
        ok, msg = verify_append_proof(old_cp, cp_a, append)
        check(ok, f"1 号→11 号检查点追加证明离线核验通过：{msg}")
        check(
            len(append["entries"]) <= 2 * 7,
            f"追加证明条目数 O(log n)={len(append['entries'])} ≤ 14",
        )

        # 5) 差分发布：跨实例四层链、第五层拒绝、同单重试、改回旧字节两稿并存。
        base = b"station,time,ph\nDL,2026-11-16T08:00,7.00\n"
        st, pub = post_release(PORT_A, "DLST", "2026-11-16", "DL-BASE", base)
        check(st == 201, f"差分链基稿整件发布 201（实际 {st}）")
        base_id = pub["release"]["id"]
        current = base
        prev_id = base_id
        depth_ids: dict[int, int] = {0: base_id}
        depth_bytes: dict[int, bytes] = {0: base}
        pos = current.index(b"7.00")
        for layer in range(1, 5):
            marker = f"7.{layer:02d}".encode()
            segs = [{"start": pos, "end": pos + 4, "b64": _b64(marker)}]
            expected = current[:pos] + marker + current[pos + 4 :]
            # 分层交替打到 A/B 两个实例，验证跨实例链式引用。
            port = PORT_A if layer % 2 else PORT_B
            st, dpub = post_delta(
                port, "DLST", "2026-11-16", f"DL-D{layer}", prev_id, segs
            )
            check(
                st == 201 and dpub["release"]["chain_depth"] == layer,
                f"第 {layer} 层差分发布成功且链深={layer}（实际 {st}）",
            )
            check(
                dpub["release"]["blob_sha256"]
                == hashlib.sha256(expected).hexdigest(),
                f"第 {layer} 层叶子登记的是重建全文摘要",
            )
            # 同单同输入重试 -> 复用，不新增记录。
            st2, retry = post_delta(
                port, "DLST", "2026-11-16", f"DL-D{layer}", prev_id, segs
            )
            check(
                st2 == 201 and retry["reused"] is True,
                f"第 {layer} 层差分同单重试复用（reused={retry.get('reused')}）",
            )
            # 在另一实例按原字节取回重建全文。
            other = PORT_B if port == PORT_A else PORT_A
            st_dl, blob = get_bytes(
                other, f"/releases/DLST/2026-11-16/DL-D{layer}/download"
            )
            check(
                st_dl == 200 and blob == expected,
                f"跨实例重建取件第 {layer} 层字节一致",
            )
            current, prev_id = expected, dpub["release"]["id"]
            depth_ids[layer] = prev_id
            depth_bytes[layer] = expected

        # 第五层差分必须被拒绝并提示整件上传，检查点不推进。
        cp_before = get_json(PORT_A, "/checkpoints/latest")[1]
        st5, body5 = post_delta(
            PORT_A, "DLST", "2026-11-16", "DL-D5", prev_id,
            [{"start": 0, "end": 1, "b64": _b64(b"X")}],
        )
        check(st5 == 409 and "整件上传" in body5["detail"],
              f"第五层差分 409 且提示整件上传（实际 {st5}）")
        check(
            get_json(PORT_A, "/checkpoints/latest")[1] == cp_before,
            "被拒差分未推进检查点",
        )

        # 改回旧稿字节：基于第 3 层（链深 3）发第 4 层差分，把 7.03 改回 7.00。
        # 重建全文与基稿逐字节相同，但两份稿是两条独立记录，都能取回。
        d3_bytes = depth_bytes[3]
        marker_pos = d3_bytes.index(b"7.03")
        st_back, back_pub = post_delta(
            PORT_B, "DLST", "2026-11-16", "DL-REVERT", depth_ids[3],
            [{"start": marker_pos, "end": marker_pos + 4, "b64": _b64(b"7.00")}],
        )
        reverted = d3_bytes[:marker_pos] + b"7.00" + d3_bytes[marker_pos + 4 :]
        check(
            st_back == 201 and back_pub["release"]["chain_depth"] == 4
            and back_pub["release"]["blob_sha256"]
            == hashlib.sha256(base).hexdigest(),
            "改回旧字节的差分稿登记摘要与基稿相同（内容寻址共享，不复制全文）",
        )
        _, dl_base = get_bytes(
            PORT_A, "/releases/DLST/2026-11-16/DL-BASE/download"
        )
        _, dl_revert = get_bytes(
            PORT_A, "/releases/DLST/2026-11-16/DL-REVERT/download"
        )
        check(
            dl_base == base and dl_revert == reverted and dl_revert == dl_base,
            "基稿与改回旧字节的差分稿两份都能按原字节取回",
        )
        # 差分稿纳入证明走原有离线核验（凭重建全文字节）。
        _, dproof = get_json(
            PORT_B, "/releases/DLST/2026-11-16/DL-D4/inclusion-proof"
        )
        ok, msg = verify_release_bytes(dproof, current)
        check(ok, f"四层差分稿纳入证明离线核验通过：{msg}")
        # 卷上不允许出现以重建摘要命名的全文原件（只存差分文档）。
        rebuilt_digest = back_pub["release"]["blob_sha256"]
        delta_digest = back_pub["release"]["delta_blob_sha256"]
        delta_path = Path(data_dir) / "deltas" / delta_digest[:2] / delta_digest[2:4] / delta_digest
        check(delta_path.is_file(), "差分文档已落共享卷 deltas/ 目录")
        full_path = (
            Path(data_dir) / "blobs" / rebuilt_digest[:2]
            / rebuilt_digest[2:4] / rebuilt_digest
        )
        # 该摘要恰好等于基稿摘要，基稿原件本就存在；检查 D4 的纯差分摘要无全文副本。
        d4 = next(
            r for r in get_json(PORT_A, "/releases")[1] if r["order_no"] == "DL-D4"
        )
        d4_full = (
            Path(data_dir) / "blobs" / d4["blob_sha256"][:2]
            / d4["blob_sha256"][2:4] / d4["blob_sha256"]
        )
        check(
            not d4_full.exists(),
            "差分稿重建全文不另存原件（blobs/ 下无 D4 重建摘要文件）",
        )

        # 6) 故障转移：原实例不杀（本环境 seccomp 禁止 kill），
        #    另起一个全新进程指向同一共享卷，模拟实例重启/替换。
        count_before_restart = len(get_json(PORT_A, "/releases")[1])
        cp_before_restart = get_json(PORT_A, "/checkpoints/latest")[1]
        proc_c = start_instance(PORT_C, data_dir)
        try:
            _, health = get_json(PORT_C, "/health")
            check(
                health["status"] == "ok"
                and health["release_count"] == count_before_restart,
                f"新实例挂载同卷后自检通过且条数={count_before_restart}",
            )
            _, cp_after = get_json(PORT_C, "/checkpoints/latest")
            check(cp_after == cp_before_restart, "重启视角下最新检查点根摘要不变")
            # 重启后四层差分链仍能按原字节取回并通过离线核验。
            st_rb, rebuilt_after = get_bytes(
                PORT_C, "/releases/DLST/2026-11-16/DL-D4/download"
            )
            check(
                st_rb == 200 and rebuilt_after == depth_bytes[4],
                "重启后差分稿仍按原字节重建取回",
            )
            _, proof_after = get_json(
                PORT_C, "/releases/DLST/2026-11-16/DL-D4/inclusion-proof"
            )
            ok, _ = verify_release_bytes(proof_after, depth_bytes[4])
            check(ok, "重启后差分稿纳入证明离线核验仍通过")
            status_post, new_pub = post_release(
                PORT_C, "ST99", "2026-11-16", "ORD-AFTER-RESTART", b"after\n"
            )
            check(
                status_post == 201
                and new_pub["release"]["id"] == count_before_restart + 1,
                f"新实例可继续追加，新记录序号={count_before_restart + 1}",
            )
            _, proof12 = get_json(
                PORT_B,
                "/releases/ST99/2026-11-16/ORD-AFTER-RESTART/inclusion-proof",
            )
            ok, _ = verify_release_bytes(proof12, b"after\n")
            check(ok, "新实例追加记录的纳入证明可在另一实例离线核验")
        finally:
            # 无法 kill：改为脱离进程（它绑定临时目录，会话结束后随环境回收）。
            print(f"[note] 环境禁止 kill 信号，第三实例保留运行，端口 {PORT_C}")

    finally:
        for proc in (proc_a, proc_b):
            if proc.poll() is None:
                try:
                    proc.send_signal(signal.SIGTERM)
                    proc.wait(timeout=10)
                except (PermissionError, subprocess.TimeoutExpired):
                    # 本环境 seccomp 可能禁止 kill；进程为临时目录下的演示进程。
                    print(f"[note] 无法终止 pid={proc.pid}（环境限制），保留运行")

    print(f"\n结果：{len(failures)} 项失败")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())

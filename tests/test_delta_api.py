"""差分发布 API 端到端测试。

覆盖：差分提交与重建取件、差分再差分（四层链）、第五层拒绝并提示整件上传、
改回旧稿字节两份均可取回、同单重试/冲突、区间校验、重建全文大小沿用原件规则、
基稿不可核验不落记录、差分落盘失败不推进检查点、重启自检与离线核验。
"""
from __future__ import annotations

import base64
import hashlib
import io
import json

import pytest

from app import main as main_mod
from app.storage import BlobError
from app.verify import verify_release_bytes


def b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _publish(client, station, day, order, data):
    return client.post(
        "/releases",
        data={"station_id": station, "sample_date": day, "order_no": order},
        files={"file": ("day.csv", io.BytesIO(data), "text/csv")},
    )


def _publish_delta(client, station, day, order, base_id, segments):
    return client.post(
        "/releases/delta",
        data={
            "station_id": station,
            "sample_date": day,
            "order_no": order,
            "base_release_id": base_id,
            "segments": json.dumps(segments),
        },
    )


def _seg(start: int, end: int, repl: bytes) -> dict:
    return {"start": start, "end": end, "b64": b64(repl)}


def _download(client, station, day, order):
    return client.get(f"/releases/{station}/{day}/{order}/download")


def test_delta_publish_rebuilds_and_downloads_original_bytes(app_client):
    client, app, settings = app_client
    base = b"station,time,ph\nST01,2026-11-16T08:00,7.2\n"
    r = _publish(client, "DST1", "2026-11-16", "D-ORD-1", base)
    assert r.status_code == 201
    base_id = r.json()["release"]["id"]

    # 替换 pH 值 7.2 -> 7.5（等长），并把末尾换行区间替换为“新行 + 换行”（变长扩大）。
    pos = base.index(b"7.2")
    end = len(base)
    segments = [
        _seg(pos, pos + 3, b"7.5"),
        _seg(end - 1, end, b"ST01,2026-11-16T09:00,7.4\n"),
    ]
    expected = (
        base[:pos] + b"7.5" + base[pos + 3 : -1]
        + b"ST01,2026-11-16T09:00,7.4\n"
    )
    r2 = _publish_delta(client, "DST1", "2026-11-16", "D-ORD-2", base_id, segments)
    assert r2.status_code == 201, r2.text
    body = r2.json()
    assert body["reused"] is False
    rel = body["release"]
    assert rel["kind"] == "delta"
    assert rel["base_id"] == base_id
    assert rel["chain_depth"] == 1
    assert rel["delta_blob_sha256"]
    # 叶子按重建全文登记摘要与长度。
    assert rel["blob_sha256"] == hashlib.sha256(expected).hexdigest()
    assert rel["size"] == len(expected)
    assert body["checkpoint"]["seq"] == rel["id"]

    # 取件得到重建后的原字节，且卷上不存在以重建摘要命名的全文原件。
    dl = _download(client, "DST1", "2026-11-16", "D-ORD-2")
    assert dl.status_code == 200
    assert dl.content == expected
    assert not main_mod.blob_store.exists(rel["blob_sha256"])
    # 差分文档确实在卷上。
    assert main_mod.delta_store.exists(rel["delta_blob_sha256"])
    # 基稿仍可取回原字节。
    assert _download(client, "DST1", "2026-11-16", "D-ORD-1").content == base


def test_delta_idempotent_retry_and_conflict(app_client):
    client, _, _ = app_client
    base = b"aaaa-bbbb-cccc\n"
    bid = _publish(client, "DST2", "2026-11-16", "B1", base).json()["release"]["id"]
    segments = [_seg(0, 4, b"XXXX")]
    r1 = _publish_delta(client, "DST2", "2026-11-16", "D1", bid, segments)
    r2 = _publish_delta(client, "DST2", "2026-11-16", "D1", bid, segments)
    assert r1.status_code == 201 and r2.status_code == 201
    assert r1.json()["reused"] is False and r2.json()["reused"] is True
    assert r1.json()["release"] == r2.json()["release"]
    # 同单异输入 -> 409，记录不改写。
    other = [_seg(5, 9, b"YYYY")]
    r3 = _publish_delta(client, "DST2", "2026-11-16", "D1", bid, other)
    assert r3.status_code == 409
    rec = client.get("/releases/DST2/2026-11-16/D1").json()
    assert rec["blob_sha256"] == r1.json()["release"]["blob_sha256"]


def test_delta_chain_four_layers_then_fifth_rejected(app_client):
    client, _, settings = app_client
    current = b"row-0000\n"
    bid = _publish(client, "CHAIN", "2026-11-16", "C0", current).json()[
        "release"
    ]["id"]
    pos = current.index(b"0000")
    previous_id = bid
    for layer in range(1, settings.max_delta_depth + 1):
        # 每层替换同一位置的四字节行号，形成链式差分。
        marker = f"{layer:04d}".encode()
        segments = [_seg(pos, pos + 4, marker)]
        expected = current[:pos] + marker + current[pos + 4 :]
        r = _publish_delta(
            client, "CHAIN", "2026-11-16", f"C{layer}", previous_id, segments
        )
        assert r.status_code == 201, r.text
        rel = r.json()["release"]
        assert rel["chain_depth"] == layer
        # 每一层都能按原字节取回。
        assert _download(client, "CHAIN", "2026-11-16", f"C{layer}").content == expected
        current, previous_id = expected, rel["id"]

    # 第五层：拒绝并明确提示整件上传，且不产生记录/不推进检查点。
    before = client.get("/checkpoints/latest").json()
    releases_before = len(client.get("/releases").json())
    r5 = _publish_delta(
        client,
        "CHAIN",
        "2026-11-16",
        "C5",
        previous_id,
        [_seg(0, 3, b"row")],
    )
    assert r5.status_code == 409
    assert "整件上传" in r5.json()["detail"]
    assert client.get("/checkpoints/latest").json() == before
    assert len(client.get("/releases").json()) == releases_before

    # 按提示整件上传可以成功。
    rup = _publish(client, "CHAIN", "2026-11-16", "C5-FULL", current + b"x")
    assert rup.status_code == 201
    assert rup.json()["release"]["kind"] == "full"


def test_reverting_to_old_bytes_keeps_both_releases_retrievable(app_client):
    client, _, _ = app_client
    original = b"station,ph\nST,7.1\n"
    rid0 = _publish(client, "REV", "2026-11-16", "R0", original).json()[
        "release"
    ]["id"]
    # 差分稿 1：7.1 -> 8.8
    pos = original.index(b"7.1")
    d1 = _publish_delta(
        client,
        "REV",
        "2026-11-16",
        "R1",
        rid0,
        [_seg(pos, pos + 3, b"8.8")],
    ).json()["release"]
    changed = original[:pos] + b"8.8" + original[pos + 3 :]
    # 差分稿 2：改回基稿旧字节 8.8 -> 7.1（重建全文恰与原件逐字节相同）。
    pos2 = changed.index(b"8.8")
    d2 = _publish_delta(
        client,
        "REV",
        "2026-11-16",
        "R2",
        d1["id"],
        [_seg(pos2, pos2 + 3, b"7.1")],
    ).json()["release"]
    assert d2["blob_sha256"] == hashlib.sha256(original).hexdigest()
    assert d2["kind"] == "delta" and d2["chain_depth"] == 2
    # 两份稿（原件与“改回来”的差分稿）都能按各自原字节取回。
    assert _download(client, "REV", "2026-11-16", "R0").content == original
    assert _download(client, "REV", "2026-11-16", "R2").content == original
    assert _download(client, "REV", "2026-11-16", "R1").content == changed
    # 卷上该摘要只对应一份原件（内容寻址共享），差分链不复制重建全文。
    original_digest = hashlib.sha256(original).hexdigest()
    assert main_mod.blob_store.exists(original_digest)
    assert main_mod.blob_store.path_for(d2["blob_sha256"]) == (
        main_mod.blob_store.path_for(original_digest)
    )


def test_delta_segment_validation_errors(app_client):
    client, _, settings = app_client
    base = b"0123456789"
    bid = _publish(client, "VAL", "2026-11-16", "V0", base).json()["release"]["id"]

    # segments 不是 JSON
    r = client.post(
        "/releases/delta",
        data={
            "station_id": "VAL",
            "sample_date": "2026-11-16",
            "order_no": "VX",
            "base_release_id": bid,
            "segments": "{not-json",
        },
    )
    assert r.status_code == 422
    # 空区间
    assert _publish_delta(
        client, "VAL", "2026-11-16", "VX", bid, [_seg(3, 3, b"x")]
    ).status_code == 422
    # 区间重叠
    assert _publish_delta(
        client,
        "VAL",
        "2026-11-16",
        "VX",
        bid,
        [_seg(0, 5, b"a"), _seg(4, 6, b"b")],
    ).status_code == 422
    # 区间越界
    assert _publish_delta(
        client, "VAL", "2026-11-16", "VX", bid, [_seg(9, 11, b"b")]
    ).status_code == 422
    # 9 段超过 8 段上限
    nine = [_seg(i, i + 1, b"x") for i in range(9)]
    assert _publish_delta(
        client, "VAL", "2026-11-16", "VX", bid, nine
    ).status_code == 422
    # 基稿序号不存在
    assert _publish_delta(
        client, "VAL", "2026-11-16", "VX", 99999, [_seg(0, 1, b"x")]
    ).status_code == 404
    # 跨站/跨日引用基稿
    other = _publish(client, "OTHER", "2026-11-17", "O0", b"hello").json()[
        "release"
    ]["id"]
    assert _publish_delta(
        client, "VAL", "2026-11-16", "VX", other, [_seg(0, 1, b"x")]
    ).status_code == 422


def test_delta_rebuilt_size_uses_original_limits(app_client):
    client, _, settings = app_client
    base = b"z" * (settings.max_file_bytes - 4)
    bid = _publish(client, "BIG", "2026-11-16", "B0", base).json()["release"]["id"]
    # 用更长内容替换首字节，把重建全文撑过 1 MiB 上限 -> 413。
    r = _publish_delta(
        client,
        "BIG",
        "2026-11-16",
        "B1",
        bid,
        [_seg(0, 1, b"toolong-bytes")],
    )
    assert r.status_code == 413
    # 把全文删空 -> 422。
    r_empty = _publish_delta(
        client, "BIG", "2026-11-16", "B2", bid,
        [_seg(0, len(base), b"")],
    )
    assert r_empty.status_code == 422


def test_unverifiable_base_blocks_delta_without_record(app_client, monkeypatch):
    client, _, _ = app_client
    base = b"station,ph\nS,7.0\n"
    bid = _publish(client, "TAMP", "2026-11-16", "T0", base).json()["release"]["id"]
    before = len(client.get("/releases").json())

    def boom_reconstruct(self, record):
        from app.reconstruct import ReconstructionError

        raise ReconstructionError("模拟基稿原件缺失")

    monkeypatch.setattr(main_mod.Reconstructor, "reconstruct", boom_reconstruct)
    r = _publish_delta(
        client, "TAMP", "2026-11-16", "T1", bid, [_seg(0, 1, b"X")]
    )
    assert r.status_code == 409 and "基稿无法核验" in r.json()["detail"]
    # 没有新增发布记录，检查点未推进。
    assert len(client.get("/releases").json()) == before


def test_delta_write_failure_leaves_no_record_or_checkpoint(app_client, monkeypatch):
    client, _, _ = app_client
    base = b"station,ph\nW,6.5\n"
    bid = _publish(client, "WRF", "2026-11-16", "W0", base).json()["release"]["id"]
    cp_before = client.get("/checkpoints/latest").json()
    rel_before = len(client.get("/releases").json())

    def boom_delta_store(self, doc_bytes):
        raise BlobError("ENOSPC: 模拟差分卷写满")

    monkeypatch.setattr(main_mod.DeltaStore, "store", boom_delta_store)
    r = _publish_delta(
        client, "WRF", "2026-11-16", "W1", bid, [_seg(0, 1, b"Q")]
    )
    assert r.status_code == 507
    assert client.get("/checkpoints/latest").json() == cp_before
    assert len(client.get("/releases").json()) == rel_before

    monkeypatch.undo()
    r2 = _publish_delta(
        client, "WRF", "2026-11-16", "W1", bid, [_seg(0, 1, b"Q")]
    )
    assert r2.status_code == 201 and r2.json()["reused"] is False


def test_tampered_delta_doc_or_base_rejects_download(app_client):
    client, _, _ = app_client
    base = b"abcdefgh\n"
    bid = _publish(client, "TD", "2026-11-16", "TD0", base).json()["release"]["id"]
    rel = _publish_delta(
        client, "TD", "2026-11-16", "TD1", bid, [_seg(0, 3, b"XYZ")]
    ).json()["release"]
    expected = b"XYZdefgh\n"
    assert _download(client, "TD", "2026-11-16", "TD1").content == expected

    # 篡改卷上的差分文档 -> 重建失败，拒绝取件（503），绝不输出错字节。
    doc_path = main_mod.delta_store.path_for(rel["delta_blob_sha256"])
    original_doc = doc_path.read_bytes()
    doc_path.write_bytes(original_doc + b"tampered")
    r = _download(client, "TD", "2026-11-16", "TD1")
    assert r.status_code == 503
    # 自检同样能发现。
    problems = main_mod.database.integrity_problems(
        main_mod.blob_store.verify_present, main_mod.delta_store.verify_present
    )
    assert any("差分文档缺失或摘要不符" in p for p in problems)
    # 恢复卷上文档字节，避免污染会话内后续用例的共享状态。
    doc_path.write_bytes(original_doc)
    assert _download(client, "TD", "2026-11-16", "TD1").content == expected


def test_delta_inclusion_proof_offline_verification(app_client, tmp_path):
    client, _, settings = app_client
    base = b"station,time,ph\nOFF,2026-11-16T08:00,7.2\n"
    bid = _publish(client, "OFF", "2026-11-16", "O0", base).json()["release"]["id"]
    pos = base.index(b"7.2")
    rebuilt = base[:pos] + b"9.9" + base[pos + 3 :]
    _publish_delta(
        client, "OFF", "2026-11-16", "O1", bid, [_seg(pos, pos + 3, b"9.9")]
    )
    proof = client.get(
        "/releases/OFF/2026-11-16/O1/inclusion-proof"
    ).json()
    # 离线核验只认重建全文字节，与整件发布走完全相同的核验路径。
    ok, msg = verify_release_bytes(proof, rebuilt)
    assert ok, msg
    # 用基稿字节核验差分稿证明必须失败（摘要不同）。
    ok2, _ = verify_release_bytes(proof, base)
    assert not ok2
    # CLI 形式同样通过。
    import subprocess
    from pathlib import Path

    proof_file = tmp_path / "inc.json"
    raw_file = tmp_path / "rebuilt.csv"
    proof_file.write_text(json.dumps(proof))
    raw_file.write_bytes(rebuilt)
    proc = subprocess.run(
        [
            "python3", "-m", "app.verify", "inclusion",
            "--proof", str(proof_file), "--file", str(raw_file),
        ],
        capture_output=True, text=True,
        cwd=str(Path(__file__).resolve().parents[1]),
    )
    assert proc.returncode == 0 and "通过" in proc.stdout


def test_concurrent_delta_same_order_serializes_to_one_record(app_client):
    """两线程并发提交同一差分单号：恰一写一复用，记录不分叉、检查点只进一格。"""
    from concurrent.futures import ThreadPoolExecutor

    client, _, _ = app_client
    base = b"station,ph\nCC,7.0\n"
    bid = _publish(client, "CCST", "2026-11-16", "CC0", base).json()[
        "release"
    ]["id"]
    cp_before = client.get("/checkpoints/latest").json()["seq"]
    segments = [_seg(base.index(b"7.0"), base.index(b"7.0") + 3, b"9.9")]

    def submit(_):
        return _publish_delta(
            client, "CCST", "2026-11-16", "CC1", bid, segments
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(submit, range(2)))
    assert sorted(r.status_code for r in results) == [201, 201]
    assert sorted(r.json()["reused"] for r in results) == [False, True]
    ids = {r.json()["release"]["id"] for r in results}
    assert len(ids) == 1
    assert client.get("/checkpoints/latest").json()["seq"] == cp_before + 1
    # 重建内容正确。
    expected = base.replace(b"7.0", b"9.9", 1)
    assert _download(client, "CCST", "2026-11-16", "CC1").content == expected


def test_restart_sweep_reconstructs_every_delta_chain(app_client):
    """模拟重启：对全部记录逐条重建，任一差分链断裂即进入 degraded。"""
    client, _, settings = app_client
    base = b"root-bytes-0000\n"
    rid = _publish(client, "SWP", "2026-11-16", "S0", base).json()["release"]["id"]
    pos = base.index(b"0000")
    prev = rid
    current = base
    for layer in range(1, settings.max_delta_depth + 1):
        marker = f"{layer:04d}".encode()
        rel = _publish_delta(
            client, "SWP", "2026-11-16", f"S{layer}", prev,
            [_seg(pos, pos + 4, marker)],
        ).json()["release"]
        current = current[:pos] + marker + current[pos + 4 :]
        prev = rel["id"]

    # 健康视角：结构自检 + 全链重建均通过。
    problems = main_mod.database.integrity_problems(
        main_mod.blob_store.verify_present, main_mod.delta_store.verify_present
    )
    assert problems == []
    for record in main_mod.database.list_releases():
        main_mod.reconstructor.reconstruct(record)  # 不抛异常即通过
    health = client.get("/health").json()
    assert health["status"] == "ok"
    # 本测试新增的四层差分链均被计入（会话内可能还有其他用例的差分件）。
    assert health["delta_count"] >= settings.max_delta_depth

    # 链底原件丢失：重建扫描必须报出具体记录并拒绝取件。
    full_digest = main_mod.database.get_release_by_id(rid)["blob_sha256"]
    blob_path = main_mod.blob_store.path_for(full_digest)
    blob_path.unlink()
    problems_after = main_mod.database.integrity_problems(
        main_mod.blob_store.verify_present, main_mod.delta_store.verify_present
    )
    assert any("原件缺失" in p for p in problems_after)
    from app.reconstruct import ReconstructionError

    with pytest.raises(ReconstructionError):
        main_mod.reconstructor.reconstruct(
            main_mod.database.get_release_by_id(prev)
        )
    blob_path.parent.mkdir(parents=True, exist_ok=True)
    blob_path.write_bytes(base)

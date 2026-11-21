"""差分纯逻辑测试：段解析校验、区间应用、规范化文档、链深规则。"""
from __future__ import annotations

import base64

import pytest

from app import delta


def b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def test_parse_segments_accepts_valid_sorted_and_adjacent():
    raw = [
        {"start": 10, "end": 12, "b64": b64(b"AB")},
        {"start": 0, "end": 5, "b64": b64(b"XYZ")},  # 乱序输入，输出按 start 排序
        {"start": 5, "end": 8, "b64": b64(b"")},     # 端点相接 + 空替换（删除）
    ]
    segs = delta.parse_segments(raw, max_segments=8)
    assert [s.start for s in segs] == [0, 5, 10]
    assert segs[2].replacement == b"AB"
    assert segs[1].replacement == b""


def test_parse_segments_rejects_bad_shapes():
    with pytest.raises(delta.DeltaValidationError):
        delta.parse_segments("not-list", 8)
    with pytest.raises(delta.DeltaValidationError):
        delta.parse_segments([], 8)  # 至少 1 段
    with pytest.raises(delta.DeltaValidationError):
        delta.parse_segments([{"start": 0, "end": 1}] * 9, 8)  # 超过 8 段
    with pytest.raises(delta.DeltaValidationError):
        delta.parse_segments([{"start": -1, "end": 2, "b64": b64(b"x")}], 8)
    with pytest.raises(delta.DeltaValidationError):
        delta.parse_segments([{"start": 3, "end": 3, "b64": b64(b"x")}], 8)  # 空区间
    with pytest.raises(delta.DeltaValidationError):
        delta.parse_segments([{"start": 5, "end": 2, "b64": b64(b"x")}], 8)
    with pytest.raises(delta.DeltaValidationError):
        delta.parse_segments([{"start": 0, "end": 2, "b64": "not base64!!"}], 8)
    with pytest.raises(delta.DeltaValidationError):
        delta.parse_segments([{"start": 0, "end": 2, "b64": "ABC"}], 8)  # 非法长度
    # bool 不能冒充整数
    with pytest.raises(delta.DeltaValidationError):
        delta.parse_segments([{"start": True, "end": 2, "b64": b64(b"x")}], 8)


def test_parse_segments_rejects_overlap():
    raw = [
        {"start": 0, "end": 5, "b64": b64(b"a")},
        {"start": 4, "end": 8, "b64": b64(b"b")},  # 与前段重叠
    ]
    with pytest.raises(delta.DeltaValidationError, match="重叠"):
        delta.parse_segments(raw, 8)


def test_validate_ranges_against_base_size():
    segs = delta.parse_segments(
        [{"start": 8, "end": 11, "b64": b64(b"xyz")}], 8
    )
    with pytest.raises(delta.DeltaValidationError, match="超出基稿长度"):
        delta.validate_ranges(segs, base_size=10)
    # end 恰等于基稿长度合法
    delta.validate_ranges(segs, base_size=11)


def test_apply_segments_grow_shrink_delete_and_identity():
    base = b"0123456789"
    # 变长：扩大
    segs = delta.parse_segments(
        [{"start": 2, "end": 4, "b64": b64(b"ABCD")}], 8
    )
    assert delta.apply_segments(base, segs) == b"01ABCD456789"
    # 缩小：替换为空即删除
    segs = delta.parse_segments([{"start": 2, "end": 5, "b64": b64(b"")}], 8)
    assert delta.apply_segments(base, segs) == b"0156789"
    # 等长替换
    segs = delta.parse_segments(
        [{"start": 0, "end": 3, "b64": b64(b"abc")}], 8
    )
    assert delta.apply_segments(base, segs) == b"abc3456789"
    # 多段联合
    segs = delta.parse_segments(
        [
            {"start": 1, "end": 3, "b64": b64(b"X")},
            {"start": 7, "end": 10, "b64": b64(b"YZ")},
        ],
        8,
    )
    assert delta.apply_segments(base, segs) == b"0X3456YZ"


def test_canonical_doc_is_stable_and_self_describing():
    segs = delta.parse_segments(
        [{"start": 0, "end": 1, "b64": b64(b"Z")}], 8
    )
    doc1 = delta.canonical_delta_doc(7, "a" * 64, 100, segs)
    doc2 = delta.canonical_delta_doc(7, "a" * 64, 100, segs)
    assert doc1 == doc2  # 同输入同字节 -> 内容寻址可复用
    import json

    parsed = json.loads(doc1)
    assert parsed["v"] == delta.DELTA_DOC_VERSION
    assert parsed["base_release_id"] == 7
    assert parsed["base_size"] == 100
    # 落盘文档可被重新解析并还原为同样的段
    back = delta.parse_delta_doc(doc1)
    assert back["base_release_id"] == 7
    assert back["segments"][0].replacement == b"Z"


def test_parse_delta_doc_rejects_corruption():
    with pytest.raises(delta.DeltaValidationError):
        delta.parse_delta_doc(b"{not json")
    segs = delta.parse_segments(
        [{"start": 0, "end": 2, "b64": b64(b"ok")}], 8
    )
    good = delta.canonical_delta_doc(1, "a" * 64, 5, segs)
    # 区间越界的文档（base_size 被改小）
    bad = good.replace(b'"base_size":5', b'"base_size":1')
    with pytest.raises(delta.DeltaValidationError):
        delta.parse_delta_doc(bad)

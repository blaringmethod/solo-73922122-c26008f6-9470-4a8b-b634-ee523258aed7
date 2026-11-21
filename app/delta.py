"""差分发布纯逻辑：替换段校验、字节区间应用、规范化差分文档。

差分语义
--------
* 一件差分稿引用同站同日的一件**基稿**（全文或另一件差分稿），携带 1~8 个
  互不重叠的非空字节区间 ``[start, end)``（左闭右开）与 Base64 替换内容；
* 应用时按区间升序，把基稿字节中的该区间整体替换为解码后的新字节（允许等长、
  变长替换，替换内容允许为空字节串 -> 区间删除）；
* 服务只保存差分文档与基稿引用，绝不保存重建后的全文；
* 每件差分稿登记的摘要与长度均针对**重建全文**，因此 Merkle 叶子、纳入证明与
  离线核验与整件发布完全一致。

本模块只做无 IO 的纯计算，存储与链式还原分别位于 :mod:`app.storage` 与
:mod:`app.reconstruct`。
"""
from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
from dataclasses import dataclass

DELTA_DOC_VERSION = 1
# 仅接受标准 Base64 字母表；允许无填充/有填充，拒绝任何其他字符（含空白）。
_B64_RE = re.compile(r"^[A-Za-z0-9+/]*={0,2}$")


class DeltaValidationError(ValueError):
    """差分请求结构或区间不合法（对应 HTTP 422）。"""


@dataclass(frozen=True)
class Segment:
    """一个替换段：替换基稿 ``[start, end)`` 的非空字节区间。"""

    start: int
    end: int
    replacement: bytes

    def to_doc(self) -> dict:
        return {
            "start": self.start,
            "end": self.end,
            "b64": base64.b64encode(self.replacement).decode("ascii"),
        }


def parse_segments(raw: object, max_segments: int) -> list[Segment]:
    """解析并校验请求中的 segments JSON。

    规则：
      * 必须是 1~max_segments 个对象的数组；
      * start/end 为非负整数且 start < end（非空区间）；
      * b64 为合法标准 Base64（解码后可为空串，即删除该区间）；
      * 区间两两互不重叠（端点相接允许），并按 start 升序返回。
    """
    if not isinstance(raw, list):
        raise DeltaValidationError("segments 必须是 JSON 数组")
    if not 1 <= len(raw) <= max_segments:
        raise DeltaValidationError(
            f"替换段数量须在 1~{max_segments} 之间，实际 {len(raw)} 段"
        )
    segments: list[Segment] = []
    for idx, item in enumerate(raw):
        if not isinstance(item, dict):
            raise DeltaValidationError(f"第 {idx + 1} 段必须是对象")
        try:
            start = item["start"]
            end = item["end"]
            b64 = item["b64"]
        except (KeyError, TypeError):
            raise DeltaValidationError(
                f"第 {idx + 1} 段须包含 start、end、b64 三个字段"
            )
        if not isinstance(start, int) or not isinstance(end, int):
            # bool 是 int 子类，显式排除，避免 True/1 歧义。
            raise DeltaValidationError(f"第 {idx + 1} 段 start/end 须为整数")
        if isinstance(start, bool) or isinstance(end, bool):
            raise DeltaValidationError(f"第 {idx + 1} 段 start/end 须为整数")
        if start < 0 or end < 0 or start >= end:
            raise DeltaValidationError(
                f"第 {idx + 1} 段须满足 0 <= start < end（非空字节区间）"
            )
        if not isinstance(b64, str) or not _B64_RE.match(b64) or len(b64) % 4 == 1:
            raise DeltaValidationError(f"第 {idx + 1} 段 b64 不是合法标准 Base64")
        try:
            replacement = base64.b64decode(b64)
        except (binascii.Error, ValueError):
            raise DeltaValidationError(f"第 {idx + 1} 段 Base64 解码失败")
        segments.append(Segment(start=start, end=end, replacement=replacement))

    segments.sort(key=lambda s: s.start)
    for prev, curr in zip(segments, segments[1:]):
        if curr.start < prev.end:
            raise DeltaValidationError(
                f"替换区间重叠：[{prev.start},{prev.end}) 与 "
                f"[{curr.start},{curr.end}) 不允许重叠"
            )
    return segments


def validate_ranges(segments: list[Segment], base_size: int) -> None:
    """校验全部区间落在基稿字节范围内（end 不超过基稿长度）。"""
    for seg in segments:
        if seg.end > base_size:
            raise DeltaValidationError(
                f"替换区间 [{seg.start},{seg.end}) 超出基稿长度 {base_size}"
            )


def apply_segments(base: bytes, segments: list[Segment]) -> bytes:
    """把替换段应用到基稿字节，返回重建全文。

    调用前须经 :func:`parse_segments` 与 :func:`validate_ranges` 校验。
    """
    parts: list[bytes] = []
    cursor = 0
    for seg in sorted(segments, key=lambda s: s.start):
        parts.append(base[cursor:seg.start])
        parts.append(seg.replacement)
        cursor = seg.end
    parts.append(base[cursor:])
    return b"".join(parts)


def canonical_delta_doc(
    base_release_id: int,
    base_blob_sha256: str,
    base_size: int,
    segments: list[Segment],
) -> bytes:
    """生成落盘的规范化差分文档字节（键排序、无空白）。

    文档自描述基稿引用与全部替换段；相同输入恒产生相同字节，
    因此差分文档同样内容寻址，同单重试可直接复用。
    """
    document = {
        "v": DELTA_DOC_VERSION,
        "base_release_id": base_release_id,
        "base_blob_sha256": base_blob_sha256,
        "base_size": base_size,
        "segments": [s.to_doc() for s in sorted(segments, key=lambda s: s.start)],
    }
    return json.dumps(
        document, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def parse_delta_doc(raw: bytes) -> dict:
    """解析落盘差分文档并做结构自检（损坏文档在还原时被拒绝）。"""
    try:
        doc = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DeltaValidationError(f"差分文档不是合法 JSON：{exc}")
    if not isinstance(doc, dict) or doc.get("v") != DELTA_DOC_VERSION:
        raise DeltaValidationError("差分文档版本不受支持或结构损坏")
    try:
        base_id = int(doc["base_release_id"])
        base_digest = str(doc["base_blob_sha256"])
        base_size = int(doc["base_size"])
        raw_segments = doc["segments"]
    except (KeyError, TypeError, ValueError):
        raise DeltaValidationError("差分文档字段不完整或类型错误")
    if base_id <= 0 or base_size < 0 or len(base_digest) != 64:
        raise DeltaValidationError("差分文档基稿引用不合法")
    segments = parse_segments(raw_segments, max_segments=8)
    validate_ranges(segments, base_size)
    return {
        "base_release_id": base_id,
        "base_blob_sha256": base_digest,
        "base_size": base_size,
        "segments": segments,
    }


def delta_doc_sha256(doc_bytes: bytes) -> str:
    return hashlib.sha256(doc_bytes).hexdigest()

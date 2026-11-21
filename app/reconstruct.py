"""差分链还原：沿基稿引用逐层重建全文，并逐跳核验摘要。

不变量
------
* 全文件 ``kind='full'``（chain_depth=0）直接读原件；
* 差分件 ``kind='delta'`` 读取自身差分文档，先还原基稿再应用替换段；
* 每一跳都核验：
    - 差分文档登记的基稿摘要 == 库中基稿记录登记的重建全文摘要；
    - 基稿实际重建字节的 SHA-256 == 该登记摘要（防卷上篡改/缺件）；
    - 重建结果 SHA-256 == 当前记录登记摘要、长度 == 登记长度；
* 一件差分稿从其基稿到自身最多应用 ``max_delta_depth``（默认 4）层差分；
  链不完整、文档损坏、任一跳核验失败都抛 :class:`ReconstructionError`，
  调用方必须拒绝取件，绝不输出无法核验的字节。
"""
from __future__ import annotations

import hashlib

from . import delta as delta_mod
from .config import Settings
from .storage import BlobStore, DeltaStore


class ReconstructionError(Exception):
    """重建失败：缺件、文档损坏、摘要不符或链深异常。拒绝取件。"""


class Reconstructor:
    def __init__(
        self,
        settings: Settings,
        database,
        blob_store: BlobStore,
        delta_store: DeltaStore,
    ):
        self._settings = settings
        self._db = database
        self._blobs = blob_store
        self._deltas = delta_store

    def reconstruct(self, record: dict) -> bytes:
        """按记录重建全文字节。失败抛 :class:`ReconstructionError`。"""
        kind = record.get("kind", "full")
        if kind == "full":
            return self._read_full(record)
        if kind == "delta":
            return self._reconstruct_delta(record)
        raise ReconstructionError(
            f"release id={record['id']} 出现未知 kind={kind!r}"
        )

    # ------------------------------------------------------------------ #
    def _read_full(self, record: dict) -> bytes:
        digest = record["blob_sha256"]
        if not self._blobs.verify_present(digest):
            raise ReconstructionError(
                f"release id={record['id']} 的原件缺失或摘要不符"
            )
        data = self._blobs.read(digest)
        self._check_record_digest(record, data)
        return data

    def _reconstruct_delta(self, record: dict) -> bytes:
        depth = record["chain_depth"]
        if not 1 <= depth <= self._settings.max_delta_depth:
            raise ReconstructionError(
                f"release id={record['id']} 差分链深 {depth} 越界"
            )
        # 沿基稿链向下收集（含自身），应用层数恰为 depth，上限四层。
        chain: list[dict] = []
        current = record
        seen: set[int] = set()
        while current.get("kind", "full") == "delta":
            if current["id"] in seen:
                raise ReconstructionError("差分链出现环，拒绝重建")
            seen.add(current["id"])
            chain.append(current)
            base_id = current["base_id"]
            if base_id is None:
                raise ReconstructionError(
                    f"release id={current['id']} 缺少基稿引用"
                )
            # 基稿必须是更早的发布：既符合只增不改语义，也保证链严格收敛。
            if base_id >= current["id"]:
                raise ReconstructionError(
                    f"release id={current['id']} 的基稿序号 {base_id} 不早于自身"
                )
            base = self._db.get_release_by_id(base_id)
            if base is None:
                raise ReconstructionError(
                    f"release id={current['id']} 的基稿 id={base_id} 不存在"
                )
            current = base
        # current 即链底全文件。
        full_record = current
        applied_layers = len(chain)
        if applied_layers > self._settings.max_delta_depth:
            raise ReconstructionError(
                f"还原需应用 {applied_layers} 层差分，"
                f"超过上限 {self._settings.max_delta_depth} 层"
            )
        if full_record.get("kind", "full") != "full":
            raise ReconstructionError("差分链底端不是全文件")
        if full_record["chain_depth"] != 0:
            raise ReconstructionError("全文件链深必须为 0")

        data = self._read_full(full_record)
        # 自底向上逐层应用：chain 当前为 [顶层, ..., 紧邻全文层]，反转后应用。
        for delta_record in reversed(chain):
            data = self._apply_one(delta_record, data)
        self._check_record_digest(record, data)
        return data

    def _apply_one(self, delta_record: dict, base_bytes: bytes) -> bytes:
        rid = delta_record["id"]
        doc_digest = delta_record["delta_blob_sha256"]
        if not doc_digest or not self._deltas.verify_present(doc_digest):
            raise ReconstructionError(
                f"release id={rid} 的差分文档缺失或摘要不符"
            )
        try:
            doc = delta_mod.parse_delta_doc(self._deltas.read(doc_digest))
        except delta_mod.DeltaValidationError as exc:
            raise ReconstructionError(
                f"release id={rid} 差分文档损坏：{exc}"
            ) from exc

        base_id = delta_record["base_id"]
        base_record = self._db.get_release_by_id(base_id)
        if base_record is None:
            raise ReconstructionError(f"release id={rid} 的基稿 id={base_id} 不存在")
        # 跳 1：文档登记的基稿摘要必须与库内基稿记录一致。
        if doc["base_blob_sha256"] != base_record["blob_sha256"]:
            raise ReconstructionError(
                f"release id={rid} 差分文档的基稿摘要与目录记录不一致"
            )
        if doc["base_release_id"] != base_id:
            raise ReconstructionError(
                f"release id={rid} 差分文档的基稿序号与目录记录不一致"
            )
        # 跳 2：实际基稿字节必须与登记摘要一致（base_bytes 已在上游核验）。
        actual_base_digest = hashlib.sha256(base_bytes).hexdigest()
        if actual_base_digest != doc["base_blob_sha256"]:
            raise ReconstructionError(
                f"release id={rid} 的基稿重建字节摘要不符，拒绝向上还原"
            )
        if doc["base_size"] != len(base_bytes):
            raise ReconstructionError(
                f"release id={rid} 的基稿长度与差分文档登记不符"
            )
        try:
            rebuilt = delta_mod.apply_segments(base_bytes, doc["segments"])
        except delta_mod.DeltaValidationError as exc:
            raise ReconstructionError(f"release id={rid} 应用替换段失败：{exc}")
        # 跳 3：本层重建结果必须与该差分记录登记的全文摘要/长度一致。
        self._check_record_digest(delta_record, rebuilt)
        return rebuilt

    @staticmethod
    def _check_record_digest(record: dict, data: bytes) -> None:
        digest = hashlib.sha256(data).hexdigest()
        if digest != record["blob_sha256"]:
            raise ReconstructionError(
                f"release id={record['id']} 重建摘要 {digest[:12]}... "
                f"与登记 {record['blob_sha256'][:12]}... 不一致"
            )
        if len(data) != record["size"]:
            raise ReconstructionError(
                f"release id={record['id']} 重建长度 {len(data)} "
                f"与登记 {record['size']} 不一致"
            )

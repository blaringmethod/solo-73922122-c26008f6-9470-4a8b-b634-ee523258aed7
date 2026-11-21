"""原件与差分文档存储：只写不改、内容寻址、原子落盘。

* 原件按 SHA-256 摘要分片存放（``blobs/ab/cd/<digest>``），数据库只保存目录与摘要；
* 差分文档按自身 SHA-256 存放（``deltas/ab/cd/<digest>``），文档内含基稿引用与
  替换段，服务不保存任何重建全文；
* 先写临时文件并 ``fsync``，再 ``os.replace`` 原子改名，随后 ``fsync`` 目录；
* 任何一步失败都不会留下被数据库引用的缺件，失败后可安全重试。
"""
from __future__ import annotations

import hashlib
import os
import uuid
from pathlib import Path

from .config import Settings


class BlobError(Exception):
    """写盘失败（空间、权限、IO 等）。"""


class _ContentStore:
    """内容寻址原子存储的通用实现（原件与差分文档共用）。"""

    def __init__(self, root: Path, temp_dir: Path):
        self._root = root
        self._temp_dir = temp_dir

    def _target_path(self, digest: str) -> Path:
        return self._root / digest[:2] / digest[2:4] / digest

    def path_for(self, digest: str) -> Path:
        return self._target_path(digest)

    def exists(self, digest: str) -> bool:
        return self._target_path(digest).is_file()

    def store(self, data: bytes) -> tuple[str, Path]:
        """原子写入，返回 (sha256, 最终路径)。

        同摘要重复写入直接复用，绝不覆盖既有内容。
        """
        digest = hashlib.sha256(data).hexdigest()
        target = self._target_path(digest)
        if target.is_file():
            # 复用同内容文件（同单同输入重试，或不同单据的相同字节）。
            return digest, target
        target.parent.mkdir(parents=True, exist_ok=True)
        # 临时名带 uuid：同进程多线程/多实例并发写同一摘要时不会互踩临时文件；
        # os.replace 对已存在目标为原子覆盖，内容寻址保证目标字节必然相同。
        tmp_path = self._temp_dir / (
            f".upload-{digest}-{os.getpid()}-{uuid.uuid4().hex}"
        )
        try:
            with open(tmp_path, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, target)
            self._fsync_dir(target.parent)
        except OSError as exc:
            tmp_path.unlink(missing_ok=True)
            raise BlobError(f"写盘失败: {exc}") from exc
        return digest, target

    def read(self, digest: str) -> bytes:
        return self._target_path(digest).read_bytes()

    def verify_present(self, digest: str) -> bool:
        """启动自检用：摘要与文件实际字节一致且存在。"""
        path = self._target_path(digest)
        if not path.is_file():
            return False
        return hashlib.sha256(path.read_bytes()).hexdigest() == digest

    @staticmethod
    def _fsync_dir(path: Path) -> None:
        try:
            fd = os.open(path, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


class BlobStore:
    def __init__(self, settings: Settings):
        self._settings = settings
        settings.ensure_dirs()
        self._store = _ContentStore(settings.blob_dir, settings.temp_dir)

    def path_for(self, digest: str) -> Path:
        return self._store.path_for(digest)

    def exists(self, digest: str) -> bool:
        return self._store.exists(digest)

    def store(self, data: bytes) -> tuple[str, Path]:
        """原子写入原件，返回 (sha256, 最终路径)。"""
        return self._store.store(data)

    def read(self, digest: str) -> bytes:
        return self._store.read(digest)

    def verify_present(self, digest: str) -> bool:
        return self._store.verify_present(digest)


class DeltaStore:
    """差分文档存储：只存差分与基稿引用，不存重建全文。"""

    def __init__(self, settings: Settings):
        self._settings = settings
        settings.ensure_dirs()
        self._store = _ContentStore(settings.delta_dir, settings.temp_dir)

    def path_for(self, digest: str) -> Path:
        return self._store.path_for(digest)

    def exists(self, digest: str) -> bool:
        return self._store.exists(digest)

    def store(self, doc_bytes: bytes) -> tuple[str, Path]:
        """原子写入差分文档，返回 (文档 sha256, 最终路径)。"""
        return self._store.store(doc_bytes)

    def read(self, digest: str) -> bytes:
        return self._store.read(digest)

    def verify_present(self, digest: str) -> bool:
        return self._store.verify_present(digest)

"""SQLite 持久层：目录与摘要（不存原件/全文字节）。

并发模型
--------
* WAL 模式 + ``busy_timeout``：两个实例共享同一库文件，读者不阻塞写者；
* 发布事务使用 ``BEGIN IMMEDIATE``，跨实例写操作被数据库锁串行化，
  后到者阻塞等待后重检唯一键，因此不会分叉、不会互相覆盖。
* 无任何 UPDATE/DELETE 路径，发布后记录不可改、不可删。

差分发布
--------
``releases`` 同时登记整件（``kind='full'``）与差分件（``kind='delta'``）。
差分件的 ``blob_sha256/size`` 始终针对**重建全文**，因此 Merkle 叶子载荷、
纳入证明与离线核验与整件发布完全一致；差分文档自身摘要存于
``delta_blob_sha256``，基稿引用存于 ``base_id``，``chain_depth`` 记录差分层数
（全文 0，差分 1~4）。
"""
from __future__ import annotations

import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterator

from . import merkle
from .config import Settings

SCHEMA = """
CREATE TABLE IF NOT EXISTS releases (
    id                  INTEGER PRIMARY KEY,          -- 1 起稠密序号，叶子下标 = id - 1
    station_id          TEXT NOT NULL,
    sample_date         TEXT NOT NULL,
    order_no            TEXT NOT NULL,
    blob_sha256         TEXT NOT NULL,                -- 重建全文（或原件）的 SHA-256
    size                INTEGER NOT NULL,             -- 重建全文（或原件）字节长度
    created_at          TEXT NOT NULL,
    kind                TEXT NOT NULL DEFAULT 'full', -- full | delta
    base_id             INTEGER,                      -- 差分件基稿 release id
    delta_blob_sha256   TEXT,                         -- 差分文档自身 SHA-256
    chain_depth         INTEGER NOT NULL DEFAULT 0,   -- 全文 0，差分 1..4
    UNIQUE(station_id, sample_date, order_no),
    FOREIGN KEY(base_id) REFERENCES releases(id)
);

CREATE TABLE IF NOT EXISTS checkpoints (
    seq         INTEGER PRIMARY KEY,            -- 该检查点对应的条数
    root_hex    TEXT NOT NULL,
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS meta (
    key    TEXT PRIMARY KEY,
    value  TEXT NOT NULL
);
"""

# 旧库（v1 表结构）升级所需的新增列：列名 -> DDL 片段。
_MIGRATION_COLUMNS = {
    "kind": "ALTER TABLE releases ADD COLUMN kind TEXT NOT NULL DEFAULT 'full'",
    "base_id": "ALTER TABLE releases ADD COLUMN base_id INTEGER",
    "delta_blob_sha256": "ALTER TABLE releases ADD COLUMN delta_blob_sha256 TEXT",
    "chain_depth": "ALTER TABLE releases ADD COLUMN chain_depth INTEGER NOT NULL DEFAULT 0",
}


def _utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class ConflictError(Exception):
    """业务冲突：异输入重试或超出累计上限。"""


class Database:
    def __init__(self, settings: Settings):
        self._settings = settings
        settings.ensure_dirs()
        self._init_schema()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(
            self._settings.db_path,
            timeout=15.0,
            isolation_level=None,  # 手动事务，显式 BEGIN/COMMIT
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout = 15000")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = FULL")
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            yield conn
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.executescript(SCHEMA)
                # 旧库平滑升级：补齐差分功能新增列，已有记录全部视为全文件。
                existing = {
                    row["name"]
                    for row in conn.execute("PRAGMA table_info(releases)").fetchall()
                }
                for column, ddl in _MIGRATION_COLUMNS.items():
                    if column not in existing:
                        conn.execute(ddl)
                # 零号检查点（空树根），幂等创建。
                empty = merkle.empty_root(self._settings.tree_depth).hex()
                conn.execute(
                    "INSERT OR IGNORE INTO checkpoints(seq, root_hex, created_at) "
                    "VALUES (0, ?, ?)",
                    (empty, _utcnow()),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    # ------------------------------------------------------------------ #
    # 发布
    # ------------------------------------------------------------------ #
    def publish(
        self,
        station_id: str,
        sample_date: str,
        order_no: str,
        blob_sha256: str,
        size: int,
    ) -> tuple[dict, bool]:
        """串行化整件发布。返回 (发布记录, 是否复用既有记录)。

        调用方必须已把原件原子落盘成功后再调用本方法，
        从而保证“库中有记录 => 卷上有完整原件”。
        """
        return self._publish_with_retry(
            station_id=station_id,
            sample_date=sample_date,
            order_no=order_no,
            blob_sha256=blob_sha256,
            size=size,
            kind="full",
            base_id=None,
            delta_blob_sha256=None,
            chain_depth=0,
        )

    def publish_delta(
        self,
        station_id: str,
        sample_date: str,
        order_no: str,
        rebuilt_sha256: str,
        rebuilt_size: int,
        base_id: int,
        delta_doc_sha256: str,
        chain_depth: int,
    ) -> tuple[dict, bool]:
        """串行化差分发布。

        调用方必须已：① 成功还原并核验基稿；② 把差分文档原子落盘；
        ③ 在本地算出重建全文摘要与长度。本方法只做目录登记与检查点追加，
        失败时不会产生任何记录，差分文档为内容寻址的孤立文件，可安全重试。
        """
        return self._publish_with_retry(
            station_id=station_id,
            sample_date=sample_date,
            order_no=order_no,
            blob_sha256=rebuilt_sha256,
            size=rebuilt_size,
            kind="delta",
            base_id=base_id,
            delta_blob_sha256=delta_doc_sha256,
            chain_depth=chain_depth,
        )

    def _publish_with_retry(self, **fields) -> tuple[dict, bool]:
        # 双实例并发时 BEGIN IMMEDIATE 可能撞上写锁；busy_timeout 之外再有限重试。
        for attempt in range(3):
            try:
                return self._publish_once(**fields)
            except sqlite3.OperationalError as exc:
                if "locked" in str(exc).lower() and attempt < 2:
                    time.sleep(0.2 * (attempt + 1))
                    continue
                raise
        raise RuntimeError("unreachable")  # pragma: no cover

    def _publish_once(
        self,
        station_id: str,
        sample_date: str,
        order_no: str,
        blob_sha256: str,
        size: int,
        kind: str,
        base_id: int | None,
        delta_blob_sha256: str | None,
        chain_depth: int,
    ) -> tuple[dict, bool]:
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT * FROM releases "
                    "WHERE station_id = ? AND sample_date = ? AND order_no = ?",
                    (station_id, sample_date, order_no),
                ).fetchone()
                if row is not None:
                    record = dict(row)
                    same = (
                        record["blob_sha256"] == blob_sha256
                        and record["size"] == size
                    )
                    conn.rollback()  # 只读复用，不产生写入
                    if not same:
                        raise ConflictError(
                            "同一站号/日期/发布单号已存在且重建内容不同，禁止改写"
                        )
                    return record, True

                count = conn.execute(
                    "SELECT COUNT(*) AS c FROM releases"
                ).fetchone()["c"]
                if count >= self._settings.max_releases:
                    conn.rollback()
                    raise ConflictError(
                        f"累计发布已达上限 {self._settings.max_releases} 件"
                    )

                cursor = conn.execute(
                    "INSERT INTO releases(station_id, sample_date, order_no, "
                    "blob_sha256, size, created_at, kind, base_id, "
                    "delta_blob_sha256, chain_depth) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        station_id,
                        sample_date,
                        order_no,
                        blob_sha256,
                        size,
                        _utcnow(),
                        kind,
                        base_id,
                        delta_blob_sha256,
                        chain_depth,
                    ),
                )
                release_id = cursor.lastrowid
                # 无删除路径，rowid 必然稠密且等于新条数。
                assert release_id == count + 1

                leaves = self._leaf_hashes_locked(conn)
                root = merkle.root_from_leaves(leaves, self._settings.tree_depth)
                conn.execute(
                    "INSERT INTO checkpoints(seq, root_hex, created_at) "
                    "VALUES (?, ?, ?)",
                    (len(leaves), root.hex(), _utcnow()),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

        record = self.get_release_by_id(release_id)
        return record, False

    @staticmethod
    def _leaf_hashes_locked(conn: sqlite3.Connection) -> list[bytes]:
        # 叶子只依赖重建全文摘要与长度，全文件/差分件载荷格式完全一致。
        rows = conn.execute(
            "SELECT station_id, sample_date, order_no, blob_sha256, size "
            "FROM releases ORDER BY id ASC"
        ).fetchall()
        return [
            merkle.leaf_hash(
                r["station_id"],
                r["sample_date"],
                r["order_no"],
                r["blob_sha256"],
                r["size"],
            )
            for r in rows
        ]

    def leaf_hashes(self) -> list[bytes]:
        with self.connect() as conn:
            return self._leaf_hashes_locked(conn)

    # ------------------------------------------------------------------ #
    # 查询
    # ------------------------------------------------------------------ #
    def get_release(self, station_id: str, sample_date: str, order_no: str):
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM releases WHERE station_id = ? AND sample_date = ? "
                "AND order_no = ?",
                (station_id, sample_date, order_no),
            ).fetchone()
            return dict(row) if row else None

    def get_release_by_id(self, release_id: int):
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM releases WHERE id = ?", (release_id,)
            ).fetchone()
            return dict(row) if row else None

    def list_releases(self) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM releases ORDER BY id ASC").fetchall()
            return [dict(r) for r in rows]

    def latest_checkpoint(self) -> dict:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM checkpoints ORDER BY seq DESC LIMIT 1"
            ).fetchone()
            return dict(row)

    def get_checkpoint(self, seq: int):
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM checkpoints WHERE seq = ?", (seq,)
            ).fetchone()
            return dict(row) if row else None

    def list_checkpoints(self) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM checkpoints ORDER BY seq ASC"
            ).fetchall()
            return [dict(r) for r in rows]

    # ------------------------------------------------------------------ #
    # 启动自检：不得带着缺件记录对外服务
    # ------------------------------------------------------------------ #
    def integrity_problems(self, blob_verifier, delta_verifier=None) -> list[str]:
        """结构级自检：序号稠密、原件/差分文档齐全且摘要相符、检查点连续。

        差分链的逐跳重建核验由调用方（app.main）通过 Reconstructor 完成，
        本方法只保证目录引用的每个卷上文件都存在且内容寻址摘要正确。
        """
        problems: list[str] = []
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT id, kind, blob_sha256, delta_blob_sha256, base_id, "
                "chain_depth FROM releases ORDER BY id"
            ).fetchall()
            expected_ids = list(range(1, len(rows) + 1))
            actual_ids = [r["id"] for r in rows]
            if actual_ids != expected_ids:
                problems.append("releases 主键不稠密，存在缺口或乱序")

            known_ids = set(actual_ids)
            for row in rows:
                kind = row["kind"]
                if kind not in ("full", "delta"):
                    problems.append(f"release id={row['id']} kind 非法：{kind!r}")
                    continue
                if kind == "full":
                    if row["chain_depth"] != 0 or row["base_id"] is not None:
                        problems.append(
                            f"release id={row['id']} 全文件链深/基稿引用字段异常"
                        )
                    if not blob_verifier(row["blob_sha256"]):
                        problems.append(
                            f"release id={row['id']} 的原件缺失或摘要不符 "
                            f"(sha256={row['blob_sha256'][:12]}...)"
                        )
                else:
                    if not 1 <= row["chain_depth"] <= self._settings.max_delta_depth:
                        problems.append(
                            f"release id={row['id']} 差分链深 "
                            f"{row['chain_depth']} 超出 1..{self._settings.max_delta_depth}"
                        )
                    if row["base_id"] is None or row["base_id"] not in known_ids:
                        problems.append(
                            f"release id={row['id']} 的基稿引用缺失或悬空"
                        )
                    elif row["base_id"] >= row["id"]:
                        problems.append(
                            f"release id={row['id']} 基稿序号必须早于自身序号"
                        )
                    if not row["delta_blob_sha256"]:
                        problems.append(
                            f"release id={row['id']} 缺少差分文档摘要"
                        )
                    elif delta_verifier is not None and not delta_verifier(
                        row["delta_blob_sha256"]
                    ):
                        problems.append(
                            f"release id={row['id']} 的差分文档缺失或摘要不符 "
                            f"(sha256={row['delta_blob_sha256'][:12]}...)"
                        )

            leaves = self._leaf_hashes_locked(conn)
            root = merkle.root_from_leaves(leaves, self._settings.tree_depth)
            cp_rows = conn.execute(
                "SELECT seq, root_hex FROM checkpoints ORDER BY seq"
            ).fetchall()
            cp_seqs = [r["seq"] for r in cp_rows]
            if cp_seqs != list(range(len(cp_seqs))):
                problems.append("checkpoints 序号不连续")
            latest = cp_rows[-1]
            if latest["seq"] != len(leaves) or latest["root_hex"] != root.hex():
                problems.append("最新检查点与实际叶子集合不一致")
        return problems

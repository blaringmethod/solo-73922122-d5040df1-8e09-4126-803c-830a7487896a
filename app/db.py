"""SQLite 存取层：批次、清单号段、增量覆盖段与按帧追查索引。

设计要点：
- 帧号以 64 位有符号整数存储（可跨十亿），存储单位是号段而不是帧。
- ``coverage`` 表缓存每侧已合并的覆盖段，登记/更正/撤回清单时
  只对受影响区间做局部重算，不重扫整批清单。
- ``frame_index`` 是号段到帧的倒排索引：用覆盖整个 int64 域（65 层）的
  区段树做规范分解，把一张清单的号段拆成至多 128 个树节点（每张清单
  固定上限），按 ``(batch_id, node_key)`` 建立主键。查一帧只需沿树根到
  叶的 65 个节点键做点查，命中的索引项全部覆盖该帧，读量只随命中清单
  数增长；长号段按号段分解，绝不逐帧展开。
- 所有写操作在单个事务（BEGIN IMMEDIATE）内完成：清单、覆盖段、帧索引
  同时生效；越界、入库或索引写入失败自动回滚，保留调用前原状。
- 旧库升级：``init_db`` 通过 ``PRAGMA user_version`` 检测并从 ``manifests``
  回填索引，升级后原有清单可直接按帧追查。
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from typing import List, Optional, Sequence, Tuple

from . import intervals

DEFAULT_DB_PATH = os.environ.get(
    "QC_DB_PATH", os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "qc.db")
)

VALID_SIDES = ("collector", "library")

# 帧号仍支持有符号 64 位整数；索引树覆盖整个 int64 域。
INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1
DOMAIN_SIZE_BITS = 64
DOMAIN_LEVELS = DOMAIN_SIZE_BITS + 1  # 第 0 层为根，第 64 层为单帧叶子

SCHEMA_VERSION = 2

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS batches (
    batch_id   TEXT PRIMARY KEY,
    frame_start INTEGER NOT NULL,
    frame_end   INTEGER NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    CHECK (frame_end >= frame_start)
);

CREATE TABLE IF NOT EXISTS manifests (
    manifest_no TEXT PRIMARY KEY,
    batch_id    TEXT NOT NULL REFERENCES batches(batch_id),
    side        TEXT NOT NULL CHECK (side IN ('collector', 'library')),
    status      TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'withdrawn')),
    seg_start   INTEGER NOT NULL,
    seg_end     INTEGER NOT NULL,
    updated_at  TEXT NOT NULL DEFAULT (datetime('now')),
    CHECK (seg_end >= seg_start)
);
CREATE INDEX IF NOT EXISTS idx_manifests_lookup
    ON manifests (batch_id, side, status, seg_start, seg_end);

CREATE TABLE IF NOT EXISTS coverage (
    batch_id  TEXT NOT NULL REFERENCES batches(batch_id),
    side      TEXT NOT NULL CHECK (side IN ('collector', 'library')),
    seg_start INTEGER NOT NULL,
    seg_end   INTEGER NOT NULL,
    PRIMARY KEY (batch_id, side, seg_start)
);
CREATE INDEX IF NOT EXISTS idx_coverage_lookup
    ON coverage (batch_id, side, seg_start, seg_end);

-- 帧号倒排索引：每张有效清单的号段分解为至多 128 个树节点。
-- 主键顺序让单次查帧能用 (batch_id, node_key) 做点查，两侧一并取回。
CREATE TABLE IF NOT EXISTS frame_index (
    batch_id    TEXT NOT NULL,
    node_key    BLOB NOT NULL,
    side        TEXT NOT NULL CHECK (side IN ('collector', 'library')),
    manifest_no TEXT NOT NULL REFERENCES manifests(manifest_no),
    PRIMARY KEY (batch_id, node_key, side, manifest_no)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_frame_index_manifest
    ON frame_index (manifest_no);
"""


# ---------- 区段树节点键 ----------

def _node_key(level: int, index: int) -> bytes:
    """节点键：1 字节层号 + 8 字节无符号节点序号（第 64 层可达 2^64-1）。

    用 BLOB 而不是 SQLite INTEGER，是因为叶子层序号需要完整的 64 位
    无符号范围，超出 SQLite 有符号 64 位整数。等值点查可直接走主键。
    """
    return level.to_bytes(1, "big") + index.to_bytes(8, "big")


def index_keys_for_segment(start: int, end: int) -> List[bytes]:
    """把闭区间 [start, end] 规范分解为区段树节点键。

    第 0 层覆盖 [INT64_MIN, INT64_MAX]，每层二分；节点完全落在号段内
    即收入结果。任意 int64 号段至多产生 2 * 64 = 128 个节点，与号段
    长度无关，长号段不会被逐帧展开。
    """
    if start > end:
        raise ValueError(f"非法号段: ({start}, {end})")
    if start < INT64_MIN or end > INT64_MAX:
        raise ValueError(f"号段超出 64 位整数范围: ({start}, {end})")

    keys: List[bytes] = []
    stack: List[Tuple[int, int, int, int]] = [(0, 0, INT64_MIN, INT64_MAX)]
    while stack:
        level, index, lo, hi = stack.pop()
        if start <= lo and hi <= end:
            keys.append(_node_key(level, index))
            continue
        mid = (lo + hi) // 2
        # 当前节点必然与 [start, end] 相交，至少一侧存在交集。
        if start <= mid:
            stack.append((level + 1, index * 2, lo, mid))
        if end > mid:
            stack.append((level + 1, index * 2 + 1, mid + 1, hi))
    keys.sort()
    return keys


def index_keys_for_frame(frame_no: int) -> List[bytes]:
    """一帧在 65 层中的节点键：根到叶唯一路径。固定 65 次点查定位。"""
    if not (INT64_MIN <= frame_no <= INT64_MAX):
        raise ValueError(f"帧号超出 64 位整数范围: {frame_no}")
    offset = frame_no - INT64_MIN  # 0 .. 2^64-1
    return [
        _node_key(level, offset >> (DOMAIN_SIZE_BITS - level))
        for level in range(DOMAIN_LEVELS)
    ]


def connect(db_path: Optional[str] = None) -> sqlite3.Connection:
    path = db_path or DEFAULT_DB_PATH
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    # 每个请求通过依赖获取独立连接；FastAPI 线程池可能在不同工作线程上
    # 运行端点与依赖，关闭同线程限制（连接不跨请求共享，无并发共用问题）。
    conn = sqlite3.connect(path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db(db_path: Optional[str] = None) -> None:
    """建表并升级旧库；旧库中的清单会被回填进帧索引，可直接按帧追查。

    旧库可能由多进程同时首次启动，迁移用 ``BEGIN IMMEDIATE`` 串行化，
    拿到写锁后再读版本号，避免重复回填或版本被抢先推进。
    """
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
        with transaction(conn):
            version = conn.execute("PRAGMA user_version").fetchone()[0]
            if version < SCHEMA_VERSION:
                _rebuild_frame_index(conn)
                _rebuild_coverage(conn)
                conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
        conn.commit()
    finally:
        conn.close()


def _rebuild_frame_index(conn: sqlite3.Connection) -> None:
    """从清单表完整重建帧索引（升级旧库或索引损坏时使用）。"""
    conn.execute("DELETE FROM frame_index")
    rows = conn.execute(
        "SELECT manifest_no, batch_id, side, seg_start, seg_end "
        "FROM manifests WHERE status='active'"
    ).fetchall()
    for row in rows:
        _add_frame_index(
            conn, row["manifest_no"], row["batch_id"], row["side"],
            row["seg_start"], row["seg_end"],
        )


def _rebuild_coverage(conn: sqlite3.Connection) -> None:
    """升级旧库时由有效清单重建覆盖段，保证核对结果与清单一致。"""
    conn.execute("DELETE FROM coverage")
    batches = conn.execute("SELECT batch_id FROM batches").fetchall()
    for batch in batches:
        for side in VALID_SIDES:
            rows = conn.execute(
                "SELECT seg_start, seg_end FROM manifests "
                "WHERE batch_id=? AND side=? AND status='active'",
                (batch["batch_id"], side),
            ).fetchall()
            for start, end in intervals.merge(
                (r["seg_start"], r["seg_end"]) for r in rows
            ):
                conn.execute(
                    "INSERT INTO coverage (batch_id, side, seg_start, seg_end) "
                    "VALUES (?, ?, ?, ?)",
                    (batch["batch_id"], side, start, end),
                )


@contextmanager
def transaction(conn: sqlite3.Connection):
    """立即加写锁的事务，异常即回滚，保证原子生效。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
        conn.commit()
    except Exception:
        conn.rollback()
        raise


# ---------- 批次 ----------

def get_batch(conn: sqlite3.Connection, batch_id: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM batches WHERE batch_id=?", (batch_id,)
    ).fetchone()


def create_batch(conn: sqlite3.Connection, batch_id: str, start: int, end: int) -> None:
    conn.execute(
        "INSERT INTO batches (batch_id, frame_start, frame_end) VALUES (?, ?, ?)",
        (batch_id, start, end),
    )


# ---------- 清单 ----------

def get_manifest(conn: sqlite3.Connection, manifest_no: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM manifests WHERE manifest_no=?", (manifest_no,)
    ).fetchone()


def insert_manifest(
    conn: sqlite3.Connection, manifest_no: str, batch_id: str, side: str, start: int, end: int
) -> None:
    conn.execute(
        "INSERT INTO manifests (manifest_no, batch_id, side, seg_start, seg_end) "
        "VALUES (?, ?, ?, ?, ?)",
        (manifest_no, batch_id, side, start, end),
    )


def update_manifest_segment(
    conn: sqlite3.Connection, manifest_no: str, start: int, end: int
) -> None:
    conn.execute(
        "UPDATE manifests SET seg_start=?, seg_end=?, updated_at=datetime('now') "
        "WHERE manifest_no=?",
        (start, end, manifest_no),
    )


def mark_manifest_withdrawn(conn: sqlite3.Connection, manifest_no: str) -> None:
    conn.execute(
        "UPDATE manifests SET status='withdrawn', updated_at=datetime('now') "
        "WHERE manifest_no=?",
        (manifest_no,),
    )


def active_manifests_in(
    conn: sqlite3.Connection, batch_id: str, side: str, start: int, end: int
) -> List[sqlite3.Row]:
    """取与 [start, end] 相交的有效（未撤回）清单，走索引区间扫描。"""
    return conn.execute(
        "SELECT * FROM manifests "
        "WHERE batch_id=? AND side=? AND status='active' "
        "AND seg_start <= ? AND seg_end >= ? "
        "ORDER BY seg_start",
        (batch_id, side, end, start),
    ).fetchall()


# ---------- 帧索引（按帧追查的读路径） ----------

def _add_frame_index(
    conn: sqlite3.Connection,
    manifest_no: str,
    batch_id: str,
    side: str,
    start: int,
    end: int,
) -> None:
    conn.executemany(
        "INSERT OR IGNORE INTO frame_index "
        "(batch_id, node_key, side, manifest_no) VALUES (?, ?, ?, ?)",
        [
            (batch_id, key, side, manifest_no)
            for key in index_keys_for_segment(start, end)
        ],
    )


def _remove_frame_index(conn: sqlite3.Connection, manifest_no: str) -> None:
    conn.execute("DELETE FROM frame_index WHERE manifest_no=?", (manifest_no,))


def index_active_manifest(
    conn: sqlite3.Connection,
    manifest_no: str,
    batch_id: str,
    side: str,
    start: int,
    end: int,
) -> None:
    """清单有效（登记或更正）时维护其索引项，与清单同事务生效。"""
    _remove_frame_index(conn, manifest_no)
    _add_frame_index(conn, manifest_no, batch_id, side, start, end)


def remove_manifest_index(conn: sqlite3.Connection, manifest_no: str) -> None:
    """清单撤回时移除索引项，与状态变更同事务生效。"""
    _remove_frame_index(conn, manifest_no)


# ---------- 覆盖段（核对结果缓存，增量维护） ----------

def coverage_segments(
    conn: sqlite3.Connection, batch_id: str, side: str
) -> List[Tuple[int, int]]:
    rows = conn.execute(
        "SELECT seg_start, seg_end FROM coverage "
        "WHERE batch_id=? AND side=? ORDER BY seg_start",
        (batch_id, side),
    ).fetchall()
    return [(r["seg_start"], r["seg_end"]) for r in rows]


def refresh_region(
    conn: sqlite3.Connection, batch_id: str, side: str, region_start: int, region_end: int
) -> None:
    """仅在受影响区间内重建该侧覆盖段，不重扫整批清单。

    重建范围向两侧各扩 1 帧，使与受影响区间首尾相邻的旧覆盖段一并
    被吸收，重新合并成最大连续段。
    """
    lo, hi = region_start - 1, region_end + 1
    # 号段可在 int64 边界上，外扩时钳制，避免绑定超出 SQLite 整数范围
    lo = max(lo, INT64_MIN)
    hi = min(hi, INT64_MAX)

    old_rows = conn.execute(
        "SELECT seg_start, seg_end FROM coverage "
        "WHERE batch_id=? AND side=? AND seg_start <= ? AND seg_end >= ? "
        "ORDER BY seg_start",
        (batch_id, side, hi, lo),
    ).fetchall()
    if old_rows:
        lo = min(lo, min(r["seg_start"] for r in old_rows))
        hi = max(hi, max(r["seg_end"] for r in old_rows))
        conn.execute(
            "DELETE FROM coverage "
            "WHERE batch_id=? AND side=? AND seg_start <= ? AND seg_end >= ?",
            (batch_id, side, hi, lo),
        )

    rows = active_manifests_in(conn, batch_id, side, lo, hi)
    pieces: List[Tuple[int, int]] = [
        (max(r["seg_start"], lo), min(r["seg_end"], hi)) for r in rows
    ]
    for seg in intervals.merge(pieces):
        conn.execute(
            "INSERT INTO coverage (batch_id, side, seg_start, seg_end) VALUES (?, ?, ?, ?)",
            (batch_id, side, seg[0], seg[1]),
        )


# ---------- 核对 ----------

def reconcile(
    conn: sqlite3.Connection, batch_id: str
) -> dict:
    batch = get_batch(conn, batch_id)
    if batch is None:
        raise LookupError(f"批次不存在: {batch_id}")
    b_start, b_end = batch["frame_start"], batch["frame_end"]
    batch_range: Sequence[Tuple[int, int]] = [(b_start, b_end)]

    collector = intervals.merge(coverage_segments(conn, batch_id, "collector"))
    library = intervals.merge(coverage_segments(conn, batch_id, "library"))

    both = intervals.intersect(collector, library)
    only_collector = intervals.subtract(collector, library)
    only_library = intervals.subtract(library, collector)
    covered = intervals.merge(both + only_collector + only_library)
    neither = intervals.subtract(batch_range, covered)

    def clip(segs):
        return [
            {"frame_start": s, "frame_end": e}
            for s, e in intervals.intersect(segs, batch_range)
        ]

    return {
        "batch_id": batch_id,
        "expected_range": {"frame_start": b_start, "frame_end": b_end},
        "both_present": clip(both),
        "only_collector": clip(only_collector),
        "only_library": clip(only_library),
        "neither_present": clip(neither),
    }


def trace_frame(conn: sqlite3.Connection, batch_id: str, frame_no: int) -> dict:
    """按帧追查两侧覆盖该帧的有效清单。

    读路径只走帧索引：沿树根到叶的 65 个节点键做点查，每个键下的索引项
    都必然覆盖该帧（节点完全落在清单号段内），因此读取的索引项只随命中
    清单数增长，与批次大小、清单总数无关。批次范围与两侧清单由同一条
    SQL 语句取回，状态判定与清单列表来自同一份已提交快照。
    """
    if not (INT64_MIN <= frame_no <= INT64_MAX):
        raise ValueError(f"帧号超出 64 位整数范围: {frame_no}")

    keys = index_keys_for_frame(frame_no)
    placeholders = ",".join("?" for _ in keys)
    # 一条语句同时取回批次范围与两侧命中清单，SQLite 的语句级读快照保证
    # 状态判定与清单列表来自同一份已提交数据，写入不会在两次读取之间混入。
    rows = conn.execute(
        "SELECT b.frame_start AS b_start, b.frame_end AS b_end, "
        "       fi.side AS side, m.manifest_no AS manifest_no, "
        "       m.seg_start AS seg_start, m.seg_end AS seg_end "
        "FROM batches b "
        "LEFT JOIN frame_index fi "
        "  ON fi.batch_id = b.batch_id "
        f" AND fi.node_key IN ({placeholders}) "
        "LEFT JOIN manifests m "
        "  ON m.manifest_no = fi.manifest_no AND m.status = 'active' "
        "WHERE b.batch_id = ? "
        "ORDER BY fi.side, m.seg_start, m.manifest_no",
        (*keys, batch_id),
    ).fetchall()
    if not rows:
        raise LookupError(f"批次不存在: {batch_id}")

    b_start, b_end = rows[0]["b_start"], rows[0]["b_end"]
    if not (b_start <= frame_no <= b_end):
        raise ValueError(
            f"帧号 {frame_no} 越界，批次范围为 [{b_start}, {b_end}]"
        )

    by_side = {"collector": [], "library": []}
    seen = set()
    for r in rows:
        manifest_no = r["manifest_no"]
        # LEFT JOIN 无命中时清单列为 NULL；索引项必然覆盖该帧，无需回表再判
        if manifest_no is None or manifest_no in seen:
            continue
        seen.add(manifest_no)
        by_side[r["side"]].append(
            {
                "manifest_no": manifest_no,
                "seg_start": r["seg_start"],
                "seg_end": r["seg_end"],
            }
        )
    collector_manifests = by_side["collector"]
    library_manifests = by_side["library"]

    in_collector = bool(collector_manifests)
    in_library = bool(library_manifests)
    if in_collector and in_library:
        status = "both_present"
    elif in_collector:
        status = "only_collector"
    elif in_library:
        status = "only_library"
    else:
        status = "neither_present"

    return {
        "batch_id": batch_id,
        "frame_no": frame_no,
        "status": status,
        "collector_manifests": collector_manifests,
        "library_manifests": library_manifests,
    }

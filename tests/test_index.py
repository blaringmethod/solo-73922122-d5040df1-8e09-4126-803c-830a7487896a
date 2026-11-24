"""帧索引的直接测试：分解上限、正确性、旧库升级与查帧一致性。"""

import sqlite3

import pytest

from app import db


def test_segment_decomposition_is_bounded_and_exact():
    segments = [
        (0, 0),
        (-1, 1),
        (1, 100),
        (0, 2**31),
        (5_000_000_000, 9_000_000_000),
        (-(2**63), 2**63 - 1),
        (-(2**63), 0),
        (1, 2**63 - 1),
    ]
    for start, end in segments:
        keys = db.index_keys_for_segment(start, end)
        assert 1 <= len(keys) <= 2 * 64
        assert len(set(keys)) == len(keys)


@pytest.mark.parametrize("start,end", [(-(2**63), -(2**63)), (2**63 - 1, 2**63 - 1)])
def test_extreme_frames_match_only_their_segment(start, end):
    keys = set(db.index_keys_for_segment(start, end))
    assert set(db.index_keys_for_frame(start)) & keys
    other = start + 1 if start < 0 else start - 1
    assert not (set(db.index_keys_for_frame(other)) & keys)


def test_frame_path_is_exactly_65_levels():
    assert len(db.index_keys_for_frame(0)) == 65
    assert len(db.index_keys_for_frame(-(2**63))) == 65
    assert len(db.index_keys_for_frame(2**63 - 1)) == 65


def test_out_of_int64_rejected():
    with pytest.raises(ValueError):
        db.index_keys_for_frame(2**63)
    with pytest.raises(ValueError):
        db.index_keys_for_segment(-(2**63) - 1, 0)


def _legacy_db(path):
    """构造升级前的库：只有三张表，没有帧索引，user_version=0。"""
    conn = db.connect(path)
    conn.executescript(
        """
        CREATE TABLE batches (
            batch_id TEXT PRIMARY KEY,
            frame_start INTEGER NOT NULL,
            frame_end INTEGER NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            CHECK (frame_end >= frame_start)
        );
        CREATE TABLE manifests (
            manifest_no TEXT PRIMARY KEY,
            batch_id TEXT NOT NULL,
            side TEXT NOT NULL CHECK (side IN ('collector','library')),
            status TEXT NOT NULL DEFAULT 'active'
                CHECK (status IN ('active','withdrawn')),
            seg_start INTEGER NOT NULL,
            seg_end INTEGER NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            CHECK (seg_end >= seg_start)
        );
        CREATE TABLE coverage (
            batch_id TEXT NOT NULL,
            side TEXT NOT NULL,
            seg_start INTEGER NOT NULL,
            seg_end INTEGER NOT NULL,
            PRIMARY KEY (batch_id, side, seg_start)
        );
        """
    )
    conn.execute(
        "INSERT INTO batches (batch_id, frame_start, frame_end) VALUES ('OLD', 0, 100)"
    )
    conn.executemany(
        "INSERT INTO manifests (manifest_no, batch_id, side, status, seg_start, seg_end) "
        "VALUES (?, 'OLD', ?, ?, ?, ?)",
        [
            ("OC", "collector", "active", 10, 40),
            ("OL", "library", "active", 20, 100),
            ("OX", "collector", "withdrawn", 1, 100),
        ],
    )
    conn.commit()
    conn.close()


def test_legacy_database_upgrades_and_traces(tmp_path):
    path = str(tmp_path / "legacy.db")
    _legacy_db(path)

    db.init_db(path)
    conn = db.connect(path)
    try:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == db.SCHEMA_VERSION
        # 已撤回的旧清单不应进入索引
        assert conn.execute(
            "SELECT count(*) FROM frame_index WHERE manifest_no='OX'"
        ).fetchone()[0] == 0

        t = db.trace_frame(conn, "OLD", 15)
        assert t["status"] == "only_collector"
        assert [m["manifest_no"] for m in t["collector_manifests"]] == ["OC"]
        assert t["library_manifests"] == []

        t = db.trace_frame(conn, "OLD", 50)
        assert t["status"] == "only_library"
        assert [m["manifest_no"] for m in t["library_manifests"]] == ["OL"]

        # 覆盖段也按清单重建，核对口径一致
        r = db.reconcile(conn, "OLD")
        assert r["both_present"] == [{"frame_start": 20, "frame_end": 40}]
        assert r["only_collector"] == [{"frame_start": 10, "frame_end": 19}]
        assert r["only_library"] == [{"frame_start": 41, "frame_end": 100}]
        assert r["neither_present"] == [{"frame_start": 0, "frame_end": 9}]
    finally:
        conn.close()


def test_init_db_is_idempotent(tmp_path):
    path = str(tmp_path / "t.db")
    db.init_db(path)
    db.init_db(path)
    conn = db.connect(path)
    try:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == db.SCHEMA_VERSION
    finally:
        conn.close()


def test_index_rows_are_removed_on_withdraw(tmp_path):
    path = str(tmp_path / "t.db")
    db.init_db(path)
    conn = db.connect(path)
    try:
        conn.execute("INSERT INTO batches (batch_id, frame_start, frame_end) VALUES ('B', 0, 100)")
        conn.commit()
        with db.transaction(conn):
            db.insert_manifest(conn, "C1", "B", "collector", 10, 30)
            db.index_active_manifest(conn, "C1", "B", "collector", 10, 30)
            db.refresh_region(conn, "B", "collector", 10, 30)
        assert conn.execute(
            "SELECT count(*) FROM frame_index WHERE manifest_no='C1'"
        ).fetchone()[0] == len(db.index_keys_for_segment(10, 30))

        with db.transaction(conn):
            db.mark_manifest_withdrawn(conn, "C1")
            db.remove_manifest_index(conn, "C1")
            db.refresh_region(conn, "B", "collector", 10, 30)
        assert conn.execute("SELECT count(*) FROM frame_index").fetchone()[0] == 0
        assert db.trace_frame(conn, "B", 20)["status"] == "neither_present"
    finally:
        conn.close()


def _vm_steps(conn, fn):
    counter = [0]
    conn.set_progress_handler(lambda: counter.__setitem__(0, counter[0] + 1), 1)
    try:
        fn()
    finally:
        conn.set_progress_handler(None, 0)
    return counter[0]


def _seed(conn, batch_id, entries):
    with db.transaction(conn):
        db.create_batch(conn, batch_id, 0, 10_000_000)
    for no, side, start, end in entries:
        with db.transaction(conn):
            db.insert_manifest(conn, no, batch_id, side, start, end)
            db.index_active_manifest(conn, no, batch_id, side, start, end)
            db.refresh_region(conn, batch_id, side, start, end)


def test_trace_read_work_depends_on_hits_not_unrelated_manifests(tmp_path):
    path = str(tmp_path / "t.db")
    db.init_db(path)
    conn = db.connect(path)
    try:
        _seed(conn, "B", [
            ("HIT-C", "collector", 9_000_000, 9_000_010),
            ("HIT-L", "library", 9_000_005, 9_000_100),
        ])
        baseline = _vm_steps(conn, lambda: db.trace_frame(conn, "B", 9_000_007))

        # 再登记大量与目标帧无关的清单，查帧的指令数不应随其增长
        for i in range(500):
            with db.transaction(conn):
                db.insert_manifest(conn, f"U{i}", "B", "collector", i * 20, i * 20)
                db.index_active_manifest(conn, f"U{i}", "B", "collector", i * 20, i * 20)
                db.refresh_region(conn, "B", "collector", i * 20, i * 20)

        after = _vm_steps(conn, lambda: db.trace_frame(conn, "B", 9_000_007))
        assert after == baseline
        # 无命中帧同样不受无关清单影响
        miss = _vm_steps(conn, lambda: db.trace_frame(conn, "B", 5_000_001))
        assert miss <= baseline
        assert db.trace_frame(conn, "B", 9_000_007)["status"] == "both_present"
    finally:
        conn.close()


def test_write_failure_keeps_manifests_coverage_and_index_unchanged(tmp_path):
    path = str(tmp_path / "t.db")
    db.init_db(path)
    conn = db.connect(path)
    try:
        _seed(conn, "B", [("C1", "collector", 10, 20)])
        before_index = conn.execute(
            "SELECT side, manifest_no, node_key FROM frame_index ORDER BY manifest_no, node_key"
        ).fetchall()
        before_manifest = dict(db.get_manifest(conn, "C1"))
        before_coverage = db.coverage_segments(conn, "B", "collector")

        # 模拟索引写入失败（例如磁盘/约束错误）：整笔事务必须回滚
        def boom(*args, **kwargs):
            raise sqlite3.OperationalError("simulated index failure")

        original = db._add_frame_index
        db._add_frame_index = boom
        try:
            with pytest.raises(sqlite3.OperationalError):
                with db.transaction(conn):
                    db.insert_manifest(conn, "C2", "B", "collector", 30, 40)
                    db.index_active_manifest(conn, "C2", "B", "collector", 30, 40)
                    db.refresh_region(conn, "B", "collector", 30, 40)
        finally:
            db._add_frame_index = original

        assert conn.execute(
            "SELECT side, manifest_no, node_key FROM frame_index ORDER BY manifest_no, node_key"
        ).fetchall() == before_index
        assert db.get_manifest(conn, "C2") is None
        assert dict(db.get_manifest(conn, "C1")) == before_manifest
        assert db.coverage_segments(conn, "B", "collector") == before_coverage
        assert db.trace_frame(conn, "B", 35)["status"] == "neither_present"
    finally:
        conn.close()

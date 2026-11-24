"""服务层比对：随机登记/更正/撤回后，按帧追查结果与旧式区间扫描一致。"""

import random

import pytest

from app import db


def _brute_trace(conn, batch_id, frame_no):
    """不依赖帧索引的参照实现：直接按号段相交语义扫描清单表。"""
    batch = db.get_batch(conn, batch_id)
    if batch is None:
        raise LookupError(batch_id)
    if not (batch["frame_start"] <= frame_no <= batch["frame_end"]):
        raise ValueError(frame_no)
    out = {"collector": [], "library": []}
    for side in ("collector", "library"):
        for row in db.active_manifests_in(conn, batch_id, side, frame_no, frame_no):
            out[side].append(
                {
                    "manifest_no": row["manifest_no"],
                    "seg_start": row["seg_start"],
                    "seg_end": row["seg_end"],
                }
            )
    return out


def test_random_operations_match_bruteforce_and_reconcile(tmp_path):
    random.seed(20261124)
    path = str(tmp_path / "fuzz.db")
    db.init_db(path)
    conn = db.connect(path)
    try:
        with db.transaction(conn):
            db.create_batch(conn, "B", -500, 500)

        active = {}  # manifest_no -> [side, start, end, status]

        def insert(no, side, start, end):
            with db.transaction(conn):
                db.insert_manifest(conn, no, "B", side, start, end)
                db.index_active_manifest(conn, no, "B", side, start, end)
                db.refresh_region(conn, "B", side, start, end)
            active[no] = [side, start, end, "active"]

        def correct(no, start, end):
            side, old_start, old_end, _ = active[no]
            with db.transaction(conn):
                db.update_manifest_segment(conn, no, start, end)
                db.index_active_manifest(conn, no, "B", side, start, end)
                db.refresh_region(conn, "B", side, start, end)
                db.refresh_region(conn, "B", side, old_start, old_end)
            active[no][1], active[no][2] = start, end

        def withdraw(no):
            side, start, end, _ = active[no]
            with db.transaction(conn):
                db.mark_manifest_withdrawn(conn, no)
                db.remove_manifest_index(conn, no)
                db.refresh_region(conn, "B", side, start, end)
            active[no][3] = "withdrawn"

        for i in range(60):
            side = random.choice(["collector", "library"])
            start = random.randint(-500, 500)
            insert(f"M{i}", side, start, random.randint(start, 500))

        for _ in range(120):
            no = random.choice(list(active))
            if active[no][3] != "active":
                continue
            if random.random() < 0.5:
                start = random.randint(-500, 500)
                correct(no, start, random.randint(start, 500))
            else:
                withdraw(no)

        expected_status = {}
        for frame in range(-500, 501):
            expected = _brute_trace(conn, "B", frame)
            got = db.trace_frame(conn, "B", frame)
            assert got["collector_manifests"] == expected["collector"]
            assert got["library_manifests"] == expected["library"]
            expected_status[frame] = got["status"]

        # 核对结果是查帧状态的聚合，逐帧必须一致
        result = db.reconcile(conn, "B")
        classes = {
            key: {
                frame
                for seg in result[key]
                for frame in range(seg["frame_start"], seg["frame_end"] + 1)
            }
            for key in ("both_present", "only_collector", "only_library", "neither_present")
        }
        for frame, status in expected_status.items():
            assert frame in classes[status]
    finally:
        conn.close()


def test_trace_is_repeatable_and_keeps_write_path_intact(tmp_path):
    path = str(tmp_path / "t.db")
    db.init_db(path)
    conn = db.connect(path)
    try:
        with db.transaction(conn):
            db.create_batch(conn, "B", 0, 10)
        with db.transaction(conn):
            db.insert_manifest(conn, "C1", "B", "collector", 2, 8)
            db.index_active_manifest(conn, "C1", "B", "collector", 2, 8)
            db.refresh_region(conn, "B", "collector", 2, 8)

        first = db.trace_frame(conn, "B", 5)
        second = db.trace_frame(conn, "B", 5)
        assert first == second
        # 查帧是只读路径，不应在某些连接上遗留未结束的事务
        with db.transaction(conn):
            db.insert_manifest(conn, "L1", "B", "library", 0, 10)
            db.index_active_manifest(conn, "L1", "B", "library", 0, 10)
            db.refresh_region(conn, "B", "library", 0, 10)
        assert db.trace_frame(conn, "B", 5)["status"] == "both_present"
    finally:
        conn.close()


@pytest.mark.parametrize("frame", [-1, 11])
def test_trace_out_of_batch_raises(tmp_path, frame):
    path = str(tmp_path / "t.db")
    db.init_db(path)
    conn = db.connect(path)
    try:
        with db.transaction(conn):
            db.create_batch(conn, "B", 0, 10)
        with pytest.raises(ValueError):
            db.trace_frame(conn, "B", frame)
        with pytest.raises(LookupError):
            db.trace_frame(conn, "MISSING", 5)
    finally:
        conn.close()

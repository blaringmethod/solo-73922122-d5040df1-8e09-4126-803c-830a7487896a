"""按帧追查读量自测脚本（仅标准库）

用 SQLite 的 progress handler 统计一次 ``trace_frame`` 实际执行的 VDBE
指令数，直观验证：

1. 无关清单大量增加时，查帧读量不变（只随命中清单数增长）；
2. 同帧命中清单增多时，读量近似线性增长；
3. 更正把清单移走、撤回其中一张后，命中与读量同步变化，且其余来源保留。

用法：python scripts/measure_trace.py
"""

import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import db  # noqa: E402

BATCH = "MEASURE"
FRAME = 5_000_000


def steps(conn, fn):
    """执行 fn，返回其消耗的 VDBE 指令数（确定性读量指标）。"""
    counter = [0]
    conn.set_progress_handler(lambda: counter.__setitem__(0, counter[0] + 1), 1)
    try:
        result = fn()
    finally:
        conn.set_progress_handler(None, 0)
    return result, counter[0]


def seed(conn, entries, one_txn=False):
    if one_txn:
        with db.transaction(conn):
            for no, side, start, end in entries:
                db.insert_manifest(conn, no, BATCH, side, start, end)
                db.index_active_manifest(conn, no, BATCH, side, start, end)
                db.refresh_region(conn, BATCH, side, start, end)
        return
    for no, side, start, end in entries:
        with db.transaction(conn):
            db.insert_manifest(conn, no, BATCH, side, start, end)
            db.index_active_manifest(conn, no, BATCH, side, start, end)
            db.refresh_region(conn, BATCH, side, start, end)


def trace(conn):
    return db.trace_frame(conn, BATCH, FRAME)


def main() -> int:
    path = os.path.join(tempfile.mkdtemp(prefix="qc-measure-"), "measure.db")
    db.init_db(path)
    conn = db.connect(path)
    try:
        with db.transaction(conn):
            db.create_batch(conn, BATCH, 0, 10_000_000)

        def measure_trace():
            return trace(conn)

        # 命中帧的两侧来源：采集端一张 + 影像库一张
        seed(conn, [
            ("HIT-C", "collector", FRAME - 10, FRAME + 10),
            ("HIT-L", "library", FRAME - 5, FRAME + 5),
        ])
        result, base = steps(conn, measure_trace)
        assert result["status"] == "both_present"
        print(f"基线：2 条命中清单                 -> {base} 步")

        # 大量与目标帧无关的清单（每条一个独立短号段）
        unrelated = [(f"U{i}", "collector", i * 20, i * 20 + 3) for i in range(5000)]
        seed(conn, unrelated, one_txn=True)
        result, grown = steps(conn, measure_trace)
        assert result["status"] == "both_present"
        print(f"再登记 5000 条无关清单            -> {grown} 步（变化 {grown - base:+d}）")

        # 与目标帧重叠的清单逐级增多
        for n in (10, 100, 1000):
            seed(conn, [
                (f"O{n}_{i}", "collector" if i % 2 else "library",
                 FRAME - 100, FRAME + 100)
                for i in range(n)
            ], one_txn=True)
            result, cost = steps(conn, measure_trace)
            hits = len(result["collector_manifests"]) + len(result["library_manifests"])
            print(f"命中清单累计 {hits:5d} 条            -> {cost} 步")

        # 更正：把 HIT-C 移出目标帧
        with db.transaction(conn):
            db.update_manifest_segment(conn, "HIT-C", FRAME + 1000, FRAME + 1010)
            db.index_active_manifest(conn, "HIT-C", BATCH, "collector", FRAME + 1000, FRAME + 1010)
            db.refresh_region(conn, BATCH, "collector", FRAME - 10, FRAME + 1010)
        result, cost = steps(conn, measure_trace)
        assert "HIT-C" not in [m["manifest_no"] for m in result["collector_manifests"]]
        assert result["status"] == "both_present"
        print(f"更正 HIT-C 移出该帧后              -> {cost} 步（其余来源保留）")

        # 撤回：同侧重叠清单只撤一张，其余仍然命中
        with db.transaction(conn):
            db.mark_manifest_withdrawn(conn, "HIT-L")
            db.remove_manifest_index(conn, "HIT-L")
            db.refresh_region(conn, BATCH, "library", FRAME - 5, FRAME + 5)
        result, cost = steps(conn, measure_trace)
        numbers = [m["manifest_no"] for m in result["library_manifests"]]
        assert "HIT-L" not in numbers and numbers, numbers
        assert result["status"] == "both_present"
        print(f"撤回 HIT-L 后                      -> {cost} 步（{len(numbers)} 条库侧来源仍在）")

        # 无命中帧：同样不受无关清单影响
        empty, cost = steps(conn, lambda: db.trace_frame(conn, BATCH, 9_000_001))
        assert empty["status"] == "neither_present"
        print(f"无命中帧（5000+ 条无关清单）       -> {cost} 步")
    finally:
        conn.close()
    print("== 读量自测完成 ==")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

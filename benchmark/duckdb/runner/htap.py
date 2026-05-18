import threading
import time
from database.database import Cursor
from .tpch import setup_tpch_benchmark, run_tpch_epoch_benchmark
from .ycsb import setup_ycsb_benchmark
from .ycsb_runner import run_ycsb_loop
from .coordinator import WorkerHandle, ActiveClock

HTAP_BENCHMARK_NAME = "htap"
HTAP_NAMESPACE_COUNT = 2  # one TPC-H DB, one YCSB DB


def setup_htap_benchmark(cursors: list[Cursor], input_dir: str,
                         tpch_sf: int, ycsb_sf: int, checkpoint_mode: str = "auto"):
    # cursors[0] -> tpch, cursors[1] -> ycsb (matches derive_db_names order
    # for ['tpch','ycsb']).
    setup_tpch_benchmark([cursors[0]], input_dir, tpch_sf)
    setup_ycsb_benchmark([cursors[1]], input_dir, ycsb_sf, checkpoint_mode)


def run_htap_epoch_benchmark(cursors: list[Cursor], tpch_sf: int, ycsb_sf: int,
                             duration_seconds: int, reps: int,
                             checkpoint_mode: str = "auto",
                             interval_seconds: int = 660,
                             coordinator=None, output_handle=None):
    if duration_seconds <= 0 and reps <= 0:
        raise ValueError("HTAP needs either duration_seconds or reps.")

    tpch_cursor, ycsb_cursor = cursors[0], cursors[1]
    results = {"tpch": [], "ycsb": []}

    tpch_handle = WorkerHandle(name="tpch", clock=ActiveClock())
    ycsb_handle = WorkerHandle(name="ycsb")
    if coordinator is not None:
        coordinator.add_worker(tpch_handle)
        coordinator.add_worker(ycsb_handle)

    tpch_out = output_handle.get("tpch") if output_handle else None
    ycsb_out = output_handle.get("ycsb") if output_handle else None

    stop_flag = threading.Event()

    def tpch_thread():
        try:
            limit_s = duration_seconds if duration_seconds > 0 else float('inf')
            start = time.monotonic()
            while not stop_flag.is_set() and (time.monotonic() - start) < limit_s:
                res = run_tpch_epoch_benchmark(
                    [tpch_cursor], tpch_sf,
                    worker_handle=tpch_handle, output_handle=tpch_out,
                )
                results["tpch"].extend(res.get("tpch", []))
        except Exception as e:
            print(f"HTAP TPC-H thread failed: {e}")

    def ycsb_thread():
        try:
            iterations = 10_000_000_000 if duration_seconds > 0 else reps * 1_000_000
            row_count = ycsb_sf * 100_000
            rows = run_ycsb_loop(
                ycsb_cursor,
                num_fields=10, field_length=2000, batch_size=30,
                row_count=row_count,
                iterations=iterations,
                duration_seconds=duration_seconds,
                interval_seconds=interval_seconds,
                checkpoint_mode=checkpoint_mode,
                worker_handle=ycsb_handle,
                output_handle=ycsb_out,
            )
            results["ycsb"].extend(rows)
        except Exception as e:
            print(f"HTAP YCSB thread failed: {e}")
        finally:
            stop_flag.set()

    t1 = threading.Thread(target=tpch_thread, name="htap-tpch")
    t2 = threading.Thread(target=ycsb_thread, name="htap-ycsb")
    t1.start(); t2.start()
    t2.join()           # YCSB drives duration
    stop_flag.set()
    t1.join()

    if coordinator is not None:
        coordinator.remove_worker(tpch_handle)
        coordinator.remove_worker(ycsb_handle)

    return results
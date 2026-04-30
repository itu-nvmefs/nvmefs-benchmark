import time

from database.database import Database
from concurrent.futures import ThreadPoolExecutor
from .tpch import setup_tpch_benchmark, run_tpch_epoch_benchmark
from .ycsb import setup_ycsb_benchmark, run_ycsb_epoch_benchmark


HTAP_BENCHMARK_NAME = "htap"
HTAP_NAMESPACE_COUNT = 2

def setup_htap_benchmark(dbs: list[Database], input_dir_path: str, tpch_sf: int, ycsb_sf: int, checkpoint_mode: str = "auto"):
    setup_tpch_benchmark([dbs[0]], input_dir_path, tpch_sf)
    setup_ycsb_benchmark([dbs[1]], input_dir_path, ycsb_sf, checkpoint_mode)

def _tpch_worker(dbs, tpch_sf, duration_seconds, reps):
    rows = []
    if duration_seconds > 0:
        start = time.monotonic()
        while (time.monotonic() - start) < duration_seconds:
            rows.extend(run_tpch_epoch_benchmark([dbs[0]], tpch_sf)["tpch"])
    else:
        for _ in range(reps):
            rows.extend(run_tpch_epoch_benchmark([dbs[0]], tpch_sf)["tpch"])
    return rows


def _ycsb_worker(dbs, ycsb_sf, duration_seconds, reps,
                 checkpoint_mode, interval_seconds):
    return run_ycsb_epoch_benchmark(
        [dbs[1]], ycsb_sf, duration_seconds, reps,
        checkpoint_mode, interval_seconds=interval_seconds,
    )["ycsb"]

def run_htap_epoch_benchmark(dbs: list[Database], tpch_sf: int, ycsb_sf: int, duration_seconds: int, reps: int, checkpoint_mode: str = "auto", interval_seconds=660):
    with ThreadPoolExecutor(max_workers=2) as pool:
        fut_tpch = pool.submit(_tpch_worker, dbs, tpch_sf, duration_seconds, reps)
        fut_ycsb = pool.submit(_ycsb_worker, dbs, ycsb_sf, duration_seconds, reps, checkpoint_mode, interval_seconds)

    return {
            "tpch": fut_tpch.result(),
            "ycsb": fut_ycsb.result(),
        }


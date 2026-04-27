import time

from database.database import Database
from concurrent.futures import ThreadPoolExecutor
from .tpch import setup_tpch_benchmark, run_tpch_epoch_benchmark
from .ycsb import setup_ycsb_benchmark, run_ycsb_epoch_benchmark


HTAP_BENCHMARK_NAME = "htap"
HTAP_NAMESPACE_COUNT = 2
HTAP_YCSB_SF = 100

def setup_htap_benchmark(dbs: list[Database], input_dir_path: str, scale_factor: int, checkpoint_mode: str = "auto"):
    setup_tpch_benchmark([dbs[0]], input_dir_path, scale_factor)
    setup_ycsb_benchmark([dbs[1]], input_dir_path, scale_factor, checkpoint_mode)

def _tpch_worker(dbs, scale_factor, duration_seconds, reps):
    tpch_results = []
    if duration_seconds > 0:
        start_time = time.time()
        while (time.time() - start_time) < duration_seconds:
            tpch_results.extend(run_tpch_epoch_benchmark([dbs[0]], scale_factor))
    else:
        for _ in range(reps):
            tpch_results.extend(run_tpch_epoch_benchmark([dbs[0]], scale_factor))
    return tpch_results

def _ycsb_worker(dbs, duration_seconds, reps, checkpoint_mode):
    return run_ycsb_epoch_benchmark([dbs[1]], HTAP_YCSB_SF, duration_seconds, reps, checkpoint_mode)

def run_htap_epoch_benchmark(dbs: list[Database], scale_factor: int, duration_seconds: int, reps: int, checkpoint_mode: str = "auto"):
    results = []
    # Ensure tpch continues running if ycsb has not finished
    
    with ThreadPoolExecutor(max_workers=2) as pool:
        fut_tpch = pool.submit(_tpch_worker, dbs, scale_factor, duration_seconds, reps)
        fut_ycsb = pool.submit(_ycsb_worker, dbs, duration_seconds, reps, checkpoint_mode)

        for line in fut_tpch.result():
            parts = line.strip().split(';', 2)
            if len(parts) == 3:
                results.append(f"tpch;tpch_q{parts[0]};{parts[1]};{parts[2]}\n")

        for line in fut_ycsb.result():
            results.append(f"ycsb;{line}")

    return results


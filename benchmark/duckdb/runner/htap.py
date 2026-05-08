import time

from database.database import Database
from concurrent.futures import ThreadPoolExecutor
from .tpch import setup_tpch_benchmark, run_tpch_epoch_benchmark
from .ycsb import setup_ycsb_benchmark, run_ycsb_epoch_benchmark
from .coordinator import WorkerHandle, ActiveClock


HTAP_BENCHMARK_NAME = "htap"
HTAP_NAMESPACE_COUNT = 2

def setup_htap_benchmark(dbs: list[Database], input_dir_path: str, tpch_sf: int, ycsb_sf: int, checkpoint_mode: str = "auto"):
    setup_tpch_benchmark([dbs[0]], input_dir_path, tpch_sf)
    setup_ycsb_benchmark([dbs[1]], input_dir_path, ycsb_sf, checkpoint_mode)

def _tpch_worker(dbs, tpch_sf, duration_seconds, reps, coordinator,
                 output_handle=None):
    handle = None
    clock = None
    if coordinator is not None:
        clock = ActiveClock()
        handle = WorkerHandle(name="tpch", clock=clock)
        coordinator.add_worker(handle)

    rows = []
    try:
        if duration_seconds > 0:
            if clock is not None:
                while clock.elapsed() < duration_seconds:
                    rows.extend(run_tpch_epoch_benchmark(
                        [dbs[0]], tpch_sf, worker_handle=handle,
                        output_handle=output_handle)["tpch"])
            else:
                start = time.monotonic()
                while (time.monotonic() - start) < duration_seconds:
                    rows.extend(run_tpch_epoch_benchmark([dbs[0]], tpch_sf)["tpch"])
        else:
            for _ in range(reps):
                rows.extend(run_tpch_epoch_benchmark(
                    [dbs[0]], tpch_sf, worker_handle=handle,
                    output_handle=output_handle)["tpch"])
    finally:
        if handle is not None and coordinator is not None:
            coordinator.remove_worker(handle)
    return rows


def _ycsb_worker(dbs, ycsb_sf, duration_seconds, reps, checkpoint_mode,
                 interval_seconds, coordinator, output_handle=None):
    return run_ycsb_epoch_benchmark(
        [dbs[1]], ycsb_sf, duration_seconds, reps,
        checkpoint_mode, interval_seconds=interval_seconds,
        coordinator=coordinator, output_handle=output_handle,
    )["ycsb"]


def run_htap_epoch_benchmark(dbs: list[Database], tpch_sf: int, ycsb_sf: int,
                             duration_seconds: int, reps: int,
                             checkpoint_mode: str = "auto",
                             interval_seconds=660, coordinator=None,
                             output_handle=None):
    tpch_out = output_handle.get("tpch") if output_handle else None
    ycsb_out = output_handle.get("ycsb") if output_handle else None

    with ThreadPoolExecutor(max_workers=2) as pool:
        fut_tpch = pool.submit(_tpch_worker, dbs, tpch_sf, duration_seconds,
                               reps, coordinator, tpch_out)
        fut_ycsb = pool.submit(_ycsb_worker, dbs, ycsb_sf, duration_seconds,
                               reps, checkpoint_mode, interval_seconds,
                               coordinator, ycsb_out)

    return {"tpch": fut_tpch.result(), "ycsb": fut_ycsb.result()}
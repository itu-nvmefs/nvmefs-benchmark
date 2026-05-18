import json
import os
import time
from database.database import Cursor
from profiler import QueryProfiler

TPCH_BENCHMARK_NAME = "tpch"

# SPILL_QUERIES = [18, 10, 13]
QUERIES = list(range(1, 23))


def setup_tpch_benchmark(cursors: list[Cursor], input_dir_path: str, scale_factor: int):
    c = cursors[0]
    input_file_path = os.path.join(
        input_dir_path, TPCH_BENCHMARK_NAME, f"tpch-sf{scale_factor}.db"
    )
    if not os.path.exists(input_file_path):
        raise FileNotFoundError(f"TPCH seed DB not found: {input_file_path}")

    c.add_extension("tpch")
    c.execute(f"ATTACH DATABASE '{input_file_path}' AS tpch_src (READ_ONLY);")
    c.execute(f"COPY FROM DATABASE tpch_src TO {c.db_name};")
    c.execute("DETACH DATABASE tpch_src;")
    c.execute("PRAGMA disable_object_cache;")


def run_tpch_epoch_benchmark(cursors: list[Cursor], scale_factor: int,
                             duration_seconds: int = 0, reps: int = 0,
                             worker_handle=None, output_handle=None):
    """
    TPC-H runner. Mirrors the YCSB pattern: duration_seconds > 0 runs until
    active-time limit reached; reps > 0 runs a fixed number of epochs.
    The duration check happens between queries, not just between epochs, so
    a run terminates promptly when the limit fires.
    """
    if duration_seconds <= 0 and reps <= 0:
        raise ValueError("TPC-H needs either duration_seconds or reps.")

    results = []
    c = cursors[0]
    use_nvmefs = c.db_path.startswith("nvmefs://")

    run_start = time.monotonic()

    def _elapsed_active() -> float:
        if worker_handle is not None and getattr(worker_handle, "clock", None) is not None:
            return worker_handle.clock.elapsed()
        return time.monotonic() - run_start

    def _should_stop(epoch_idx: int) -> bool:
        if duration_seconds > 0:
            return _elapsed_active() >= duration_seconds
        return epoch_idx >= reps

    epoch = 0
    while not _should_stop(epoch):
        for query_nr in QUERIES:
            if duration_seconds > 0 and _elapsed_active() >= duration_seconds:
                break

            try:
                with QueryProfiler(c, f"tpch-{query_nr}", use_nvmefs) as profiler:
                    c.execute(f"PRAGMA tpch({query_nr});").fetchall()
                metrics_json = json.dumps(profiler.nvmefs_metrics)
                row = f"{query_nr};{profiler.latency_ms:.2f};{metrics_json}\n"
            except Exception as e:
                print(f"{query_nr} failed due to {e}")
                row = f"{query_nr};FAIL;{{}}\n"

            results.append(row)
            if output_handle is not None:
                output_handle.write(row)
                output_handle.flush()
            if worker_handle is not None:
                worker_handle.checkpoint_if_requested(f"tpch-q{query_nr}")

        epoch += 1
    if worker_handle is not None:
        worker_handle.checkpoint_if_requested("tpch-final")

    c.execute("PRAGMA disable_profiling;")
    return {"tpch": results}
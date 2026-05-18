import json
import os
from database.database import Cursor
from profiler import QueryProfiler

TPCH_BENCHMARK_NAME = "tpch"

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
                             worker_handle=None, output_handle=None):
    results = []
    c = cursors[0]
    use_nvmefs = c.db_path.startswith("nvmefs://")

    SPILL_QUERIES = [18, 10, 13]
    for query_nr in SPILL_QUERIES:  # range(1, 23):
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

    c.execute("PRAGMA disable_profiling;")
    return {"tpch": results}
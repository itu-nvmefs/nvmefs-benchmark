import json
import os
from database.database import Database
from benchmark.duckdb.profiler import QueryProfiler

TPCH_BENCHMARK_NAME = "tpch"

def setup_tpch_benchmark(db: Database, input_dir_path: str, scale_factor: int):
    input_file_path = os.path.join(input_dir_path, TPCH_BENCHMARK_NAME, f"tpch-sf{scale_factor}.db")

    if not os.path.exists(input_file_path):
        print(f"ERROR: TPCH benchmark {input_file_path} does not exist")

    db.add_extension("tpch")
    db.execute(f"ATTACH DATABASE '{input_file_path}' AS tpch (READ_WRITE);")
    db.execute("COPY FROM DATABASE tpch TO bench;")
    db.execute("DETACH DATABASE tpch;")
    db.execute("PRAGMA disable_object_cache;")

def run_tpch_epoch_benchmark(db: Database, scale_factor: int):
    results: list[str] = []

    for query_nr in range(1, 22):
        with QueryProfiler(db, f"tpch-{query_nr}") as profiler:
            db.query(f"PRAGMA tpch({query_nr});")

        metrics_json = json.dumps(profiler.nvmefs_metrics)
        results.append(f"{query_nr};{profiler.latency_ms:.2f};{metrics_json}\n")

    return results
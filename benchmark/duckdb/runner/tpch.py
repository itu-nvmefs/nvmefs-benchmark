import json
import os
from database.database import Database
from profiler import QueryProfiler

TPCH_BENCHMARK_NAME = "tpch"

def setup_tpch_benchmark(dbs: list[Database], input_dir_path: str, scale_factor: int):
    input_file_path = os.path.join(input_dir_path, TPCH_BENCHMARK_NAME, f"tpch-sf{scale_factor}.db")

    if not os.path.exists(input_file_path):
        print(f"ERROR: TPCH benchmark {input_file_path} does not exist")

    db = dbs[0]
    db.add_extension("tpch")
    db.execute(f"ATTACH DATABASE '{input_file_path}' AS tpch (READ_WRITE);")
    db.execute("COPY FROM DATABASE tpch TO bench;")
    db.execute("DETACH DATABASE tpch;")
    db.execute("PRAGMA disable_object_cache;")

def run_tpch_epoch_benchmark(dbs: list[Database], scale_factor: int):
    results: list[str] = []
    db = dbs[0]
    use_nvmefs = db.db_path.startswith("nvmefs://")
    
    for query_nr in range(1, 22):
        try: 
            with QueryProfiler(db, f"tpch-{query_nr}", use_nvmefs) as profiler:
                db.execute(f"PRAGMA tpch({query_nr});").fetchall()

            metrics_json = json.dumps(profiler.nvmefs_metrics)
            results.append(f"{query_nr};{profiler.latency_ms:.2f};{metrics_json}\n")
        except Exception as e:
            print(f"{query_nr} failed due to {e}")  
            results.append(f"{query_nr};FAIL;{{}}\n")

    return {"tpch": results}
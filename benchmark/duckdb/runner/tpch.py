import os
import time
from database.database import Database

TPCH_BENCHMARK_NAME = "tpch"

def setup_tpch_benchmark(db: Database, input_dir_path: str, scale_factor: int):
    input_file_path = os.path.join(input_dir_path, f"tpch-sf{scale_factor}.db")

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
        start = time.perf_counter()
        db.query(f"PRAGMA tpch({query_nr});")
        end = time.perf_counter()

        # Get query elapsed time in milliseconds
        query_elapsed = (end - start) * 1000
        results.append(f"{query_nr};{query_elapsed}\n")

    return results
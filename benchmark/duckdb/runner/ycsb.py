import os
from .ycsb_lib import ycsb_engine
from database.database import Database

YCSB_BENCHMARK_NAME = "ycsb"
runner = None # YCSB Engine

def setup_ycsb_benchmark(db: Database, input_dir_path: str, scale_factor: int):
    row_count = scale_factor * 100000
    db.execute("CREATE TABLE IF NOT EXISTS usertable (YCSB_KEY VARCHAR PRIMARY KEY, FIELD0 VARCHAR);")
    db.execute(f"INSERT INTO usertable SELECT 'user' || i, 'val' FROM range({row_count}) t(i);")
    db.execute("CHECKPOINT;")

def run_ycsb_epoch_benchmark(db, scale_factor: int):
    global runner

    iterations = 50000
    row_count = scale_factor * 100000

    if runner is None:
        runner = ycsb_engine.YCSBRunner(
            db.db_path,
            db.device_path,
            db.backend,
            db.config.get_fdp_mapping() if db.use_fdp else ""
        )

    total_time_ms = runner.run(iterations, row_count)
    throughput = (iterations / total_time_ms) * 1000

    return [f"ycsb_workload_a;{total_time_ms};{throughput}\n"]

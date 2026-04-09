import os
from .ycsb_lib import ycsb_engine
from database.database import Database

YCSB_BENCHMARK_NAME = "ycsb"

def setup_ycsb_benchmark(db: Database, input_dir_path: str, scale_factor: int):
    row_count = scale_factor * 100000
    db.execute("CREATE TABLE IF NOT EXISTS usertable (YCSB_KEY VARCHAR PRIMARY KEY, FIELD0 VARCHAR);")
    db.execute(f"INSERT INTO usertable SELECT 'user' || i, 'val' FROM range({row_count}) t(i);")
    db.execute("CHECKPOINT;")

def run_ycsb_epoch_benchmark(db, scale_factor: int):
    iterations = 50000
    row_count = scale_factor * 100000
    
    # Passing control to the C++ YCSB loop via string parameters
    total_time_ms = ycsb_engine.run_native_ycsb(
        db.db_path, 
        db.device_path, 
        db.backend, 
        db.config.get_fdp_mapping() if db.use_fdp else "", 
        iterations, 
        row_count
    )

    throughput = (iterations / total_time_ms) * 1000
    return [f"ycsb_workload_a;{total_time_ms};{throughput}\n"]

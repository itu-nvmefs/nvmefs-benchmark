import os
import json
from .ycsb_lib import ycsb_engine
from database.database import Database
from profiler import QueryProfiler

YCSB_BENCHMARK_NAME = "ycsb"
runner = None # YCSB Engine

def setup_ycsb_benchmark(db: Database, input_dir_path: str, scale_factor: int, checkpoint_mode: str = "auto"):
    row_count = scale_factor * 100000

    if checkpoint_mode == "manual":
        # Disable auto-checkpointing by setting it incredibly high
        db.execute("PRAGMA wal_autocheckpoint='1TB';")
        print("Manual checkpoint")
    else:
        # DuckDB default for auto-checkpointing
        db.execute("PRAGMA wal_autocheckpoint='16MB';")
        print("Auto checkpoint")

    db.execute("CREATE TABLE IF NOT EXISTS usertable (YCSB_KEY VARCHAR PRIMARY KEY, FIELD0 VARCHAR);")
    db.execute(f"INSERT INTO usertable SELECT 'user' || i, 'val' FROM range({row_count}) t(i);")
    db.execute("CHECKPOINT;")

def run_ycsb_epoch_benchmark(db, scale_factor: int, checkpoint_mode: str = "auto"):
    global runner
    iterations = 1000000
    row_count = scale_factor * 100000
    use_nvmefs = db.db_path.startswith("nvmefs://")

    if runner is None:
        use_nvmefs = db.db_path.startswith("nvmefs://") # Determine whether we are using nvmefs extension
        dev_path = getattr(db, "device_path", "")
        backend = getattr(db, "backend", "")
        use_fdp = getattr(db, "use_fdp", False)
        fdp_map = db.config.get_fdp_mapping() if use_fdp else ""
        memory_limit = getattr(db, "memory", 1000)

        runner = ycsb_engine.YCSBRunner(
            db.db_path,
            dev_path,
            backend,
            fdp_map,
            use_nvmefs,
            memory_limit,
            checkpoint_mode
        )

    try:
        total_time_ms = runner.run(iterations, row_count)
        nvmefs_metrics = runner.get_metrics() if use_nvmefs else {}
        throughput = (iterations / total_time_ms) * 1000
        metrics_json = json.dumps(nvmefs_metrics)
        result_row = f"ycsb_workload_a;{total_time_ms:.2f};{throughput:.2f};{metrics_json}\n"
    except Exception as e:
        print(f"YCSB failed due to {e}")
        result_row = f"ycsb_workload_a;FAIL;FAIL;{{}}\n"
    
    if checkpoint_mode == "manual":
        db.execute("CHECKPOINT;")

    return [result_row]

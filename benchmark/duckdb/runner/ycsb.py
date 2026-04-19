import os
import json
from .ycsb_lib import ycsb_engine
from database.database import Database

from ..profiler import QueryProfiler

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
    use_nvmefs = db.db_path.startswith("nvmefs://")

    if runner is None:
        use_nvmefs = db.db_path.startswith("nvmefs://") # Determine whether we are using nvmefs extension
        dev_path = getattr(db, "device_path", "")
        backend = getattr(db, "backend", "")

        use_fdp = getattr(db, "use_fdp", False)
        fdp_map = db.config.get_fdp_mapping() if use_fdp else ""

        runner = ycsb_engine.YCSBRunner(
            db.db_path,
            dev_path,
            backend,
            fdp_map,
            use_nvmefs
        )

    with QueryProfiler(db, "ycsb", use_nvmefs) as profiler:
        total_time_ms = runner.run(iterations, row_count)

    throughput = (iterations / total_time_ms) * 1000
    metrics_json = json.dumps(profiler.nvmefs_metrics)

    return [f"ycsb_workload_a;{total_time_ms:.2f};{throughput:.2f};{metrics_json}\n"]

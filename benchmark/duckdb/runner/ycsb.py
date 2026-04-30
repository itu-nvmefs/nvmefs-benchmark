import os
import json
from .ycsb_lib import ycsb_engine
from database.database import Database

YCSB_BENCHMARK_NAME = "ycsb"
runner = None # YCSB Engine
num_fields = 10
field_length = 100

def setup_ycsb_benchmark(dbs: list[Database], input_dir_path: str, scale_factor: int, checkpoint_mode: str = "auto"):
    db = dbs[0]
    row_count = scale_factor * 100000

    if checkpoint_mode == "manual":
        # Disable auto-checkpointing by setting it incredibly high
        db.execute("PRAGMA wal_autocheckpoint='1TB';")
        print("Manual checkpoint")
    else:
        # DuckDB default for auto-checkpointing
        db.execute("PRAGMA wal_autocheckpoint='16MB';")
        print("Auto checkpoint")

    field_cols = ", ".join(f"FIELD{i} VARCHAR" for i in range(num_fields))
    db.execute(f"CREATE TABLE IF NOT EXISTS usertable (YCSB_KEY VARCHAR PRIMARY KEY, {field_cols});")

    field_exprs = ", ".join(
        f"substr(md5(random()::VARCHAR || {i} || i::VARCHAR), 1, {field_length}) AS FIELD{i}"
        for i in range(num_fields)
    )
    db.execute(
        f"INSERT INTO usertable "
        f"SELECT 'user' || i AS YCSB_KEY, {field_exprs} "
        f"FROM range({row_count}) t(i);"
    )
    db.execute("CHECKPOINT;")

def run_ycsb_epoch_benchmark(dbs: list[Database], scale_factor: int, duration_seconds: int = 0, reps: int = 0, checkpoint_mode: str = "auto", interval_seconds: int = 660):
    global runner
    db = dbs[0]

    if duration_seconds <= 0 and reps <= 0:
        raise ValueError("Error: YCSB received duration=0 and reps=0. Aborting to prevent issues.")

    iterations = 100000000 if duration_seconds > 0 else (reps * 1000000)
    row_count = scale_factor * 100000


    use_nvmefs = db.db_path.startswith("nvmefs://")
    if runner is None:
        dev_path = getattr(db, "device_path", "")
        backend = getattr(db, "backend", "")
        use_fdp = getattr(db, "use_fdp", False)
        fdp_map = db.config.get_fdp_mapping() if use_fdp else ""
        memory_limit = getattr(db, "memory", 1000)

        # Handing off the DuckDB connection to the runner
        try:
            db.close()
        except Exception:
            pass

        runner = ycsb_engine.YCSBRunner(
            db.db_path,
            dev_path,
            backend,
            fdp_map,
            use_nvmefs,
            memory_limit,
            checkpoint_mode,
            num_fields,
            field_length

        )

    result_rows = []
    try:
        intervals = runner.run(iterations, row_count, duration_seconds, interval_seconds)
        for offset_s, interval_ms, iters, metrics in intervals:
            throughput = (iters / interval_ms) * 1000 if interval_ms > 0 else 0
            metrics_json = json.dumps(dict(metrics))
            result_rows.append(
                f"ycsb_workload_a;{offset_s:.2f};{interval_ms:.2f};{iters};{throughput:.2f};{metrics_json}\n"
            )
    except Exception as e:
        print(f"YCSB failed due to {e}")
        result_rows.append(f"ycsb_workload_a;FAIL;FAIL;FAIL;FAIL;{{}}\n")


    return {"ycsb": result_rows}

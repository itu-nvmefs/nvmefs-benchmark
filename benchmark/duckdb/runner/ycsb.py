import os
import json
from .ycsb_lib import ycsb_engine
from database.database import Database
from .coordinator import CppWorkerAdapter

YCSB_BENCHMARK_NAME = "ycsb"
runner = None # YCSB Engine
num_fields = 10
field_length = 2000

def nvmefs_db_size(db, label=""):
    metrics = {}
    if db.db_path.startswith("nvmefs://"):
        result = db.execute("SELECT * FROM print_nvmefs_metrics();").fetchall()
        for row in result:
            key, val = row[0], row[1]
            if val is None:
                metrics[key] = 0
            else:
                try:
                    metrics[key] = int(val)
                except (ValueError, TypeError):
                    metrics[key] = val  # leave non-numeric strings alone
        db_gb = metrics.get("current_db_bytes", 0) / (1024 ** 3)
        temp_gb = metrics.get("current_temp_bytes", 0) / (1024 ** 3)
        print(f"[{label}] db={db_gb:.2f} GB  temp={temp_gb:.2f} GB")
    return metrics

def setup_ycsb_benchmark(dbs: list[Database], input_dir_path: str, scale_factor: int, checkpoint_mode: str = "auto"):
    db = dbs[0]
    input_file_path = os.path.join(input_dir_path, YCSB_BENCHMARK_NAME, f"ycsb-sf{scale_factor}.db")
    
    if not os.path.exists(input_file_path):
        print(f"ERROR: YCSB benchmark {input_file_path} does not exist")
    
    db.execute(f"ATTACH DATABASE '{input_file_path}' AS ycsb (READ_ONLY);")

    # Create the destination table in the bench DB, schema-only
    db.execute("CREATE TABLE bench.usertable AS SELECT * FROM ycsb.usertable LIMIT 0;")

    total_rows = scale_factor * 100000
    chunk_size = 1_000_000   # see note below

    for i, offset in enumerate(range(0, total_rows, chunk_size), start=1):
        db.execute(f"""
            INSERT INTO bench.usertable
            SELECT * FROM ycsb.usertable
            LIMIT {chunk_size} OFFSET {offset};
        """)
        nvmefs_db_size(db, f"after chunk {i} (pre-checkpoint)")
        db.execute("CHECKPOINT bench;")
        nvmefs_db_size(db, f"after chunk {i} (post-checkpoint)")

    db.execute("DETACH DATABASE ycsb;")
    db.execute("PRAGMA disable_object_cache;")

    if checkpoint_mode == "manual":
        # Disable auto-checkpointing by setting it incredibly high
        db.execute("PRAGMA wal_autocheckpoint='1GB';")
        print("Manual checkpoint")
    else: 
        # DuckDB default for auto-checkpointing
        db.execute("PRAGMA wal_autocheckpoint='16MB';")
        print("Auto checkpoint")

def run_ycsb_epoch_benchmark(dbs: list[Database], scale_factor: int,
                             duration_seconds: int = 0, reps: int = 0,
                             checkpoint_mode: str = "auto",
                             interval_seconds: int = 660,
                             coordinator=None, output_handle=None):
    global runner
    db = dbs[0]

    if duration_seconds <= 0 and reps <= 0:
        raise ValueError("Error: YCSB received duration=0 and reps=0.")

    iterations = 10_000_000_000 if duration_seconds > 0 else (reps * 1_000_000)
    row_count = scale_factor * 100000

    use_nvmefs = db.db_path.startswith("nvmefs://")
    print(f"DEBUG: db.db_path = {repr(db.db_path)}")
    print(f"DEBUG: use_nvmefs computed = {db.db_path.startswith('nvmefs://')}")
    print(f"DEBUG: use_nvmefs alt check = {db.db_path.startswith('nvmefs:')}")
    if runner is None:
        dev_path = getattr(db, "device_path", "")
        backend = getattr(db, "backend", "")
        use_fdp = getattr(db, "use_fdp", False)
        fdp_map = db.config.get_fdp_mapping() if use_fdp else ""
        memory_limit = getattr(db, "memory", 1000)

        try:
            db.close()
        except Exception:
            pass

        runner = ycsb_engine.YCSBRunner(
            db.db_path, dev_path, backend, fdp_map, use_nvmefs,
            memory_limit, checkpoint_mode, num_fields, field_length,
        )

    adapter = None
    if coordinator is not None:
        adapter = CppWorkerAdapter(runner, name="ycsb")
        coordinator.add_worker(adapter)

    result_rows = []

    def results_callback(offset_s, interval_ms, iters, metrics):
        throughput = (iters / interval_ms) * 1000 if interval_ms > 0 else 0
        metrics_json = json.dumps(dict(metrics))
        row = f"ycsb_workload_a;{offset_s:.2f};{interval_ms:.2f};{iters};{throughput:.2f};{metrics_json}\n"
        
        result_rows.append(row)
        if output_handle is not None:
            output_handle.write(row)
            output_handle.flush()

    try:
        runner.run(iterations, row_count, duration_seconds, interval_seconds, results_callback)
    except Exception as e:
        print(f"YCSB failed due to {e}")
        result_rows.append(f"ycsb_workload_a;FAIL;FAIL;FAIL;FAIL;{{}}\n")
    finally:
        if adapter is not None and coordinator is not None:
            coordinator.remove_worker(adapter)

    return {"ycsb": result_rows}
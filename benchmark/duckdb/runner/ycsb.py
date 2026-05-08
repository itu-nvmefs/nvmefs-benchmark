import os
import json
from .ycsb_lib import ycsb_engine
from database.database import Database
from .coordinator import CppWorkerAdapter

YCSB_BENCHMARK_NAME = "ycsb"
runner = None # YCSB Engine
num_fields = 10
field_length = 2000

def setup_ycsb_benchmark(dbs: list[Database], input_dir_path: str, scale_factor: int, checkpoint_mode: str = "auto"):
    db = dbs[0]
    input_file_path = os.path.join(input_dir_path, YCSB_BENCHMARK_NAME, f"ycsb-sf{scale_factor}.db")
    
    if not os.path.exists(input_file_path):
        print(f"ERROR: YCSB benchmark {input_file_path} does not exist")
    
    db.execute(f"ATTACH DATABASE '{input_file_path}' AS ycsb (READ_ONLY);")
    db.execute("COPY FROM DATABASE ycsb TO bench;")
    db.execute("DETACH DATABASE ycsb;")
    db.execute("CREATE UNIQUE INDEX ycsb_key_idx ON usertable(YCSB_KEY);")
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

    iterations = 100000000 if duration_seconds > 0 else (reps * 1000000)
    row_count = scale_factor * 100000

    use_nvmefs = db.db_path.startswith("nvmefs://")
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
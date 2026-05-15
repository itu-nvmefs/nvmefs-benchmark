import os
from database.database import Cursor
from .ycsb_runner import run_ycsb_loop

YCSB_BENCHMARK_NAME = "ycsb"
NUM_FIELDS = 10
FIELD_LENGTH = 2000
BATCH_SIZE = 30

def setup_ycsb_benchmark(cursors: list[Cursor], input_dir_path: str,
                         scale_factor: int, checkpoint_mode: str = "auto"):
    c = cursors[0]
    input_file_path = os.path.join(
        input_dir_path, YCSB_BENCHMARK_NAME, f"ycsb-sf{scale_factor}.db"
    )
    if not os.path.exists(input_file_path):
        raise FileNotFoundError(f"YCSB seed DB not found: {input_file_path}")

    c.execute(f"ATTACH DATABASE '{input_file_path}' AS ycsb_src (READ_ONLY);")
    c.execute(
        f"CREATE TABLE {c.db_name}.usertable AS "
        f"SELECT * FROM ycsb_src.usertable LIMIT 0;"
    )

    total_rows = scale_factor * 100_000
    chunk_size = 1_000_000

    for offset in range(0, total_rows, chunk_size):
        c.execute(f"""
            INSERT INTO {c.db_name}.usertable
            SELECT * FROM ycsb_src.usertable
            LIMIT {chunk_size} OFFSET {offset};
        """)
        c.execute(f"CHECKPOINT {c.db_name};")

    c.execute("DETACH DATABASE ycsb_src;")
    c.execute("PRAGMA disable_object_cache;")

    if checkpoint_mode == "manual":
        c.execute("PRAGMA wal_autocheckpoint='4GB';")
        print("YCSB: manual checkpoint (4GB)")
    else:
        c.execute("PRAGMA wal_autocheckpoint='16MB';")
        print("YCSB: auto checkpoint (16MB)")


def run_ycsb_epoch_benchmark(cursors: list[Cursor], scale_factor: int,
                             duration_seconds: int = 0, reps: int = 0,
                             checkpoint_mode: str = "auto",
                             interval_seconds: int = 660,
                             coordinator=None, output_handle=None,
                             worker_handle=None):
    c = cursors[0]

    if duration_seconds <= 0 and reps <= 0:
        raise ValueError("YCSB needs either duration_seconds or reps.")

    iterations = 10_000_000_000 if duration_seconds > 0 else reps * 1_000_000
    row_count = scale_factor * 100_000

    try:
        rows = run_ycsb_loop(
            c,
            num_fields=NUM_FIELDS,
            field_length=FIELD_LENGTH,
            batch_size=BATCH_SIZE,
            row_count=row_count,
            iterations=iterations,
            duration_seconds=duration_seconds,
            interval_seconds=interval_seconds,
            checkpoint_mode=checkpoint_mode,
            worker_handle=worker_handle,
            output_handle=output_handle,
        )
    except Exception as e:
        print(f"YCSB failed: {e}")
        rows = [f"ycsb_workload_a;FAIL;FAIL;FAIL;FAIL;{{}}\n"]
        if output_handle is not None:
            output_handle.write(rows[0])
            output_handle.flush()

    return {"ycsb": rows}
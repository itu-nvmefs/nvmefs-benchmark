import base64
import json
import os
import random
import time
from database.database import Cursor


# 16 MB pre-generated random pool. Sample offsets at runtime instead of
# generating new random bytes per call.
_POOL_SIZE = 16 * 1024 * 1024
_random_pool = base64.b64encode(os.urandom(_POOL_SIZE))[:_POOL_SIZE].decode('ascii')


def _value_from_pool(rng: random.Random, length: int) -> str:
    offset = rng.randint(0, _POOL_SIZE - length)
    return _random_pool[offset:offset + length]


def _sample_metrics(cursor: Cursor) -> dict:
    if not cursor.db_path.startswith("nvmefs://"):
        return {}
    try:
        rows = cursor.query("SELECT * FROM print_nvmefs_metrics();")
        metrics = {}
        for key, val in rows:
            if val is None:
                metrics[key] = 0
            else:
                try:
                    metrics[key] = int(val)
                except (ValueError, TypeError):
                    metrics[key] = val
        return metrics
    except Exception:
        return {}

def run_ycsb_loop(cursor: Cursor,
                  *,
                  num_fields: int,
                  field_length: int,
                  batch_size: int,
                  row_count: int,
                  iterations: int,
                  duration_seconds: int,
                  interval_seconds: int,
                  checkpoint_mode: str = "auto",
                  worker_handle=None,
                  output_handle=None) -> list[str]:
    """
    YCSB Workload A driven from Python against a Cursor. Uses executemany +
    a pre-allocated random pool to minimize per-iteration Python overhead.
    """
    if checkpoint_mode == "manual":
        cursor.execute("PRAGMA wal_autocheckpoint='4GB';")
    else:
        cursor.execute("PRAGMA wal_autocheckpoint='16MB';")

    set_clause = ", ".join(f"FIELD{i}=${i + 1}" for i in range(num_fields))
    update_sql = (
        f"UPDATE usertable SET {set_clause} "
        f"WHERE YCSB_KEY=${num_fields + 1}"
    )

    key_rng = random.Random()
    val_rng = random.Random()

    rows: list[str] = []
    run_start = time.monotonic()
    interval_start = run_start
    interval_iters = 0
    interval_paused_time = 0.0
    batch_count = 0
    batch_params: list[list] = []

    cursor.execute("BEGIN TRANSACTION;")

    def emit_interval(now: float):
        nonlocal interval_start, interval_iters, interval_paused_time
        if interval_iters == 0:
            return
            
        chronological_ms = (now - interval_start) * 1000.0
        active_ms = chronological_ms - (interval_paused_time * 1000.0)
        
        offset_s = interval_start - run_start
        throughput = (interval_iters / active_ms) * 1000.0 if active_ms > 0 else 0.0
        
        metrics = _sample_metrics(cursor)
        row = (
            f"ycsb_workload_a;{offset_s:.2f};{active_ms:.2f};"
            f"{interval_iters};{throughput:.2f};{json.dumps(metrics)}\n"
        )
        rows.append(row)
        if output_handle is not None:
            output_handle.write(row)
            output_handle.flush()
            
        interval_start = time.monotonic()
        interval_iters = 0
        interval_paused_time = 0.0  # Reset the pause accumulator

    try:
        for i in range(iterations):
            key = f"user{key_rng.randint(0, row_count - 1)}"
            params = [_value_from_pool(val_rng, field_length) for _ in range(num_fields)]
            params.append(key)
            batch_params.append(params)

            interval_iters += 1
            batch_count += 1

            if batch_count >= batch_size:
                cursor.executemany(update_sql, batch_params)
                cursor.execute("COMMIT;")
                cursor.execute("BEGIN TRANSACTION;")
                batch_params.clear()
                batch_count = 0

            if i > 0 and i % 100 == 0:
                if worker_handle is not None and hasattr(worker_handle, "checkpoint_if_requested"):
                    pause_start = time.monotonic()
                    worker_handle.checkpoint_if_requested(f"ycsb-op{i}")
                    interval_paused_time += (time.monotonic() - pause_start)  # Add to accumulator
                
                now = time.monotonic()

                if interval_seconds > 0 and (now - interval_start) >= interval_seconds:
                    emit_interval(now)

                if worker_handle is not None and getattr(worker_handle, "clock", None) is not None:
                    elapsed = worker_handle.clock.elapsed()
                else:
                    elapsed = now - run_start

                if duration_seconds > 0 and elapsed >= duration_seconds:
                    break
    finally:
        if batch_params:
            try:
                cursor.executemany(update_sql, batch_params)
            except Exception:
                pass
            batch_params.clear()
        try:
            cursor.execute("COMMIT;")
        except Exception:
            pass
        emit_interval(time.monotonic())

        if worker_handle is not None and hasattr(worker_handle, "checkpoint_if_requested"):
            worker_handle.checkpoint_if_requested("ycsb-final")

    return rows
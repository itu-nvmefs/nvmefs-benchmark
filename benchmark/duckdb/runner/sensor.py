import time
import random
from datetime import datetime
from database.database import Database

SENSOR_BENCHMARK_NAME = "sensor"
BATCH_SIZE = 100  # Number of Insert/Update pairs per epoch

def setup_sensor_benchmark(db: Database, input_dir_path: str, scale_factor: int):
    db.execute("DROP TABLE IF EXISTS sensor_readings;")
    db.execute("""
        CREATE TABLE sensor_readings (
            ts TIMESTAMP,
            sensor_id INTEGER,
            temperature DOUBLE,
            status VARCHAR
        );
    """)

def run_sensor_epoch_benchmark(db: Database, scale_factor: int) -> list[str]:

    results: list[str] = []
    
    start = time.perf_counter()
    
    for _ in range(BATCH_SIZE):
        val = random.uniform(10.0, 30.0)
        ts = datetime.now()
        s_id = random.randint(1, 1000)

        # Insert/Update pair
        db.execute(f"INSERT INTO sensor_readings VALUES ('{ts}', {s_id}, {val}, 'PENDING')")
        db.execute(f"UPDATE sensor_readings SET status = 'VERIFIED' WHERE sensor_id = {s_id} AND ts = '{ts}'")

    end = time.perf_counter()
    duration_ms = (end - start) * 1000

    # We query DuckDB to see the current WAL size
    wal_size = "0"
    try:
        res = db.query("SELECT wal_size FROM pragma_database_size('bench');")
        if res and res[0]:
            wal_size = str(res[0][0])
    except Exception:
        wal_size = "Error"

    results.append(f"{BATCH_SIZE};{duration_ms};{wal_size}\n")

    return results
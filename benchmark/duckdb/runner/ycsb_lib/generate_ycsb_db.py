import os
import duckdb

NUM_FIELDS = 10
DEFAULT_FIELD_LENGTH = 2000
CHUNK_ROWS = 1_000_000 

def field_expr(i, field_length):
    chunks = (field_length + 31) // 32
    parts = " || ".join(
        f"md5((i * {NUM_FIELDS * 8} + {i * 8 + k})::VARCHAR)"
        for k in range(chunks)
    )
    return f"substr({parts}, 1, {field_length}) AS FIELD{i}"


def generate_ycsb_data(scale_factor: int, output_dir: str,
                       field_length: int = DEFAULT_FIELD_LENGTH,
                       threads: int = 8):
    os.makedirs(output_dir, exist_ok=True)
    db_path = os.path.join(output_dir, f"ycsb-sf{scale_factor}.db")

    if os.path.exists(db_path):
        print(f"SF={scale_factor} already exists at {db_path}.")
        return

    row_count = scale_factor * 100_000
    est_gb = row_count * (NUM_FIELDS * field_length + 64) / 1024**3
    print(f"Generating SF={scale_factor}: {row_count:,} rows, ~{est_gb:.1f} GB, Path: {db_path}")

    conn = duckdb.connect(db_path)
    conn.execute(f"PRAGMA threads={threads};")
    conn.execute("PRAGMA wal_autocheckpoint='1TB';")

    field_cols = ", ".join(f"FIELD{i} VARCHAR" for i in range(NUM_FIELDS))
    conn.execute(
        f"CREATE TABLE usertable (YCSB_KEY VARCHAR PRIMARY KEY, {field_cols});"
    )
    field_exprs = ", ".join(field_expr(i, field_length) for i in range(NUM_FIELDS))

    for start in range(0, row_count, CHUNK_ROWS):
        end = min(start + CHUNK_ROWS, row_count)
        conn.execute(
            f"INSERT INTO usertable "
            f"SELECT 'user' || i AS YCSB_KEY, {field_exprs} "
            f"FROM range({start}, {end}) t(i);"
        )
        print(f"  inserted [{start:,} .. {end:,})")

    conn.execute("CHECKPOINT;")
    conn.close()
    print(f"Successfully generated {db_path}")

if __name__ == "__main__":
    target_sfs = [5] 
    
    # Updated to your requested mount point
    output_directory = "/mnt/data/benchmark/ycsb/"
    
    for sf in target_sfs:
        generate_ycsb_data(sf, output_directory)
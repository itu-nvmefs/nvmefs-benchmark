import os
import duckdb

NUM_FIELDS = 10
FIELD_LENGTH = 2000


def field_expr(i, field_length):
    chunks = (field_length + 31) // 32
    parts = " || ".join(
        f"md5(random()::VARCHAR || {i * 100 + k} || i::VARCHAR)"
        for k in range(chunks)
    )
    return f"substr({parts}, 1, {field_length}) AS FIELD{i}"


def generate_ycsb_data(scale_factor: int, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)
    db_path = os.path.join(output_dir, f"ycsb-sf{scale_factor}.db")

    print(f"Generating YCSB SF {scale_factor} at {db_path}...")

    conn = duckdb.connect(db_path)
    conn.execute("PRAGMA wal_autocheckpoint='1TB';")

    row_count = scale_factor * 100000

    field_cols = ", ".join(f"FIELD{i} VARCHAR" for i in range(NUM_FIELDS))
    conn.execute(f"CREATE TABLE usertable (YCSB_KEY VARCHAR PRIMARY KEY, {field_cols});")

    field_exprs = ", ".join(field_expr(i, FIELD_LENGTH) for i in range(NUM_FIELDS))

    conn.execute(
        f"INSERT INTO usertable "
        f"SELECT 'user' || i AS YCSB_KEY, {field_exprs} "
        f"FROM range({row_count}) t(i);"
    )

    conn.execute("CHECKPOINT;")
    conn.close()
    print(f"Successfully generated {db_path}")

if __name__ == "__main__":
    target_sfs = [25] 
    
    # Updated to your requested mount point
    output_directory = "/mnt/data/benchmark/ycsb/"
    
    for sf in target_sfs:
        generate_ycsb_data(sf, output_directory)
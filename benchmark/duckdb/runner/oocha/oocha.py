import os
import csv
import time
from database.database import Database

OOCHA_SPILL_BENCHMARK_NAME = "oocha-spill"

def setup_oocha_spill_benchmark(db: Database, input_dir_path: str, scale_factor: int):
    input_file_path = os.path.join(input_dir_path, f"oocha-{scale_factor}.db")

    db.execute(f"ATTACH DATABASE '{input_file_path}' AS oocha;")
    db.execute("COPY FROM DATABASE oocha TO bench;")
    db.execute("DETACH DATABASE oocha;")
    db.execute("PRAGMA force_compression = 'uncompressed';")
    db.execute("PRAGMA disable_object_cache;")


def run_oocha_spill_epoch_benchmark(db: Database, scale_factor: int):

    start = time.perf_counter()
    db.query(f"""SELECT count(*) FROM (SELECT distinct(l_orderkey) FROM lineitem)""")
    end = time.perf_counter()

    # Get query elapsed time in milliseconds
    query_elapsed = (end - start) * 1000

    return [f"{query_elapsed}\n"]

OOCHA_BENCHMARK_NAME = "oocha"
def setup_oocha_benchmark(db: Database, input_dir_path: str, scale_factor: int):
    input_file_path = os.path.join(input_dir_path, f"oocha-{scale_factor}.db")

    db.execute(f"ATTACH DATABASE '{input_file_path}' AS oocha;")
    db.execute("COPY FROM DATABASE oocha TO bench;")
    db.execute("DETACH DATABASE oocha;")
    db.execute("PRAGMA disable_object_cache;")

def _getqueries(queries_dir: str):
    thin_queries_dir = os.path.join(queries_dir, "thin")
    wide_queries_dir = os.path.join(queries_dir, "wide")
    
    wides = [False, True]
    queries = []
    for wide in wides:
        source_dir = wide_queries_dir if wide else thin_queries_dir
        for file_name in os.listdir(source_dir):
            file_path = f'{source_dir}/{file_name}'
            with open(file_path, 'r') as f:
                queries.append((file_name.split('.')[0], wide, f.read()))

    return queries

def _getcounts(queries_dir: str, scale_factor: int):
    counts_filepath = f"{queries_dir}/counts-{scale_factor}.csv"
    query_counts = {}

    with open(counts_filepath, newline='\n') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        next(reader, None) # Skip the header

        for group, count in reader:
            query_counts[group] = int(count)

    return query_counts

def run_oocha_epoch_benchmark(db: Database, scale_factor: int):

    results = []
    queries_dir = "./runner/oocha/queries"
    queries = _getqueries(queries_dir)
    query_counts = _getcounts(queries_dir, scale_factor)

    # counts_con = duckdb.connect()
    for grouping, wide, query in queries:
        print(f"Running {grouping} {'wide' if wide else 'thin'} query")
        count = query_counts[grouping]
        prepared_query = query.replace('offset', f'{count - 1}')

        start = time.perf_counter()
        db.query(prepared_query)
        end = time.perf_counter()

        # Get query elapsed time in milliseconds
        query_elapsed = (end - start) * 1000
        results.append(f"{grouping};{wide};{query_elapsed}\n")

    return results
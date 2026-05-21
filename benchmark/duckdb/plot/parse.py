import csv
import json
import datetime
from dataclasses import dataclass
from collections import defaultdict

@dataclass
class BenchmarkRun:
    benchmark: str
    run_type: str
    device: str
    backend: str
    span: int
    span_type: str
    memory: int
    scale_factor: int
    threads: int
    parallel: bool
    parallel_threads: int
    fdp: bool
    results: dict = None
    metrics: dict = None
    throughputs: dict = None
    raw_series: list = None

# --- Helper Functions ---
def safe_float(val):
    """Attempt to parse as float; return original string (e.g. 'FAIL') if it fails."""
    try:
        return float(val)
    except ValueError:
        return val

def parse_nvmefs_metrics(json_str: str) -> dict:
    try:
        return json.loads(json_str)
    except Exception:
        return {}

def parse_filename(filepath: str):
    """
    Parse the filename to extract the benchmark name and parameters.
    oocha-reps4-normal-mem20000-io_uring_cmd-sf2-t16-s-nofdp
    """
    filename = filepath.split("/")[-1]
    parts = filename.split("-")
    
    has_dash_in_benchmark_name = not (parts[1].startswith("reps") or parts[1].startswith("dur"))
    benchmark_name = f"{parts[0]}-{parts[1]}" if has_dash_in_benchmark_name else parts[0]
    index = 2 if has_dash_in_benchmark_name else 1

    span_type = "duration" if "dur" in parts[index] else "repetition"
    span = int(parts[index][3:]) if span_type == "duration" else int(parts[index][4:])
    index += 1

    device = parts[index]
    index += 1
    memory = int(parts[index][3:])
    index += 1
    backend = parts[index]
    index += 1
    scale_factor = int(parts[index][2:])
    index += 1
    threads = int(parts[index][1:])
    index += 1
    parallel = False if "s" in parts[index] else True
    parallel_threads = int(parts[index][1:]) if parallel else 1
    index += 1
    fdp = True if "fdp" == parts[index].split(".")[0] else False
    
    return BenchmarkRun(
        benchmark=benchmark_name, 
        run_type=span_type, 
        device=device,
        backend=backend,
        span_type=span_type,
        span=span,
        memory=memory,
        scale_factor=scale_factor,
        threads=threads,
        parallel=parallel,
        parallel_threads=parallel_threads,
        fdp=fdp
    )

def parse_device_results(filepath: str) -> BenchmarkRun:
    benchmark = parse_filename(filepath)
    device_data = []
    start_time = None
    
    with open(filepath, newline="") as csvfile:
        reader = csv.reader(csvfile, delimiter=";")
        for row in reader:
            if not row or row[0] == "timestamp": continue
            if len(row) < 6: continue
            
            # Parse the timestamp string into a datetime object
            try:
                dt = datetime.datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S.%f")
            except ValueError:
                try:
                    dt = datetime.datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    continue
            
            # Record the very first timestamp in the file as T_0
            if start_time is None:
                start_time = dt
            
            phase = row[2]
            if phase != "post-drain":
                continue
            
            host_med = row[3].split(",")
            host_w = int(host_med[0]) if len(host_med) == 2 else 0
            media_w = int(host_med[1]) if len(host_med) == 2 else 0
            
            # Calculate elapsed time in minutes
            elapsed_min = (dt - start_time).total_seconds() / 60.0
            
            device_data.append({
                "timestamp": row[0],
                "elapsed_min": elapsed_min,
                "phase": phase,
                "host_written": host_w,
                "media_written": media_w,
                "waf": safe_float(row[4]),
                "cumulative_waf": safe_float(row[5]) 
            })
            
    benchmark.results = {"device": device_data}
    return benchmark

def parse_tpch_results(filepath: str) -> BenchmarkRun:
    benchmark = parse_filename(filepath)
    benchmark.results = defaultdict(list)
    benchmark.metrics = defaultdict(list)
    benchmark.raw_series = []  
    
    with open(filepath, newline="") as csvfile:
        reader = csv.reader(csvfile, delimiter=";")
        for row in reader:
            if not row or row[0] == "query_name": continue
            
            # --- NEW: Skip the query if it failed ---
            if row[1] == "FAIL":
                continue
                
            query_nr = int(row[0])
            latency = safe_float(row[1])
            
            benchmark.results[query_nr].append(latency)
            if len(row) >= 3:
                metrics = parse_nvmefs_metrics(row[2])
                benchmark.metrics[query_nr].append(metrics)
                
                # We need this to reconstruct the timeline!
                benchmark.raw_series.append({
                    "query_name": query_nr,
                    "latency_ms": latency,  # <--- MAKE SURE THIS IS HERE
                    "metrics": metrics
                })
                
    return benchmark

def parse_ycsb_results(filepath: str) -> BenchmarkRun:
    benchmark = parse_filename(filepath)
    benchmark.results = defaultdict(list)
    benchmark.throughputs = defaultdict(list)
    benchmark.metrics = defaultdict(list)
    benchmark.raw_series = []
    
    with open(filepath, newline="") as csvfile:
        reader = csv.reader(csvfile, delimiter=";")
        for row in reader:
            if not row or "ycsb_workload" not in row[0]: continue
            workload = row[0]
            
            offset_s = safe_float(row[1])
            
            interval_ms = safe_float(row[2]) 
            
            throughput = safe_float(row[4]) if len(row) > 4 else safe_float(row[2])
            metrics = parse_nvmefs_metrics(row[-1]) if len(row) >= 4 else {}
            
            benchmark.raw_series.append({
                "offset_s": offset_s,
                "interval_ms": interval_ms, 
                "throughput_ops": throughput,
                "metrics": metrics
            })
            
            benchmark.results[workload].append(offset_s)
            if len(row) >= 4:
                benchmark.throughputs[workload].append(throughput)
                benchmark.metrics[workload].append(metrics)
                
    return benchmark

def parse_htap_results(filepath: str) -> BenchmarkRun:
    benchmark = parse_filename(filepath)
    benchmark.results = defaultdict(list)
    benchmark.throughputs = defaultdict(list)
    benchmark.metrics = defaultdict(list)
    
    with open(filepath, newline="") as csvfile:
        reader = csv.reader(csvfile, delimiter=";")
        for row in reader:
            if not row or row[0] == "benchmark": continue
            task_name = row[1]
            benchmark.results[task_name].append(safe_float(row[2]))
            if len(row) == 5:
                benchmark.throughputs[task_name].append(safe_float(row[3]))
                benchmark.metrics[task_name].append(parse_nvmefs_metrics(row[4]))
            elif len(row) == 4:
                benchmark.metrics[task_name].append(parse_nvmefs_metrics(row[3]))
                
    return benchmark

def parse_oocha_results(filepath: str) -> BenchmarkRun:
    oocha_groupings = {
        "l_returnflag-l_linestatus": 1, 
        "l_partkey": 2,
        "l_partkey-l_returnflag-l_linestatus": 3,
        "l_suppkey-l_partkey": 4,
        "l_orderkey": 5,
        "l_orderkey-l_returnflag-l_linestatus": 6,
        "l_suppkey-l_partkey-l_returnflag-l_linestatus": 7,
        "l_suppkey-l_partkey-l_shipinstruct": 8,
        "l_suppkey-l_partkey-l_shipmode": 9,
        "l_suppkey-l_partkey-l_shipinstruct-l_shipmode": 10,
        "l_orderkey-l_partkey": 11,
        "l_orderkey-l_suppkey": 12,
        "l_suppkey-l_partkey-l_orderkey": 13
    }

    benchmark = parse_filename(filepath)
    grouped_results = defaultdict(lambda: list())
    
    with open(filepath, newline='\n') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')

        for group, wide, elapsed_ms in reader:
            if group not in oocha_groupings:
                continue

            is_wide = True if wide == "True" else False
            grouped_results[(oocha_groupings[group], is_wide)].append(safe_float(elapsed_ms))

    benchmark.results = grouped_results
    return benchmark

def parse_oocha_spill_waf_results(filepath: str) -> BenchmarkRun:
    benchmark = parse_filename(filepath)
    grouped_results = defaultdict(lambda: list())
    
    with open(filepath, newline='\n') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')

        minute = 0
        for _, bytes_written, waf in reader:
            host_written_bytes, media_written_bytes = map(int, bytes_written.split(","))
            grouped_results[minute].append((host_written_bytes, media_written_bytes, safe_float(waf)))
            minute += 10

    benchmark.results = grouped_results
    return benchmark

def parse_oocha_spill_elapsed_results(filepath: str) -> BenchmarkRun:
    benchmark = parse_filename(filepath)
    grouped_results = defaultdict(lambda: list())
    
    with open(filepath, newline='\n') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')

        for row in reader:
            elapsed_ms = row[0]
            grouped_results[1].append(safe_float(elapsed_ms))

    benchmark.results = grouped_results
    return benchmark
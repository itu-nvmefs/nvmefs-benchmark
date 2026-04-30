import os
from threading import Thread
import time
from typing import Callable

from runner.factory import create_benchmark_runner, get_namespace_count
from device.nvme import NvmeDevice, setup_device, calculate_waf, NvmeDeviceNamespace
from database import database
from datetime import datetime
import multiprocessing.pool
from args import Arguments

type SetupFunc = Callable[[], tuple[list[database.Database],NvmeDevice]]

def prepare_setup_func(args: Arguments, namespace_count: int = 1) -> SetupFunc:
    """
    Prepare the database configuration and database extensions that are needed depending on the storage device
    """
    device = NvmeDevice(args.device) if args.device else None

    def setup_nvme():
        dbs = []
        namespaces = []

        for ns_id in range(1, namespace_count + 1):
            if not args.skip_reset:
                device_namespace, _ = setup_device(device, namespace_id=ns_id, enable_fdp=args.use_fdp, size_blocks=args.namespace_size, precondition=args.precondition)
            else:
                print(f"Use existing namespace {ns_id}...")
                device_namespace = NvmeDeviceNamespace(device.device_path, ns_id, args.namespace_size)
            namespaces.append(device_namespace)

        time.sleep(5)

        for ns_id, device_namespace in enumerate(namespaces, start=1):
            device_path = device_namespace.get_generic_device_path() if args.use_generic_device else device_namespace.get_device_path()
            print(f"Using device path: {device_path}")

            config = database.ConnectionConfig(
                device_path, 
                args.io_backend, 
                args.use_fdp,
                args.fdp_strategy,
                args.buffer_manager_mem_size,
                args.threads,
                ns_id)

            db = database.connect(f"nvmefs:///bench_ns{ns_id}.db", args.threads, args.buffer_manager_mem_size, config)
            dbs.append(db)
        return dbs, device
    
    def setup_normal():
        dbs = []
        for ns_id in range(1, namespace_count + 1):
            _, mount_path = setup_device(device, namespace_id=ns_id, should_mount=args.should_mount, size_blocks=args.namespace_size, precondition=args.precondition)
            normal_db_path = os.path.join(mount_path, f"bench_ns{ns_id}.db")

            time.sleep(5)

            db = database.connect(normal_db_path, args.threads, args.buffer_manager_mem_size)
            temp_dir = os.path.join(mount_path, ".tmp")
            db.execute(f"SET temp_directory = '{temp_dir}';")
            dbs.append(db)
        return dbs, device

    return setup_nvme if not args.should_mount else setup_normal

def run_concurrent_benchmark(num_threads: int, benchmark_runner, db: database.Database, span: int):

    def _run(db: database.Database, span: int):
        db.execute("USE bench;")
        return benchmark_runner(db, span)

    with multiprocessing.pool.ThreadPool(processes=num_threads) as pool:
        flattened_results = []
        results = pool.starmap(
                _run,
                [(db.create_concurrent_connection(), span) for _ in range(num_threads)],
                chunksize=1
        )
    merged = {}
    for r in results:
        for key, rows in r.items():
            merged.setdefault(key, []).extend(rows)
    return merged

# Global flag for WAF measurement thread
RUN_MEASUREMENT = True

def start_device_measurements(device: NvmeDevice, file_name: str, enable_fdp: bool):
    """
    Starts a background thread to measure device-level Write Amplification Factor (WAF)
    Returns a function to stop measurement
    """

    global RUN_MEASUREMENT
    RUN_MEASUREMENT = True

    os.system("sync")

    start_host, start_media = device.get_written_bytes()
    start_fdp_host, start_fdp_media = device.get_written_bytes_fdp() if enable_fdp else (0, 0)

    with open(file_name, "w", newline="\n") as waf_file:
        waf_file.write("timestamp;source;host_written,media_written;waf;cumulative_waf\n")

        timestamp = datetime.now()
        waf_file.write(f"{timestamp};smart-log;{start_host},{start_media};0.0000;0.0000\n")
        if enable_fdp:
            waf_file.write(f"{datetime.now()};fdp-stats;{start_fdp_host},{start_fdp_media};0.0000;0.0000\n")

    def run():
        previous_host_written, previous_media_written = start_host, start_media
        previous_fdp_host, previous_fdp_media = start_fdp_host, start_fdp_media

        while RUN_MEASUREMENT:
            for _ in range(660): # 11 Minute interval
                if not RUN_MEASUREMENT: return
                time.sleep(1)

            # Calculate the Write Amplification Factor
            os.system("sync")
            timestamp = datetime.now()

            # Smart Logs (WAF)
            current_host_written, current_media_written = device.get_written_bytes()

            diff_host_written = current_host_written - previous_host_written
            diff_media_written = current_media_written - previous_media_written
            interval_waf = calculate_waf(diff_host_written, diff_media_written)

            cumulative_host = current_host_written - start_host
            cumulative_media = current_media_written - start_media
            cumulative_waf = calculate_waf(cumulative_host, cumulative_media)

            with open(file_name, "a", newline="\n") as waf_file:
                waf_file.write(f"{timestamp};smart-log;{diff_host_written},{diff_media_written};{interval_waf:.4f};{cumulative_waf:.4f}\n")

            previous_host_written = current_host_written
            previous_media_written = current_media_written

            # FDP Stats (WAF)
            if enable_fdp:
                current_fdp_host, current_fdp_media = device.get_written_bytes_fdp()

                diff_host_fdp = current_fdp_host - previous_fdp_host
                diff_media_fdp = current_fdp_media - previous_fdp_media
                interval_fdp_waf = calculate_waf(diff_host_fdp, diff_media_fdp)

                cumulative_fdp_host = current_fdp_host - start_fdp_host
                cumulative_fdp_media = current_fdp_media - start_fdp_media
                cumulative_fdp_waf = calculate_waf(cumulative_fdp_host, cumulative_fdp_media)

                with open(file_name, "a", newline="\n") as waf_file:
                    waf_file.write(f"{timestamp};fdp-stats;{diff_host_fdp},{diff_media_fdp};{interval_fdp_waf:.4f};{cumulative_fdp_waf:.4f}\n")

                previous_fdp_host = current_fdp_host
                previous_fdp_media = current_fdp_media

        print("WAF measurement complete")

    print("Starting WAF measurement")
    waf_measurement_thread = Thread(target=run, daemon=True)
    waf_measurement_thread.start()

    def stop_measurement():
        print("Stopping WAF measurement")
        global RUN_MEASUREMENT
        RUN_MEASUREMENT = False
        waf_measurement_thread.join()

        os.system("sync")
        timestamp = datetime.now()

        # Smart log
        end_host_written, end_media_written = device.get_written_bytes()
        total_diff_host = end_host_written - start_host
        total_diff_media = end_media_written - start_media
        waf = calculate_waf(total_diff_host, total_diff_media)

        with open(file_name, "a", newline="\n") as waf_file:
            waf_file.write(f"{timestamp};smart-log;{total_diff_host},{total_diff_media};{waf:.4f};{waf:.4f}\n")

        # FDP Stats
        if enable_fdp:
            end_fdp_host, end_fdp_media = device.get_written_bytes_fdp()
            total_fdp_host = end_fdp_host - start_fdp_host
            total_fdp_media = end_fdp_media - start_fdp_media
            fdp_waf = calculate_waf(total_fdp_host, total_fdp_media)

            with open(file_name, "a", newline="\n") as waf_file:
                waf_file.write(f"{timestamp};fdp-stats;{total_fdp_host},{total_fdp_media};{fdp_waf:.4f};{fdp_waf:.4f}\n")

    return stop_measurement

def _scale_factor_name(args):
    if args.benchmark == "tpch":
        return f"sf{args.tpch_sf}"
    elif args.benchmark == "ycsb":
        return f"sf{args.ycsb_sf}"
    elif args.benchmark == "htap":
        return f"tsf{args.tpch_sf}-ysf{args.ycsb_sf}"
    else:
        return f"sf{args.tpch_sf}"

HEADERS = {
    "tpch": "query_name;latency_ms;nvmefs_metrics\n",
    "ycsb": "workload_name;offset_s;interval_ms;iterations;throughput_ops;nvmefs_metrics\n",
    "oocha": "grouping;wide;latency_ms\n",
    "oocha-spill": "latency_ms\n",
}

def generate_filenames(args: Arguments) -> tuple[str, str]:
    run_with_duration = args.duration > 0
    duration_display = f"dur{args.duration}" if run_with_duration else f"reps{args.repetitions}"
    parallel = f"p{args.parallel}" if args.parallel > 0 else "s"
    fdp_name = args.fdp_strategy if args.use_fdp else "nofdp"
    device_name = "nvme" if not args.should_mount else "normal"
    scale_factor = _scale_factor_name(args)

    name = f"{args.benchmark}-{duration_display}-{device_name}-mem{args.buffer_manager_mem_size}-{args.io_backend}-{scale_factor}-t{args.threads}-{parallel}-{fdp_name}"

    run_id = (getattr(args, "run_id", None)
          or os.environ.get("SUITE_TIMESTAMP")
          or datetime.now().strftime("%Y%m%d_%H%M%S"))
    target_dir = os.path.join("results", args.benchmark, run_id)
    os.makedirs(target_dir, exist_ok=True)

    device_output_file = os.path.join(target_dir, f"{name}-device.csv")

    if args.benchmark == "htap":
        output_files = {
            "tpch": os.path.join(target_dir, f"{name}-tpch.csv"),
            "ycsb": os.path.join(target_dir, f"{name}-ycsb.csv"),
        }
    else:
        output_files = {args.benchmark: os.path.join(target_dir, f"{name}.csv")}

    return output_files, device_output_file

if __name__ == "__main__":
    args: Arguments = Arguments.parse_args()

    initial_device = NvmeDevice(args.device) if args.device else None
    if not args.skip_reset and initial_device:
        # Device reset and preconditioning
        print("Resetting device to ensure consistent state...")
        initial_device.reset()
        initial_device.number_of_blocks, initial_device.unallocated_number_of_blocks = initial_device._NvmeDevice__get_device_info()

    namespace_count = get_namespace_count(args.benchmark)

    setup_device_and_db = prepare_setup_func(args, namespace_count)
    output_file, device_output_file = generate_filenames(args)

    run_with_duration = args.duration > 0
    run_benchmark, setup_benchmark = create_benchmark_runner(
        args.benchmark, run_with_duration, args.checkpoint_mode,
        tpch_sf=args.tpch_sf, ycsb_sf=args.ycsb_sf,
    )
    
    # Setup the database with the correct device config
    dbs, device = setup_device_and_db()
    print(f"Setting up benchmark using {args.threads} threads and {args.buffer_manager_mem_size} MB of memory")
    setup_benchmark(dbs, args.input_dir)
    metric_results = []

    # Run the benchmark
    stop_measurement = start_device_measurements(device, device_output_file, enable_fdp=args.use_fdp)

    if args.parallel > 0:
        print(f"Running benchmark with {args.parallel} parallel executions")
        metric_results = run_concurrent_benchmark(args.parallel, run_benchmark, dbs, args.duration if run_with_duration else args.repetitions)
    else:
        print(f"Running benchmark with sequential execution")
        metric_results = run_benchmark(dbs, args.duration if run_with_duration else args.repetitions) 

    stop_measurement()

    # Write the results to a CSV file
    for key, path in output_file.items():
        with open(path, "w", newline="\n") as f:
            f.write(HEADERS.get(key, "name;metrics\n"))
            for row in metric_results[key]:
                f.write(row)
    
    for db in dbs:
        try:
            db.close()
        except:
            pass

    print(f"Benchmark results: {list(output_file.values())}")
    print(f"WAF results: {device_output_file}")

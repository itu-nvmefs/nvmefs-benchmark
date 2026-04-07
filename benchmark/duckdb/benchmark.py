import subprocess
import os
from threading import Thread
import time
from typing import Callable
from runner.factory import create_benchmark_runner
from device.nvme import NvmeDevice, setup_device, calculate_waf
from database import database
from datetime import datetime
import multiprocessing.pool
from args import Arguments

type SetupFunc = Callable[[], tuple[database.Database,NvmeDevice]]

def prepare_setup_func(args: Arguments) -> SetupFunc:
    """
    Prepare the database configuration and database extensions that are needed depending on the storage device
    """
    device = NvmeDevice(args.device) if args.device else None

    def setup_nvme():
        device_namespace = setup_device(device, namespace_id=args.namespace_id, enable_fdp=args.use_fdp, size_blocks=args.namespace_size)
        device_path = device_namespace.get_generic_device_path() if args.use_generic_device else device_namespace.get_device_path()

        print(f"Using device path: {device_path}")

        config = database.ConnectionConfig(
            device_path, 
            args.io_backend, 
            args.use_fdp,
            args.fdp_strategy,
            args.buffer_manager_mem_size,
            args.threads)

        db = database.connect("nvmefs:///bench.db", args.threads, args.buffer_manager_mem_size, config)
        return db, device
    
    def setup_normal():
        normal_db_path = os.path.join(args.mount_path, "bench.db")
        setup_device(device, namespace_id=args.namespace_id, mount_path=args.mount_path, size_blocks=args.namespace_size)
        db = database.connect(normal_db_path, args.threads, args.buffer_manager_mem_size)
        temp_dir = os.path.join(args.mount_path, ".tmp")        
        db.execute(f"SET temp_directory = '{temp_dir}';")

        return db, device

    return setup_nvme if args.mount_path is None else setup_normal

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
        for result in results:
            flattened_results.extend(result)
        return flattened_results

# Global flag for WAF measurement thread
RUN_MEASUREMENT = True

def start_device_measurements(device: NvmeDevice, file_name: str):
    """
    Starts a background thread to measure device-level Write Amplification Factor (WAF)
    Returns a function to stop measurement
    """

    def run():
        global RUN_MEASUREMENT
        os.system("sync")

        previous_host_written, previous_media_written = device.get_written_bytes()

        with open(file_name, "w+", newline="\n") as waf_file:
            while RUN_MEASUREMENT:
                time.sleep(600)  # 10 minutes interval
                os.system("sync")

                host_written, media_written = device.get_written_bytes()
                if host_written == 0:
                    continue

                # Calculate the Write Amplification Factor
                diff_host_written = host_written - previous_host_written
                diff_media_written = media_written - previous_media_written
                waf = calculate_waf(diff_host_written, diff_media_written)

                waf_file.write(f"{datetime.now()};{diff_host_written},{diff_media_written};{waf}\n")
                waf_file.flush()
                os.fsync(waf_file.fileno())

                previous_host_written = host_written
                previous_media_written = media_written

        print("WAF measurement complete")

    print("Starting WAF measurement")
    waf_measurement_thread = Thread(target=run, args=(device, file_name))
    waf_measurement_thread.start()

    def stop_measurement():
        print("Stopping WAF measurement")
        global RUN_MEASUREMENT
        RUN_MEASUREMENT = False
        waf_measurement_thread.join()

    return stop_measurement

def generate_filenames(args: Arguments) -> tuple[str, str]:
    run_with_duration = args.duration > 0
    duration_display = f"dur{args.duration}" if run_with_duration else f"reps{args.repetitions}"
    parallel = f"p{args.parallel}" if args.parallel > 0 else "s"
    fdp_name = args.fdp_strategy if args.use_fdp else "nofdp"
    device_name = "nvme" if args.mount_path is None else "normal"

    name = f"{args.benchmark}-{duration_display}-{device_name}-mem{args.buffer_manager_mem_size}-{args.io_backend}-sf{args.scale_factor}-t{args.threads}-{parallel}-{fdp_name}"

    return f"{name}.csv", f"{name}-device.csv"

if __name__ == "__main__":
    args: Arguments = Arguments.parse_args()

    # Device reset and preconditioning
    print("Resetting device to ensure consistent state...")
    initial_device = NvmeDevice(args.device)
    initial_device.reset()

    setup_device_and_db = prepare_setup_func(args)
    output_file, device_output_file = generate_filenames(args)

    run_with_duration = args.duration > 0
    run_benchmark, setup_benchmark = create_benchmark_runner(args.benchmark, args.scale_factor, run_with_duration)

    # Setup the database with the correct device config
    db, device = setup_device_and_db()
    print(f"Setting up benchmark using {args.threads} threads and {args.buffer_manager_mem_size} MB of memory")
    setup_benchmark(db, args.input_dir, args.scale_factor)
    metric_results = []

    # Run the benchmark
    stop_measurement = start_device_measurements(device, device_output_file)

    if args.parallel > 0:
        print(f"Running benchmark with {args.parallel} parallel executions")
        metric_results = run_concurrent_benchmark(args.parallel, run_benchmark, db, args.duration if run_with_duration else args.repetitions)
    else:
        print(f"Running benchmark with sequential execution")
        metric_results = run_benchmark(db, args.duration if run_with_duration else args.repetitions) 

    stop_measurement()

    # Write the results to a CSV file
    with open(output_file, mode="w", newline="\n") as file:
        for result in metric_results:
            file.write(result)
    
    db.close()

    print(f"Benchmark results written to {output_file} and WAF results written to {device_output_file}")

import os
import time
import multiprocessing
import multiprocessing.pool
import subprocess
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Barrier

from args import Arguments
from database import database
from datetime import datetime
from threading import Thread, Event
from runner.coordinator import _PassthroughCoordinator, IPCWorkerHandleParent, WAFCheckpoint
from runner.factory import create_benchmark_runner, get_namespace_count, derive_db_names
from device.nvme import NvmeDevice, setup_device, calculate_waf, NvmeDeviceNamespace, fill_namespace_with_data, parse_fdp_handles

def prepare_setup_func(args: Arguments, namespace_identities: list):
    device = NvmeDevice(args.device) if args.device else None
    target_nsid = 2 if args.filler else 1

    def setup_nvme():
        fdp_handles = parse_fdp_handles(args.fdp_mapping) if args.use_fdp else []

        if not args.skip_reset:
            workload_ns, _ = setup_device(
                device, namespace_id=target_nsid, enable_fdp=args.use_fdp,
                size_blocks=args.namespace_size, precondition=args.precondition,
                fio_file=args.fio_file, settle_seconds=args.settle_seconds,
                dsm_after_precondition=args.dsm_after_preconditioning,
                fdp_handles=fdp_handles
            )
        else:
            workload_ns = NvmeDeviceNamespace(device.device_path, target_nsid, args.namespace_size)
        time.sleep(5)

        db_names = derive_db_names(namespace_identities)
        db_configs_dict = args.parse_db_configs()

        if len(db_names) > 1 and not db_configs_dict:
            raise ValueError(f"Multi-workload run {db_names} requires --db_configs.")
        if db_configs_dict:
            missing = [n for n in db_names if n not in db_configs_dict]
            if missing:
                raise ValueError(
                    f"--db_configs missing entries for: {missing}. "
                    f"Got {list(db_configs_dict.keys())}, need {db_names}."
                )

        device_path = workload_ns.get_generic_device_path() if args.use_generic_device else workload_ns.get_device_path()
        config = database.ConnectionConfig(
            device=device_path,
            backend=args.io_backend,
            use_fdp=args.use_fdp,
            fdp_mapping=args.fdp_mapping,
            memory=args.get_memory_limit(),
            threads=args.threads,
            ns_id=target_nsid,
            extension_path=args.extension_path,
            db_configs=db_configs_dict,
        )

        print(f"[setup] use_fdp={args.use_fdp} mapping={args.fdp_mapping!r} "
              f"dbs={db_names} db_configs={db_configs_dict}")

        db = database.Database(args.threads, args.get_memory_limit(),
                               args.temp_size, config)
        cursors = [db.attach(name) for name in db_names]
        return db, cursors, device

    def setup_normal():
        _, mount_path = setup_device(
            device, namespace_id=target_nsid, should_mount=args.should_mount,
            size_blocks=args.namespace_size, precondition=args.precondition,
            fio_file=args.fio_file, settle_seconds=args.settle_seconds,
            dsm_after_precondition=args.dsm_after_preconditioning,
        )
        time.sleep(5)
        args.mount_path = mount_path

        db = database.Database(args.threads, args.get_memory_limit(), args.temp_size)
        db_names = derive_db_names(namespace_identities)
        cursors = [db.attach(name, mount_path=mount_path) for name in db_names]
        return db, cursors, device

    return setup_nvme if not args.should_mount else setup_normal

def setup_namespaces_for_multi(args: Arguments, benchmarks: list,
                               unallocated_blocks: int = 0):
    device = NvmeDevice(args.device) if args.device else None

    db_configs_dict = args.parse_db_configs()
    ns_sizes_dict = args.parse_ns_sizes()
    workload_blocks_dict = args.parse_workload_blocks()
    temp_sizes_dict = args.parse_temp_sizes()

    # Auto-derive at most one missing ns size from device free capacity.
    missing = [b for b in benchmarks if b not in ns_sizes_dict]
    if missing:
        if len(missing) > 1:
            raise ValueError(
                f"--ns_sizes missing entries for {missing}; can auto-derive "
                f"at most one namespace from device free capacity."
            )
        specified_total = sum(ns_sizes_dict.values())
        remainder = unallocated_blocks - specified_total
        needed = workload_blocks_dict.get(missing[0], 0)
        if remainder <= needed:
            raise ValueError(
                f"Cannot auto-size '{missing[0]}': device has "
                f"{unallocated_blocks} unallocated blocks, "
                f"{specified_total} already specified, leaving "
                f"{remainder} blocks -- not enough for "
                f"'{missing[0]}'s workload ({needed} blocks)."
            )
        ns_sizes_dict[missing[0]] = remainder
        print(f"[setup] Auto-sized '{missing[0]}' ns to {remainder} blocks "
              f"(unallocated {unallocated_blocks} - specified {specified_total})")

    fdp_handles = parse_fdp_handles(args.fdp_mapping) if args.use_fdp else []

    namespaces: list[NvmeDeviceNamespace] = []
    if not args.skip_reset:
        # Create namespaces
        for i, b in enumerate(benchmarks):
            ns_id = i + 1
            ns_blocks = ns_sizes_dict[b]
            wb = workload_blocks_dict[b]
            print(f"[parent]   -> Creating NS {ns_id} for '{b}': "
                  f"ns_blocks={ns_blocks}, workload_blocks={wb}")
            ns = device.create_workload_namespace(
                namespace_id=ns_id,
                ns_size_blocks=ns_blocks,
                workload_blocks=wb,
                enable_fdp=args.use_fdp,
                fdp_handles=fdp_handles,
            )
            namespaces.append(ns)

        # Fill filler region (no-op when workload covers entire ns)
        for ns, b in zip(namespaces, benchmarks):
            device.fill_filler_region(
                ns,
                workload_blocks=workload_blocks_dict[b],
                ns_size_blocks=ns_sizes_dict[b],
                passes=2,
            )

        # Precondition workload region
        if args.precondition:
            print(f"[parent] Preconditioning {len(namespaces)} workload "
                  f"regions concurrently...")
            with ThreadPoolExecutor(max_workers=len(namespaces)) as ex:
                futures = {
                    ex.submit(
                        device.precondition_workload_region,
                        ns,
                        workload_blocks=workload_blocks_dict[b],
                        sequential_passes=2,
                        random_write_seconds=args.random_write_seconds,
                    ): b
                    for ns, b in zip(namespaces, benchmarks)
                }
                for f in futures:
                    b = futures[f]
                    f.result()
                    print(f"[parent]   -> Preconditioning '{b}' finished")
            if args.settle_seconds > 0:
                print(f"[parent] Settling for {args.settle_seconds}s ...")
                time.sleep(args.settle_seconds)

        if args.precondition or args.dsm_after_preconditioning:
            for ns, b in zip(namespaces, benchmarks):
                device.dsm_workload_region(ns, workload_blocks_dict[b])
        time.sleep(5)
    else:
        for i, b in enumerate(benchmarks):
            ns_id = i + 1
            namespaces.append(
                NvmeDeviceNamespace(device.device_path, ns_id, ns_sizes_dict[b])
            )

    namespace_infos = []
    if args.should_mount:
        print("[parent] Formatting and mounting workload regions...")
        mount_paths = []
        for ns, b in zip(namespaces, benchmarks):
            mp = device.format_and_mount_workload_region(
                ns, workload_blocks_dict[b]
            )
            mount_paths.append(mp)
        args.mount_paths = mount_paths
        for i, (b, mp) in enumerate(zip(benchmarks, mount_paths)):
            namespace_infos.append({"benchmark": b, "ns_id": i + 1, "mount_path": mp})
    else:
        for i, b in enumerate(benchmarks):
            namespace_infos.append({"benchmark": b, "ns_id": i + 1, "mount_path": None})

    return device, namespace_infos

def _run_workload_in_subprocess(benchmark_name: str, ns_id: int,
                                args: Arguments, duration_or_reps: int,
                                target_dir: str, base_name: str, task_idx: int,
                                mount_path: str = None,
                                start_barrier = None,
                                pause_req=None, paused_ack=None, release_ev=None
                                
                                ):
    db_configs_dict = args.parse_db_configs()
    temp_sizes_dict = args.parse_temp_sizes()
    temp_gb = temp_sizes_dict[benchmark_name]
    mem_mb = args.get_memory_limit_for(benchmark_name)

    if args.should_mount and mount_path:
        print(f"[child:{benchmark_name}] DB on mount {mount_path} "
              f"(ns_id={ns_id}, mem={mem_mb}MB, temp={temp_gb}GB)", flush=True)
        db = database.Database(args.threads, mem_mb, temp_gb)
        cursor = db.attach(benchmark_name, mount_path=mount_path)
    else:
        # Namespace already exists; size doesn't matter for the connection.
        ns = NvmeDeviceNamespace(args.device, ns_id, 0)
        device_path = (ns.get_generic_device_path()
                       if args.use_generic_device
                       else ns.get_device_path())


        config = database.ConnectionConfig(
            device=device_path,
            backend=args.io_backend,
            use_fdp=args.use_fdp,
            fdp_mapping=args.fdp_mapping,
            memory=mem_mb,
            threads=args.threads,
            ns_id=ns_id,
            extension_path=args.extension_path,
            db_configs={benchmark_name: db_configs_dict[benchmark_name]},
        )
        print(f"[child:{benchmark_name}] DB on {device_path} "
              f"(ns_id={ns_id}, mem={mem_mb}MB, temp={temp_gb}GB)", flush=True)
        db = database.Database(args.threads, mem_mb, temp_gb, config)
        cursor = db.attach(benchmark_name)

    task_filename = f"{base_name}_{benchmark_name}_task{task_idx}.csv"
    task_filepath = os.path.join(target_dir, task_filename)
    out_f = open(task_filepath, "w", newline="\n")
    out_f.write(HEADERS.get(benchmark_name, "name;metrics\n"))
    out_f.flush()

    dummy_coord = None
    if pause_req and paused_ack and release_ev:
        class SubprocessWorkerHandle:
            def __init__(self, name="", clock=None):
                self.name = name
                self.clock = clock
                self._pause_requested = pause_req
                self._paused = paused_ack
                self._release = release_ev

            def checkpoint_if_requested(self, label="") -> bool:
                if not self._pause_requested.is_set():
                    return False
                if self.clock is not None:
                    self.clock.pause()
                self._paused.set()
                self._release.wait()
                if self.clock is not None:
                    self.clock.resume()
                return True
        import runner.factory as factory_module
        factory_module.WorkerHandle = SubprocessWorkerHandle
        dummy_coord = _PassthroughCoordinator()

    try:
        run_with_duration = args.duration > 0
        run_bench, setup_bench = create_benchmark_runner(
            benchmark_name, run_with_duration, args.checkpoint_mode,
            tpch_sf=args.tpch_sf, ycsb_sf=args.ycsb_sf,
            coordinator=dummy_coord,
            output_handle={benchmark_name: out_f},
            wal_skip_threshold_bytes=args.wal_skip_threshold_bytes,
        )

        print(f"[child:{benchmark_name}] setup_bench...", flush=True)
        setup_bench([cursor], args.input_dir)

        if start_barrier is not None:
            print(f"[child:{benchmark_name}] Waiting at barrier to sync start...", flush=True)
            start_barrier.wait()

        print(f"[child:{benchmark_name}] run_bench (duration_or_reps={duration_or_reps})...",
              flush=True)
        run_bench([cursor], duration_or_reps)

        print(f"[child:{benchmark_name}] complete.", flush=True)
    finally:
        try: out_f.close()
        except: pass
        try: db.close()
        except: pass

def run_concurrent_benchmark(tasks: list, span: int):
    def _run(runner, cursor_chunk, span):
        return runner(cursor_chunk, span)

    with multiprocessing.pool.ThreadPool(processes=len(tasks)) as pool:
        results = pool.starmap(
            _run, [(runner, chunk, span) for runner, chunk in tasks], chunksize=1
        )
    merged = {}
    for r in results:
        for k, rows in r.items():
            merged.setdefault(k, []).extend(rows)
    return merged


RUN_MEASUREMENT = True


def start_device_measurements(device: NvmeDevice, file_name: str, enable_fdp: bool):
    global RUN_MEASUREMENT
    RUN_MEASUREMENT = True

    os.system("sync")
    start_host, start_media = device.get_written_bytes()
    start_fdp_host, start_fdp_media = device.get_written_bytes_fdp() if enable_fdp else (0, 0)

    with open(file_name, "w", newline="\n") as waf_file:
        waf_file.write("timestamp;source;host_written,media_written;waf;cumulative_waf\n")
        waf_file.write(f"{datetime.now()};smart-log;{start_host},{start_media};0.0000;0.0000\n")
        if enable_fdp:
            waf_file.write(f"{datetime.now()};fdp-stats;{start_fdp_host},{start_fdp_media};0.0000;0.0000\n")

    def run():
        prev_h, prev_m = start_host, start_media
        prev_fh, prev_fm = start_fdp_host, start_fdp_media
        while RUN_MEASUREMENT:
            for _ in range(660):
                if not RUN_MEASUREMENT: return
                time.sleep(1)
            os.system("sync")
            ts = datetime.now()
            cur_h, cur_m = device.get_written_bytes()
            interval_waf = calculate_waf(cur_h - prev_h, cur_m - prev_m)
            cum_waf = calculate_waf(cur_h - start_host, cur_m - start_media)
            with open(file_name, "a", newline="\n") as f:
                f.write(f"{ts};smart-log;{cur_h - prev_h},{cur_m - prev_m};{interval_waf:.4f};{cum_waf:.4f}\n")
            prev_h, prev_m = cur_h, cur_m

            if enable_fdp:
                cur_fh, cur_fm = device.get_written_bytes_fdp()
                interval_fwaf = calculate_waf(cur_fh - prev_fh, cur_fm - prev_fm)
                cum_fwaf = calculate_waf(cur_fh - start_fdp_host, cur_fm - start_fdp_media)
                with open(file_name, "a", newline="\n") as f:
                    f.write(f"{ts};fdp-stats;{cur_fh - prev_fh},{cur_fm - prev_fm};{interval_fwaf:.4f};{cum_fwaf:.4f}\n")
                prev_fh, prev_fm = cur_fh, cur_fm

    thread = Thread(target=run, daemon=True)
    thread.start()

    def stop():
        global RUN_MEASUREMENT
        RUN_MEASUREMENT = False
        thread.join()
        os.system("sync")
        ts = datetime.now()
        end_h, end_m = device.get_written_bytes()
        waf = calculate_waf(end_h - start_host, end_m - start_media)
        with open(file_name, "a", newline="\n") as f:
            f.write(f"{ts};smart-log;{end_h - start_host},{end_m - start_media};{waf:.4f};{waf:.4f}\n")
        if enable_fdp:
            end_fh, end_fm = device.get_written_bytes_fdp()
            fwaf = calculate_waf(end_fh - start_fdp_host, end_fm - start_fdp_media)
            with open(file_name, "a", newline="\n") as f:
                f.write(f"{ts};fdp-stats;{end_fh - start_fdp_host},{end_fm - start_fdp_media};{fwaf:.4f};{fwaf:.4f}\n")

    return stop


def _scale_factor_name(args):
    if args.benchmark == "tpch":
        return f"sf{args.tpch_sf}"
    if args.benchmark == "ycsb":
        return f"sf{args.ycsb_sf}"
    if "tpch" in args.benchmark and "ycsb" in args.benchmark:
        return f"tsf{args.tpch_sf}-ysf{args.ycsb_sf}"
    if args.benchmark == "htap":
        return f"tsf{args.tpch_sf}-ysf{args.ycsb_sf}"
    return f"sf{args.tpch_sf}"


HEADERS = {
    "tpch": "query_name;latency_ms;nvmefs_metrics\n",
    "ycsb": "workload_name;offset_s;interval_ms;iterations;throughput_ops;nvmefs_metrics\n",
    "oocha": "grouping;wide;latency_ms\n",
    "oocha-spill": "latency_ms\n",
}


def generate_filenames(args: Arguments):
    run_with_duration = args.duration > 0
    duration_display = f"dur{args.duration}" if run_with_duration else f"reps{args.repetitions}"
    parallel = f"p{args.parallel}" if args.parallel > 0 else "s"
    fdp_name = "fdp" if args.use_fdp else "nofdp"
    device_name = "nvme" if not args.should_mount else "normal"
    scale_factor = _scale_factor_name(args)

    base_name = f"{args.benchmark}-{duration_display}-{device_name}-mem{args.buffer_manager_mem_size}-{args.io_backend}-{scale_factor}-t{args.threads}-{parallel}-{fdp_name}"

    run_id = (getattr(args, "run_id", None)
              or os.environ.get("SUITE_TIMESTAMP")
              or datetime.now().strftime("%Y%m%d_%H%M%S"))

    target_dir = os.path.join("results", run_id)
    os.makedirs(target_dir, exist_ok=True)
    device_output_file = os.path.join(target_dir, f"{base_name}-device.csv")
    return target_dir, base_name, device_output_file


def run_fragmentation_loop(stop_event: Event, target_dir: str, output_log: str, script: str):
    base, ext = os.path.splitext(output_log)
    while not stop_event.is_set():
        now = datetime.now()
        timestamped_log = f"{base}_{now.strftime('%Y%m%d_%H%M%S')}{ext}"
        try:
            with open(timestamped_log, "a") as f:
                f.write(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.flush()
                subprocess.run(
                    ["ionice", "-c", "3", "nice", "-n", "19", "/bin/bash", script, target_dir],
                    stdout=f, stderr=f)
        except Exception as e:
            print(f"Failed to run fragmentation script: {e}")
        if stop_event.wait(timeout=900):
            break


def start_fragmentation_threads(args, target_dir, created_task_files):
    if not args.should_mount:
        return None, []

    mount_paths = []
    if getattr(args, "mount_paths", None):
        mount_paths = list(args.mount_paths)
    elif getattr(args, "mount_path", None):
        mount_paths = [args.mount_path]

    if not mount_paths:
        return None, []

    script = getattr(args, "frag_script_path", None)
    if not script or not os.path.exists(script):
        return None, []

    frag_dir = os.path.join(target_dir, "fragmentation")
    os.makedirs(frag_dir, exist_ok=True)

    stop_event = Event()
    threads = []
    for mp in mount_paths:
        tag = mp.rstrip("/").split("/")[-1] if len(mount_paths) > 1 else "fragmentation"
        frag_log_file = os.path.join(frag_dir, f"{tag}_over_time.log")
        t = Thread(
            target=run_fragmentation_loop,
            args=(stop_event, mp, frag_log_file, script),
            daemon=True,
        )
        t.start()
        threads.append(t)
        created_task_files.append(frag_log_file)
        print(f"[fragmentation] watching mount {mp} -> {frag_log_file}")
    return stop_event, threads


if __name__ == "__main__":
    args: Arguments = Arguments.parse_args()

    if ',' in args.benchmark:
        benchmarks = args.benchmark.split(',')
        num_parallel = len(benchmarks)
    else:
        num_parallel = args.parallel if args.parallel > 0 else 1
        benchmarks = [args.benchmark] * num_parallel

    if num_parallel > 1 and args.threads > 1:
        args.threads = max(1, args.threads // num_parallel)
        print(f"Partitioning CPU: {args.threads} threads per instance.")

    multi_workload = args.is_multi_workload()

    benchmark_ns_counts = []
    namespace_identities = []
    for b in benchmarks:
        if multi_workload:
            ns_count = 1
        else:
            ns_count = get_namespace_count(b)
        benchmark_ns_counts.append(ns_count)
        for local_idx in range(ns_count):
            namespace_identities.append((b, local_idx))

    initial_device = NvmeDevice(args.device) if args.device else None
    if not args.skip_reset and initial_device:
        print("Resetting device.")
        initial_device.reset()
        initial_device.number_of_blocks, initial_device.unallocated_number_of_blocks = \
            initial_device._NvmeDevice__get_device_info()

    if args.use_fdp:
        initial_device.enable_fdp()

    if (not multi_workload) and args.filler and not args.skip_reset and initial_device is not None:
        filler_size = initial_device.unallocated_number_of_blocks - args.namespace_size
        if filler_size <= 0:
            raise ValueError(
                f"Workload namespace ({args.namespace_size} blocks) exceeds device capacity."
            )
        filler_ns = initial_device.create_filler_namespace(
                                        namespace_id=1, size_blocks=filler_size,
                                        enable_fdp=args.use_fdp, phndls="0",)
        fill_namespace_with_data(filler_ns, passes=2)

    if multi_workload:
        unallocated = (initial_device.unallocated_number_of_blocks
                       if initial_device is not None else 0)
        device, namespace_infos = setup_namespaces_for_multi(
            args, benchmarks, unallocated_blocks=unallocated
        )

        target_dir, base_name, device_output_file = generate_filenames(args)
        run_with_duration = args.duration > 0
        duration_or_reps = args.duration if run_with_duration else args.repetitions

        mp_ctx = multiprocessing.get_context("spawn")
        start_barrier = mp_ctx.Barrier(len(namespace_infos))
        parent_workers = []
        process_events = []

        for info in namespace_infos:
            if args.drain:
                pause_req = mp_ctx.Event()
                paused_ack = mp_ctx.Event()
                release_ev = mp_ctx.Event()
                
                parent_handle = IPCWorkerHandleParent(info["benchmark"], pause_req, paused_ack, release_ev)
                parent_workers.append(parent_handle)
                process_events.append((pause_req, paused_ack, release_ev))
            else:
                process_events.append((None, None, None))

        if args.drain:
            coordinator = WAFCheckpoint(
                device, device_output_file, enable_fdp=args.use_fdp,
                drain_interval_s=args.drain_interval, drain_duration_s=args.drain_duration,
                drain_final_duration_s=args.drain_final_duration,
                drain_poll_interval_s=args.drain_poll_interval,
            )
            for w in parent_workers:
                coordinator.add_worker(w)
            coordinator.start()
            stop_measurement = coordinator.stop
        else:
            stop_measurement = start_device_measurements(
                device, device_output_file, enable_fdp=args.use_fdp
            )

        created_task_files = []
        frag_stop_event, frag_threads = start_fragmentation_threads(
            args, target_dir, created_task_files
        )

        processes = []
        for task_idx, (info, events) in enumerate(zip(namespace_infos, process_events)):
            pause_req, paused_ack, release_ev = events
            p = mp_ctx.Process(
                target=_run_workload_in_subprocess,
                args=(info["benchmark"], info["ns_id"], args, duration_or_reps,
                      target_dir, base_name, task_idx, info["mount_path"],
                      start_barrier, pause_req, paused_ack, release_ev),
                name=f"workload-{info['benchmark']}",
            )
            p.start()
            processes.append(p)
            print(f"[parent] Spawned '{info['benchmark']}' (pid={p.pid}, ns_id={info['ns_id']})")

        exit_codes = {}
        for p in processes:
            p.join()
            exit_codes[p.name] = p.exitcode
            print(f"[parent] '{p.name}' exited (code={p.exitcode})")

        stop_measurement()

        if frag_stop_event is not None:
            frag_stop_event.set()
            for t in frag_threads:
                if t.is_alive():
                    t.join(timeout=5)

        task_files = [
            os.path.join(target_dir, f"{base_name}_{info['benchmark']}_task{i}.csv")
            for i, info in enumerate(namespace_infos)
        ]

        any_failed = any(code != 0 for code in exit_codes.values())
        status = "with failures" if any_failed else "successfully"
        print(f"\n--- Run Complete {status}: {target_dir} ---")
        print(f"Global WAF File: {os.path.basename(device_output_file)}")
        print(f"Task Metric Files: {[os.path.basename(f) for f in (task_files + created_task_files)]}")
        if any_failed:
            print(f"Subprocess exit codes: {exit_codes}")

    else:
        setup_device_and_db = prepare_setup_func(args, namespace_identities)

        target_dir, base_name, device_output_file = generate_filenames(args)
        run_with_duration = args.duration > 0
        db, cursors, device = setup_device_and_db()

        tasks = []
        all_open_files = []
        created_task_files = []

        frag_stop_event, frag_threads = start_fragmentation_threads(args, target_dir, created_task_files)

        cursor_chunks = []
        current = 0
        for count in benchmark_ns_counts:
            cursor_chunks.append(cursors[current : current + count])
            current += count

        coordinator = None
        if args.drain:
            coordinator = WAFCheckpoint(
                device, device_output_file, enable_fdp=args.use_fdp,
                drain_interval_s=args.drain_interval, drain_duration_s=args.drain_duration,
                drain_final_duration_s=args.drain_final_duration,
                drain_poll_interval_s=args.drain_poll_interval,
            )
            coordinator.start()
            stop_measurement = coordinator.stop
        else:
            stop_measurement = start_device_measurements(device, device_output_file, enable_fdp=args.use_fdp)

        for task_idx, (b_name, cursor_chunk) in enumerate(zip(benchmarks, cursor_chunks)):
            task_filename = f"{base_name}_{b_name}_task{task_idx}.csv"
            task_filepath = os.path.join(target_dir, task_filename)
            created_task_files.append(task_filepath)
            f = open(task_filepath, "w", newline="\n")
            f.write(HEADERS.get(b_name, "name;metrics\n"))
            f.flush()
            all_open_files.append(f)
            task_output_handle = {b_name: f}

            run_bench, setup_bench = create_benchmark_runner(
                b_name, run_with_duration, args.checkpoint_mode,
                tpch_sf=args.tpch_sf, ycsb_sf=args.ycsb_sf,
                coordinator=coordinator, output_handle=task_output_handle,
                wal_skip_threshold_bytes=args.wal_skip_threshold_bytes,
            )
            print(f"Setting up {b_name} task {task_idx} on cursors {[c.db_name for c in cursor_chunk]}...")
            setup_bench(cursor_chunk, args.input_dir)
            tasks.append((run_bench, cursor_chunk))

        if args.parallel > 0:
            print(f"Running {len(tasks)} workloads CONCURRENTLY...")
            metric_results = run_concurrent_benchmark(tasks, args.duration if run_with_duration else args.repetitions)
        else:
            print(f"Running {len(tasks)} workloads SEQUENTIALLY...")
            metric_results = {}
            for runner_func, cursor_chunk in tasks:
                res = runner_func(cursor_chunk, args.duration if run_with_duration else args.repetitions)
                for key, rows in res.items():
                    metric_results.setdefault(key, []).extend(rows)

        stop_measurement()

        if frag_stop_event is not None:
            frag_stop_event.set()
            for t in frag_threads:
                if t.is_alive():
                    t.join(timeout=5)

        for f in all_open_files:
            try: f.close()
            except: pass

        try:
            if isinstance(db, list):
                for d in db:
                    try: d.close()
                    except: pass
            else:
                db.close()
        except: pass

        print(f"\n--- Run Complete: {target_dir} ---")
        print(f"Global WAF File: {os.path.basename(device_output_file)}")
        print(f"Task Metric Files: {[os.path.basename(f) for f in created_task_files]}")
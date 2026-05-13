from dataclasses import dataclass
import argparse

@dataclass
class Arguments:
    duration: int = 0
    parallel: int = 0
    threads: int = 1
    repetitions: int = 0
    ycsb_sf: int = 1
    tpch_sf: int = 1
    buffer_manager_mem_size: str = "50"
    device: str = ""
    io_backend: str = ""
    use_generic_device: bool = False
    benchmark: str = ""
    should_mount: bool = False
    input_dir: str = "./"
    sensor_batch_size: int = 100
    namespace_id: int = 1
    namespace_size: int = 0
    precondition: bool = False
    checkpoint_mode: str = "auto"
    skip_reset: bool = False
    run_id: str = ""
    extension_path: str = ""
    drain: bool = False
    drain_interval: int = 660
    drain_duration: int = 660
    drain_final_duration: int = 1800
    drain_poll_interval: int = 30
    fio_file: str = None
    settle_seconds: int = 900
    dsm_after_preconditioning: bool = False
    filler: bool = False
    use_fdp: bool = False
    fdp_mapping: str = ""
    db_configs: str = ""
    temp_size: int = 200

    def get_memory_limit(self) -> int:
        """Single shared memory budget for the whole DuckDB instance, in MB."""
        try:
            return int(self.buffer_manager_mem_size)
        except ValueError:
            limits = {}
            for pair in self.buffer_manager_mem_size.split(','):
                k, v = pair.split('=')
                limits[k.strip()] = int(v.strip())
            return max(limits.values()) if limits else 50

    def parse_db_configs(self) -> dict[str, int]:
        if not self.db_configs:
            return {}
        result = {}
        for pair in self.db_configs.split(','):
            name, size = pair.split(':')
            size = size.strip().upper().rstrip('GB')
            result[name.strip()] = int(size)
        return result

    def valid(self) -> bool:
        if self.use_fdp and not self.device:
            print("--fdp requires --device_path")
            return False
        if self.use_fdp and not self.fdp_mapping:
            print("--fdp requires --fdp_mapping")
            return False
        if (self.repetitions == 0 and self.duration == 0) or \
        (self.repetitions != 0 and self.duration != 0):
            print("Either duration or repetitions must be set (but not both)")
            return False
        return True

    @staticmethod
    def parse_args():
        parser = argparse.ArgumentParser(description="DuckDB Benchmark Runner")

        parser.add_argument("benchmark", type=str, default="tpch",
                            help="Name of the benchmark to run (tpch, sensor, oocha)")

        parser.add_argument("--tpch_sf", type=int, default=1,
                            help="TPC-H Scaling Factor")
        
        parser.add_argument("--ycsb_sf", type=int, default=100,
                            help="YCSB Scaling Factor")

        parser.add_argument("-d", "--duration", type=int, default=0,
                            help="Duration in minutes")

        parser.add_argument("-r", "--repetitions", type=int, default=0,
                            help="Number of repetitions")

        parser.add_argument("-m", "--memory_limit", type=str, default="50",
                            help="Memory limit in MB (e.g. '50000' or 'tpch=45000,ycsb=2000')")

        parser.add_argument("-p", "--device_path", type=str, default=None,
                            help="Path to NVMe device (e.g., /dev/nvme1n1)")

        parser.add_argument("-g", "--generic_device", action="store_true", default=False,
                            help="Use generic device path")

        parser.add_argument("-b", "--backend", type=str, default="io_uring_cmd",
                            help="IO Backend ('io_uring_cmd', 'io_uring')")

        parser.add_argument("-fs", "--fdp_strategy", default=None,
                            choices=["baseline", "temp-isolated", "wal-isolated", "fully-isolated"],
                            help="FDP placement strategy to use")

        parser.add_argument("--mount", action="store_true", default=False,
                            help="Whether the block device should be mounted using udisks2")

        parser.add_argument("-i", "--input_directory", type=str, default="./",
                            help="Input directory for data files")

        parser.add_argument("-t", "--threads", type=int, default=1,
                            help="Number of DuckDB threads")

        parser.add_argument("-par", "--parallel", type=int, default=0,
                            help="Number of parallel execution threads (clients)")
                            
        parser.add_argument("--sensor_batch_size", type=int, default=100,
                            help="Base batch size for sensor benchmark inserts")

        parser.add_argument("--namespace_id", "-ns", type=int, default=1,
                            help="Namespace id for NVMe device")

        parser.add_argument("--namespace_size", type=int, default=100,
                            help="Namspace size in blocks for NVMe device")

        parser.add_argument("--precondition", action="store_true", default=False,
                            help="Execute sequential fill to precondition the SSD before benchmarking")
        
        parser.add_argument("--checkpoint_mode", type=str, default="auto", choices=["auto", "manual"],
                    help="Determines how WAL checkpointing is handled in YCSB")

        parser.add_argument("--skip_reset", action="store_true", default=False,
                    help="Skip device reset and use existing preconditioned namespace")
        
        parser.add_argument("--run_id", type=str, default=None,
                    help="Suite run identifier; results go to results/<benchmark>/<run_id>/")

        parser.add_argument("--extension_path", type=str, default="", 
                help="Path to the compiled duckdb extension")
        
        parser.add_argument("--drain", action="store_true", default=False,
                help="Pause workloads at safe points to take quiesced WAF readings "
                "(works around media-written counters that update in lazy bursts).")
        
        parser.add_argument("--drain-interval", type=int, default=660,
                help="Seconds between drain checkpoints (default 660).")
        
        parser.add_argument("--drain-duration", type=int, default=660,
                help="Seconds to wait at each drain for media-written to settle (default 660).")

        parser.add_argument("--drain-final-duration", type=int, default=1800,
                help="Seconds to wait at end-of-run before taking the final drained reading. "
                "Should be >= --drain-duration; longer captures post-stop GC.")
        
        parser.add_argument("--drain-poll-interval", type=int, default=30,
                help="Seconds between media-counter polls during mid-run drains (default 30).")

        parser.add_argument("--fio_file", type=str, default=None,
                    help="Path to fio job file for preconditioning (e.g. fio/uniform.fio)")
        
        parser.add_argument("--settle_seconds", type=int, default=900,
                    help="Seconds to wait after preconditioning for FTL to settle")
        
        parser.add_argument("--dsm_after_preconditioning", action="store_true",
                    help="Skip DSM after preconditioning")

        parser.add_argument("--filler", action="store_true", default=False,
                    help="Create a filler namespace with valid data to occupy device capacity outside the workload region")
        
        parser.add_argument("--max_temp_size", type=int, default="200", 
                    help="DuckDB max_temp_directory_size limit")

        parser.add_argument("--fdp_mapping", type=str, default="",
                    help="Raw FDP mapping string, e.g. "
                         "'tpch.db:1,tpch.wal:2,ycsb.db:3,ycsb.wal:4,.tmp:5'. "
                         "Empty when --fdp is off.")
        
        parser.add_argument("--db_configs", type=str, default="",
                    help="Per-database sizes, e.g. 'tpch:800GB,ycsb:400GB'.")

        parser.add_argument("-f", "--fdp", action="store_true", default=False,
                            help="Enable Flexible Data Placement (FDP)")

        args = parser.parse_args()
        
        arguments = Arguments(
            duration=args.duration,
            repetitions=args.repetitions,
            device=args.device_path,
            ycsb_sf=args.ycsb_sf,
            tpch_sf=args.tpch_sf,
            buffer_manager_mem_size=args.memory_limit,
            io_backend=args.backend,
            use_fdp=args.fdp,
            use_generic_device=args.generic_device,
            benchmark=args.benchmark,
            should_mount=args.mount,
            input_dir=args.input_directory,
            threads=args.threads,
            parallel=args.parallel, 
            sensor_batch_size=args.sensor_batch_size,
            namespace_id=args.namespace_id,
            namespace_size=args.namespace_size,
            precondition=args.precondition,
            checkpoint_mode=args.checkpoint_mode,
            skip_reset=args.skip_reset,
            run_id=args.run_id,
            extension_path=args.extension_path,
            drain=args.drain,
            drain_interval=args.drain_interval,
            drain_duration=args.drain_duration,
            drain_final_duration=args.drain_final_duration,
            drain_poll_interval=args.drain_poll_interval,
            fio_file=args.fio_file,
            settle_seconds=args.settle_seconds,
            dsm_after_preconditioning=args.dsm_after_preconditioning,
            filler=args.filler,
            temp_size=args.max_temp_size,
            fdp_mapping=args.fdp_mapping,
            db_configs=args.db_configs,
        )

        if not arguments.valid():
            parser.print_help()
            exit(1)
        
        return arguments

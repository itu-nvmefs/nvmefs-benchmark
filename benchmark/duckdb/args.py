from dataclasses import dataclass
import argparse

@dataclass
class Arguments:
    duration: int = 0
    parallel: int = 0
    threads: int = 1
    repetitions: int = 0
    scale_factor: int = 1
    buffer_manager_mem_size: int = 50
    device: str = ""
    io_backend: str = ""
    use_fdp: bool = False
    fdp_strategy: str = ""
    use_generic_device: bool = False
    benchmark: str = ""
    mount_path: str = None
    input_dir: str = "./"
    sensor_batch_size: int = 100
    namespace_id: int = 1
    namespace_size: int = 0
    precondition: bool = False
    

    def valid(self) -> bool:
        if self.use_fdp and self.device is None:
            print("Device path is required for FDP")
            return False

        if (self.repetitions == 0 and self.duration == 0) or (self.repetitions != 0 and self.duration != 0):
            print("Either duration or repetitions must be set (but not both)")
            return False
        
        return True

    @staticmethod
    def parse_args():
        parser = argparse.ArgumentParser(description="DuckDB Benchmark Runner")

        parser.add_argument("benchmark", type=str, default="tpch",
                            help="Name of the benchmark to run (tpch, sensor, oocha)")

        parser.add_argument("-s", "--sf", type=int, default=1,
                            help="Scale factor (or batch multiplier for sensor)")

        parser.add_argument("-d", "--duration", type=int, default=0,
                            help="Duration in minutes")

        parser.add_argument("-r", "--repetitions", type=int, default=0,
                            help="Number of repetitions")

        parser.add_argument("-m", "--memory_limit", type=int, default=50,
                            help="Buffer manager memory limit in MB")

        parser.add_argument("-p", "--device_path", type=str, default=None,
                            help="Path to NVMe device (e.g., /dev/nvme1n1)")

        parser.add_argument("-g", "--generic_device", action="store_true", default=False,
                            help="Use generic device path")

        parser.add_argument("-b", "--backend", type=str, default="io_uring_cmd",
                            help="IO Backend ('io_uring_cmd', 'io_uring')")

        parser.add_argument("-f", "--fdp", action="store_true", default=False,
                            help="Enable File Descriptor Passing (FDP)")

        parser.add_argument("-fs", "--fdp_strategy", default=None,
                            choices=["baseline", "temp-isolated", "wal-isolated", "fully-isolated"],
                            help="FDP placement strategy to use")

        parser.add_argument("-mp", "--mount_path", type=str, default=None,
                            help="Mount path for normal file system tests")

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

        args = parser.parse_args()
        
        arguments = Arguments(
            duration=args.duration,
            repetitions=args.repetitions,
            device=args.device_path,
            scale_factor=args.sf,
            buffer_manager_mem_size=args.memory_limit,
            io_backend=args.backend,
            use_fdp=args.fdp,
            fdp_strategy=args.fdp_strategy,
            use_generic_device=args.generic_device,
            benchmark=args.benchmark,
            mount_path=args.mount_path,
            input_dir=args.input_directory,
            threads=args.threads,
            parallel=args.parallel,
            sensor_batch_size=args.sensor_batch_size,
            namespace_id=args.namespace_id,
            namespace_size=args.namespace_size,
            precondition=args.precondition
        )

        if not arguments.valid():
            parser.print_help()
            exit(1)
        
        return arguments
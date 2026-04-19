import time

from . import benchmark_types, tpch, sensor, ycsb
from .oocha import oocha


def create_benchmark_runner(name: str, scale_factor: int, run_with_duration: bool) -> tuple[benchmark_types.BenchmarkRunnerFunc, benchmark_types.BenchmarkSetupFunc]:

    def create_runner_function(benchmark: benchmark_types.BenchmarkEpochFunc) -> benchmark_types.BenchmarkRunnerFunc:
        """
        Create a benchmark runner function that runs the benchmark for a specified duration.
        :param benchmark: The benchmark function to run.
        :return: A function that runs the benchmark for a specified duration.
        """
        def duration_wrapper(db: benchmark_types.Database, duration_minutes: int) -> list[str]:

            def get_time():
                # Return the current time in minutes
                return time.monotonic() / 60

            start_time = get_time()
            delta = 0
            consolidated_results: list[str] = []

            print(f"Running benchmark '{name}' for {duration_minutes} minutes...")

            while delta < duration_minutes:
                # Run the benchmark
                results = benchmark(db, scale_factor)
                consolidated_results.extend(results)
                delta = get_time() - start_time

            return consolidated_results

        def repetition_wrapper(db: benchmark_types.Database, repetitions: int) -> list[str]:
            # Run the benchmark for a specified number of repetitions
            consolidated_results: list[str] = []

            print(f"Running benchmark '{name}' for {repetitions} repetitions...")

            for _ in range(repetitions):
                # Run the benchmark
                results = benchmark(db, scale_factor)

                consolidated_results.extend(results)

            return consolidated_results
    
        return duration_wrapper if run_with_duration else repetition_wrapper

    if name == tpch.TPCH_BENCHMARK_NAME:
        return create_runner_function(tpch.run_tpch_epoch_benchmark), tpch.setup_tpch_benchmark 
    elif name == ycsb.YCSB_BENCHMARK_NAME:
        return create_runner_function(ycsb.run_ycsb_epoch_benchmark), ycsb.setup_ycsb_benchmark
    elif name == sensor.SENSOR_BENCHMARK_NAME:
        return create_runner_function(sensor.run_sensor_epoch_benchmark), sensor.setup_sensor_benchmark
    elif name == oocha.OOCHA_SPILL_BENCHMARK_NAME:
        return create_runner_function(oocha.run_oocha_spill_epoch_benchmark), oocha.setup_oocha_spill_benchmark
    elif name == oocha.OOCHA_BENCHMARK_NAME:
        return create_runner_function(oocha.run_oocha_epoch_benchmark), oocha.setup_oocha_benchmark

    raise ValueError(f"Unknown benchmark '{name}'")

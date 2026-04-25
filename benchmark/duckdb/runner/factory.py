import time

from . import benchmark_types, tpch, ycsb, htap
from .oocha import oocha

NAMESPACE_COUNTS = {
    tpch.TPCH_BENCHMARK_NAME: 1,
    ycsb.YCSB_BENCHMARK_NAME: 1,
    htap.HTAP_BENCHMARK_NAME: htap.HTAP_NAMESPACE_COUNT
}

def get_namespace_count(name: str) -> int:
    return NAMESPACE_COUNTS.get(name, 1)

def create_benchmark_runner(name: str, scale_factor: int, run_with_duration: bool, checkpoint_mode: str = "auto") -> tuple[benchmark_types.BenchmarkRunnerFunc, benchmark_types.BenchmarkSetupFunc]:
    def create_managed_runner(benchmark):
        # Managed runner for Python benchmarks
        def duration_wrapper(dbs, duration_minutes: int):
            def get_time():
                # Current time in minutes
                return time.monotonic() / 60

            start_time = get_time()
            results = []
            print(f"Running '{name}' for {duration_minutes} minutes...")
            while (get_time() - start_time < duration_minutes):
                results.extend(benchmark(dbs, scale_factor))
            return results
            
        def repetition_wrapper(dbs, repetitions: int):
            results = []
            print(f"Running '{name}' for {repetitions} reps...")
            for _ in range(repetitions):
                results.extend(benchmark(dbs, scale_factor))
            return results

        return duration_wrapper if run_with_duration else repetition_wrapper

    def create_native_runner(benchmark):
        # Custom runner for C++ benchmark
        def native_wrapper(dbs, duration_or_reps: int):
            duration = (duration_or_reps * 60) if run_with_duration else 0
            reps = 0 if run_with_duration else duration_or_reps 
            print(f"Running '{name}' natively in C++...")
            return benchmark(dbs, scale_factor, duration, reps, checkpoint_mode)
        return native_wrapper


    if name == tpch.TPCH_BENCHMARK_NAME:
        return create_managed_runner(tpch.run_tpch_epoch_benchmark), tpch.setup_tpch_benchmark
    elif name == ycsb.YCSB_BENCHMARK_NAME:
        ycsb_setup = lambda db, input_dir, sf: ycsb.setup_ycsb_benchmark(db, input_dir, sf, checkpoint_mode)
        return create_native_runner(ycsb.run_ycsb_epoch_benchmark), ycsb_setup
    elif name == htap.HTAP_BENCHMARK_NAME:
        htap_setup = lambda db, input_dir, sf: htap.setup_htap_benchmark(db, input_dir, sf, checkpoint_mode)
        return create_managed_runner(htap.run_htap_epoch_benchmark), htap_setup
    elif name == oocha.OOCHA_SPILL_BENCHMARK_NAME:
        return create_managed_runner(oocha.run_oocha_spill_epoch_benchmark), oocha.setup_oocha_spill_benchmark
    elif name == oocha.OOCHA_BENCHMARK_NAME:
        return create_managed_runner(oocha.run_oocha_epoch_benchmark), oocha.setup_oocha_benchmark

    raise ValueError(f"Unknown benchmark '{name}'")

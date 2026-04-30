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

def create_benchmark_runner(name: str, run_with_duration: bool, checkpoint_mode: str = "auto", tpch_sf: int = 100, ycsb_sf: int = 100) -> tuple[benchmark_types.BenchmarkRunnerFunc, benchmark_types.BenchmarkSetupFunc]:
    def create_managed_runner(benchmark, sf):
        def duration_wrapper(dbs, duration_minutes: int):
            start = time.monotonic()
            merged = {}
            print(f"Running '{name}' for {duration_minutes} minutes...")
            while (time.monotonic() - start) / 60 < duration_minutes:
                for key, rows in benchmark(dbs, sf).items():
                    merged.setdefault(key, []).extend(rows)
            return merged

        def repetition_wrapper(dbs, repetitions: int):
            merged = {}
            print(f"Running '{name}' for {repetitions} reps...")
            for _ in range(repetitions):
                for key, rows in benchmark(dbs, sf).items():
                    merged.setdefault(key, []).extend(rows)
            return merged

        return duration_wrapper if run_with_duration else repetition_wrapper

    def create_native_runner(benchmark, sf):
        # Custom runner for C++ benchmark
        def native_wrapper(dbs, duration_or_reps: int):
            duration = (duration_or_reps * 60) if run_with_duration else 0
            reps = 0 if run_with_duration else duration_or_reps 
            print(f"Running '{name}' natively in C++...")
            return benchmark(dbs, sf, duration, reps, checkpoint_mode, interval_seconds=660) # 11 minutes to match WAF
        return native_wrapper

    if name == tpch.TPCH_BENCHMARK_NAME:
        tpch_setup = lambda dbs, input_dir: tpch.setup_tpch_benchmark(dbs, input_dir, tpch_sf)
        return create_managed_runner(tpch.run_tpch_epoch_benchmark, tpch_sf), tpch_setup

    elif name == ycsb.YCSB_BENCHMARK_NAME:
        ycsb_setup = lambda dbs, input_dir: ycsb.setup_ycsb_benchmark(
            dbs, input_dir, ycsb_sf, checkpoint_mode)
        return create_native_runner(ycsb.run_ycsb_epoch_benchmark, ycsb_sf), ycsb_setup

    elif name == htap.HTAP_BENCHMARK_NAME:
        htap_setup = lambda dbs, input_dir: htap.setup_htap_benchmark(
            dbs, input_dir, tpch_sf, ycsb_sf, checkpoint_mode)

        def htap_run(dbs, duration_or_reps: int):
            duration = (duration_or_reps * 60) if run_with_duration else 0
            reps = 0 if run_with_duration else duration_or_reps
            print(f"Running '{name}'...")
            return htap.run_htap_epoch_benchmark(
                dbs, tpch_sf, ycsb_sf, duration, reps, checkpoint_mode, interval_seconds=660)

        return htap_run, htap_setup

    elif name == oocha.OOCHA_SPILL_BENCHMARK_NAME:
        oocha_spill_setup = lambda dbs, input_dir: oocha.setup_oocha_spill_benchmark(dbs, input_dir, tpch_sf)
        return create_managed_runner(oocha.run_oocha_spill_epoch_benchmark, tpch_sf), oocha_spill_setup

    elif name == oocha.OOCHA_BENCHMARK_NAME:
        oocha_setup = lambda dbs, input_dir: oocha.setup_oocha_benchmark(dbs, input_dir, tpch_sf)
        return create_managed_runner(oocha.run_oocha_epoch_benchmark, tpch_sf), oocha_setup

    raise ValueError(f"Unknown benchmark '{name}'")

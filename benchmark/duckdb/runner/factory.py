import time

from . import benchmark_types, tpch, ycsb, htap
from .coordinator import WorkerHandle, ActiveClock
from .oocha import oocha

NAMESPACE_COUNTS = {
    tpch.TPCH_BENCHMARK_NAME: 1,
    ycsb.YCSB_BENCHMARK_NAME: 1,
    htap.HTAP_BENCHMARK_NAME: htap.HTAP_NAMESPACE_COUNT,
}

def get_namespace_count(name):
    return NAMESPACE_COUNTS.get(name, 1)


def create_benchmark_runner(name, run_with_duration, checkpoint_mode="auto",
                            tpch_sf=100, ycsb_sf=100, coordinator=None,
                            output_handle=None):

    def create_managed_runner(benchmark, sf, register_handle=False):
        def duration_wrapper(dbs, duration_minutes):
            handle = None
            clock = None
            if register_handle and coordinator is not None:
                clock = ActiveClock()
                handle = WorkerHandle(name=name, clock=clock)
                coordinator.add_worker(handle)
            try:
                limit_s = duration_minutes * 60
                merged = {}
                out = output_handle.get(name) if output_handle else None
                print(f"Running '{name}' for {duration_minutes} minutes...")
                if clock is not None:
                    while clock.elapsed() < limit_s:
                        kwargs = {"worker_handle": handle, "output_handle": out} if register_handle else {}
                        for key, rows in benchmark(dbs, sf, **kwargs).items():
                            merged.setdefault(key, []).extend(rows)
                else:
                    start = time.monotonic()
                    while (time.monotonic() - start) < limit_s:
                        for key, rows in benchmark(dbs, sf).items():
                            merged.setdefault(key, []).extend(rows)
                return merged
            finally:
                if handle is not None and coordinator is not None:
                    coordinator.remove_worker(handle)

        def repetition_wrapper(dbs, repetitions):
            handle = None
            if register_handle and coordinator is not None:
                handle = WorkerHandle(name=name)
                coordinator.add_worker(handle)
            try:
                merged = {}
                out = output_handle.get(name) if output_handle else None
                print(f"Running '{name}' for {repetitions} reps...")
                for _ in range(repetitions):
                    kwargs = {"worker_handle": handle, "output_handle": out} if register_handle else {}
                    for key, rows in benchmark(dbs, sf, **kwargs).items():
                        merged.setdefault(key, []).extend(rows)
                return merged
            finally:
                if handle is not None and coordinator is not None:
                    coordinator.remove_worker(handle)

        return duration_wrapper if run_with_duration else repetition_wrapper

    def create_native_runner(benchmark, sf):
        def native_wrapper(dbs, duration_or_reps):
            duration = (duration_or_reps * 60) if run_with_duration else 0
            reps = 0 if run_with_duration else duration_or_reps
            out = output_handle.get("ycsb") if output_handle else None
            print(f"Running '{name}' natively in C++...")
            return benchmark(dbs, sf, duration, reps, checkpoint_mode,
                             interval_seconds=660, coordinator=coordinator,
                             output_handle=out)
        return native_wrapper

    if name == tpch.TPCH_BENCHMARK_NAME:
        tpch_setup = lambda dbs, input_dir: tpch.setup_tpch_benchmark(dbs, input_dir, tpch_sf)
        return create_managed_runner(tpch.run_tpch_epoch_benchmark, tpch_sf, register_handle=True), tpch_setup

    if name == ycsb.YCSB_BENCHMARK_NAME:
        ycsb_setup = lambda dbs, input_dir: ycsb.setup_ycsb_benchmark(
            dbs, input_dir, ycsb_sf, checkpoint_mode)
        return create_native_runner(ycsb.run_ycsb_epoch_benchmark, ycsb_sf), ycsb_setup

    if name == htap.HTAP_BENCHMARK_NAME:
        htap_setup = lambda dbs, input_dir: htap.setup_htap_benchmark(
            dbs, input_dir, tpch_sf, ycsb_sf, checkpoint_mode)

        def htap_run(dbs, duration_or_reps):
            duration = (duration_or_reps * 60) if run_with_duration else 0
            reps = 0 if run_with_duration else duration_or_reps
            print(f"Running '{name}'...")
            return htap.run_htap_epoch_benchmark(
                dbs, tpch_sf, ycsb_sf, duration, reps, checkpoint_mode,
                interval_seconds=660, coordinator=coordinator,
                output_handle=output_handle)
        return htap_run, htap_setup

    if name == oocha.OOCHA_SPILL_BENCHMARK_NAME:
        oocha_spill_setup = lambda dbs, input_dir: oocha.setup_oocha_spill_benchmark(dbs, input_dir, tpch_sf)
        return create_managed_runner(oocha.run_oocha_spill_epoch_benchmark, tpch_sf), oocha_spill_setup

    if name == oocha.OOCHA_BENCHMARK_NAME:
        oocha_setup = lambda dbs, input_dir: oocha.setup_oocha_benchmark(dbs, input_dir, tpch_sf)
        return create_managed_runner(oocha.run_oocha_epoch_benchmark, tpch_sf), oocha_setup

    raise ValueError(f"Unknown benchmark '{name}'")
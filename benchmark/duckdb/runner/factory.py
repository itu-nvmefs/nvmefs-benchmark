import time
from . import tpch, ycsb, htap
from .coordinator import WorkerHandle, ActiveClock
from .oocha import oocha

NAMESPACE_COUNTS = {
    tpch.TPCH_BENCHMARK_NAME: 1,
    ycsb.YCSB_BENCHMARK_NAME: 1,
    htap.HTAP_BENCHMARK_NAME: htap.HTAP_NAMESPACE_COUNT,
}


def get_namespace_count(name):
    return NAMESPACE_COUNTS.get(name, 1)


def derive_db_names(namespace_identities: list) -> list:
    """
    [('tpch',0)]                       -> ['tpch']
    [('ycsb',0),('ycsb',1)]            -> ['ycsb_0','ycsb_1']
    [('tpch',0),('ycsb',0)]            -> ['tpch','ycsb']
    [('tpch',0),('ycsb',0),('ycsb',1)] -> ['tpch','ycsb_0','ycsb_1']
    """
    counts = {}
    for name, _ in namespace_identities:
        counts[name] = counts.get(name, 0) + 1
    used, out = {}, []
    for name, _ in namespace_identities:
        if counts[name] > 1:
            idx = used.get(name, 0)
            out.append(f"{name}_{idx}")
            used[name] = idx + 1
        else:
            out.append(name)
    return out


def create_benchmark_runner(name, run_with_duration, checkpoint_mode="auto",
                            tpch_sf=100, ycsb_sf=100, coordinator=None,
                            output_handle=None):

    def create_managed_runner(benchmark, sf, register_handle=False):
        def duration_wrapper(cursors, duration_minutes):
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

                kwargs = {}
                if out is not None:
                    kwargs["output_handle"] = out
                if handle is not None:
                    kwargs["worker_handle"] = handle
                if clock is not None:
                    while clock.elapsed() < limit_s:
                        for key, rows in benchmark(cursors, sf, **kwargs).items():
                            merged.setdefault(key, []).extend(rows)
                else:
                    start = time.monotonic()
                    while (time.monotonic() - start) < limit_s:
                        for key, rows in benchmark(cursors, sf, **kwargs).items():
                            merged.setdefault(key, []).extend(rows)
                return merged
            finally:
                if handle is not None and coordinator is not None:
                    coordinator.remove_worker(handle)

        def repetition_wrapper(cursors, repetitions):
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
                    for key, rows in benchmark(cursors, sf, **kwargs).items():
                        merged.setdefault(key, []).extend(rows)
                return merged
            finally:
                if handle is not None and coordinator is not None:
                    coordinator.remove_worker(handle)

        return duration_wrapper if run_with_duration else repetition_wrapper

    if name == tpch.TPCH_BENCHMARK_NAME:
        tpch_setup = lambda cursors, input_dir: tpch.setup_tpch_benchmark(cursors, input_dir, tpch_sf)
        return create_managed_runner(tpch.run_tpch_epoch_benchmark, tpch_sf, register_handle=True), tpch_setup

    if name == ycsb.YCSB_BENCHMARK_NAME:
        ycsb_setup = lambda cursors, input_dir: ycsb.setup_ycsb_benchmark(
            cursors, input_dir, ycsb_sf, checkpoint_mode)

        def ycsb_run(cursors, duration_or_reps):
            duration = (duration_or_reps * 60) if run_with_duration else 0
            reps = 0 if run_with_duration else duration_or_reps

            handle = None
            if coordinator is not None:
                handle = WorkerHandle(name=name)
                coordinator.add_worker(handle)

            out = output_handle.get(name) if output_handle else None
            try:
                return ycsb.run_ycsb_epoch_benchmark(
                    cursors, ycsb_sf, duration, reps, checkpoint_mode,
                    interval_seconds=660, coordinator=coordinator,
                    output_handle=out, worker_handle=handle,
                )
            finally:
                if handle is not None and coordinator is not None:
                    coordinator.remove_worker(handle)

        return ycsb_run, ycsb_setup

    if name == htap.HTAP_BENCHMARK_NAME:
        htap_setup = lambda cursors, input_dir: htap.setup_htap_benchmark(
            cursors, input_dir, tpch_sf, ycsb_sf, checkpoint_mode)

        def htap_run(cursors, duration_or_reps):
            duration = (duration_or_reps * 60) if run_with_duration else 0
            reps = 0 if run_with_duration else duration_or_reps
            return htap.run_htap_epoch_benchmark(
                cursors, tpch_sf, ycsb_sf, duration, reps, checkpoint_mode,
                interval_seconds=660, coordinator=coordinator,
                output_handle=output_handle)
        return htap_run, htap_setup

    if name == oocha.OOCHA_SPILL_BENCHMARK_NAME:
        oocha_spill_setup = lambda cursors, input_dir: oocha.setup_oocha_spill_benchmark(cursors, input_dir, tpch_sf)
        return create_managed_runner(oocha.run_oocha_spill_epoch_benchmark, tpch_sf), oocha_spill_setup

    if name == oocha.OOCHA_BENCHMARK_NAME:
        oocha_setup = lambda cursors, input_dir: oocha.setup_oocha_benchmark(cursors, input_dir, tpch_sf)
        return create_managed_runner(oocha.run_oocha_epoch_benchmark, tpch_sf), oocha_setup

    raise ValueError(f"Unknown benchmark '{name}'")
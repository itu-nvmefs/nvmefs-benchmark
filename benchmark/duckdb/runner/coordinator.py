import os
import time
import threading
from datetime import datetime
from typing import List, Optional

class WorkerHandle:
    """
    Pause/resume handle for a Python-side worker 
    """
    def __init__(self, name: str = "", clock: Optional["ActiveClock"] = None):
        self.name = name
        self.clock = clock
        self._pause_requested = threading.Event()
        self._paused = threading.Event()
        self._release = threading.Event()
    
    def request_pause(self):
        self._release.clear()
        self._paused.clear()
        self._pause_requested.set()
    
    def wait_until_paused(self, timeout: Optional[float] = None) -> bool:
        return self._paused.wait(timeout)
    
    def release(self):
        self._pause_requested.clear()
        self._release.set()

    def checkpoint_if_requested(self, label: str = "") -> bool:
        if not self._pause_requested.is_set():
            return False
        if self.clock is not None:
            self.clock.pause()
        self._paused.set()
        self._release.wait()
        if self.clock is not None:
            self.clock.resume()
        return True

class CppWorkerAdapter:
    """
    Adapter exposing the YCSB C++ runner to the coordinator with the same
    interface as WorkerHandle.
    """
    def __init__(self, runner, name: str = "ycsb"):
        self.runner = runner
        self.name = name
    
    def request_pause(self):
        self.runner.request_checkpoint()

    def wait_until_paused(self, timeout: Optional[float] = None) -> bool:
        deadline = time.monotonic() + (timeout if timeout is not None else 1e9)
        while time.monotonic() < deadline:
            if self.runner.is_checkpoint_paused():
                return True
            time.sleep(0.05)
        return False

    def release(self):
        self.runner.release_checkpoint()
    
class WAFCheckpoint:
    """
    Coordinates drain checkpoints across one or more workers
    Output CSV: timestamp;source;phase;host_written,media_written;interval_waf;cumulative_waf
    """

    def __init__(self, device, output_file: str, enable_fdp: bool, 
                drain_interval_s: int = 660, 
                drain_duration_s: int = 660, 
                drain_final_duration_s= 1800,
                drain_poll_interval_s: int = 30):
        self.device = device
        self.output_file = output_file
        self.enable_fdp = enable_fdp
        self.drain_interval_s = drain_interval_s
        self.drain_duration_s = drain_duration_s
        self.drain_final_duration_s = drain_final_duration_s
        self.drain_poll_interval_s = drain_poll_interval_s

        self._workers: List = []
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

        # WAF deltas
        self._start_host = self._start_media = 0
        self._prev_host = self._prev_media = 0
    
    def add_worker(self, worker):
        with self._lock:
            self._workers.append(worker)

    def remove_worker(self, worker):
        with self._lock:
            if worker in self._workers:
                self._workers.remove(worker)

    def start(self):
        os.system("sync")
        h, m = self.device.get_written_bytes()
        self._start_host = self._prev_host = h
        self._start_media = self._prev_media = m

        with open(self.output_file, "w", newline="\n") as f:
            f.write("timestamp;source;phase;host_written,media_written;interval_waf;cumulative_waf\n")
            ts = datetime.now()
            f.write(f"{ts};smart-log;start;{h},{m};0.0000;0.0000\n")

        self._thread = threading.Thread(target=self._loop, name="waf-coord", daemon=True)
        self._thread.start()
    
    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join()
        self._record_final()
    
    def _loop(self):
        while not self._stop.is_set():
            for _ in range(self.drain_interval_s):
                if self._stop.is_set():
                    return
                time.sleep(1)
            try:
                self._do_drain()
            except Exception as e:
                print(f"[WAF coord] drain failed: {e}")
    
    def _do_drain(self):
        with self._lock:
            workers = list(self._workers)

        if not workers:
            return

        slow_workers = [w for w in workers if "tpch" in getattr(w, "name", "").lower()]
        fast_workers = [w for w in workers if w not in slow_workers]

        for w in slow_workers:
            w.request_pause()

        all_ok = True
        for w in slow_workers:
            if not w.wait_until_paused(timeout=1800):
                print(f"[WAF coord] slow worker '{getattr(w, 'name', '?')}' did not pause within 1800s")
                all_ok = False

        if not all_ok:
            for w in slow_workers:
                w.release()
            return

        for w in fast_workers:
            w.request_pause()

        for w in fast_workers:
            if not w.wait_until_paused(timeout=300):
                print(f"[WAF coord] fast worker '{getattr(w, 'name', '?')}' did not pause within 300s")
                all_ok = False

        if not all_ok:
            for w in workers:
                w.release()
            return

        try:
            os.system("sync")
            ts_pre = datetime.now()
            h_pre, m_pre = self.device.get_written_bytes()
        
            print(f"[WAF coord] draining for {self.drain_duration_s}s")
            waited, observed = self._wait_for_counter_update(self.drain_duration_s)
            status = "counter updated" if observed else "TIMEOUT (no change)"
            print(f"[WAF coord] drain done in {waited}s — {status}")
            
            os.system("sync")
            ts_post = datetime.now()
            h_post, m_post = self.device.get_written_bytes()
            self._write_row(ts_pre, "smart-log", "pre-drain",
                            h_pre, m_pre, self._prev_host, self._prev_media,
                            self._start_host, self._start_media)
            self._write_row(ts_post, "smart-log", "post-drain",
                            h_post, m_post, self._prev_host, self._prev_media,
                            self._start_host, self._start_media)
            
            self._prev_host, self._prev_media = h_post, m_post
        finally:
            print("[WAF coord] releasing workers")
            for w in workers:
                w.release()    
        
    def _wait_for_counter_update(self, max_wait_s: int) -> tuple[int, bool]:
        _, m_initial = self.device.get_written_bytes()
        waited = 0

        while waited < max_wait_s:
            if self._stop.is_set():
                return waited, False
            time.sleep(self.drain_poll_interval_s)
            waited += self.drain_poll_interval_s

            _, m_now = self.device.get_written_bytes()
            if m_now != m_initial:
                return waited, True

        return waited, False
    
    def _write_row(self, ts, source, phase, h, m, prev_h, prev_m, start_h, start_m):
        d_h, d_m = h - prev_h, m - prev_m
        cum_h, cum_m = h - start_h, m - start_m
        waf = (d_m / d_h) if d_h > 0 else 0.0
        cum_waf = (cum_m / cum_h) if cum_h > 0 else 0.0
        with open(self.output_file, "a", newline="\n") as f:
            f.write(f"{ts};{source};{phase};{d_h},{d_m};{waf:.4f};{cum_waf:.4f}\n")

    def _record_final(self):
        os.system("sync")
        ts_immediate = datetime.now()
        h_imm, m_imm = self.device.get_written_bytes()

        self._write_row(ts_immediate, "smart-log", "final-immediate",
                    h_imm, m_imm,
                    self._prev_host, self._prev_media,
                    self._start_host, self._start_media)

        print(f"[WAF coord] final drain for {self.drain_final_duration_s}s")
        time.sleep(self.drain_final_duration_s)

        os.system("sync")
        ts_drained = datetime.now()
        h_drn, m_drn = self.device.get_written_bytes()

        self._write_row(ts_drained, "smart-log", "final-drained",
                    h_drn, m_drn,
                    self._prev_host, self._prev_media,
                    self._start_host, self._start_media)


class ActiveClock:
    """
    Wall-clock that excludes time spent paused for drains.
    """

    def __init__(self):
        self._origin = time.monotonic()
        self._paused_total = 0.0
        self._pause_started_at: Optional[float] = None

    def pause(self):
        if self._pause_started_at is None:
            self._pause_started_at = time.monotonic()

    def resume(self):
        if self._pause_started_at is not None:
            self._paused_total += time.monotonic() - self._pause_started_at
            self._pause_started_at = None

    def elapsed(self) -> float:
        now = time.monotonic()
        live_pause = (now - self._pause_started_at) if self._pause_started_at is not None else 0.0
        return (now - self._origin) - self._paused_total - live_pause

    

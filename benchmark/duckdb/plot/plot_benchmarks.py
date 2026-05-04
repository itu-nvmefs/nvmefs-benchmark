import os
import sys
import datetime
import parse
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict

def build_title(run: parse.BenchmarkRun, metric_name: str, version_label: str = "") -> str:
    fdp_str = " (FDP)" if run.fdp else ""
    bench_name = run.benchmark.upper()
    if bench_name == "TPCH":
        bench_name = "TPC-H"
        
    version_str = f" [{version_label}]" if version_label else ""
        
    return f"{bench_name}{version_str} {metric_name}, {run.memory / 1000:.1f}GB, {run.threads} Threads, SF{run.scale_factor}, {run.backend}{fdp_str}"

def make_bar_plot(x_labels, y_data, y_err, ylabel, title, out_path, color, y_max=None):
    plt.figure(figsize=(7, 5))
    plt.grid(axis="y", linestyle="--", alpha=0.7, zorder=0)
    
    plt.bar(x_labels, y_data, yerr=y_err, capsize=4, color=color, 
            edgecolor="black", zorder=3, width=0.6)
            
    plt.xticks(rotation=45, ha="right")
    plt.ylabel(ylabel)
    plt.title(title, pad=15, fontsize=10, wrap=True)
    
    # Strictly respect the provided y_max limit
    if y_max is not None and y_max > 0:
        plt.ylim(0, y_max)
        
    plt.tight_layout()
    plt.savefig(out_path, dpi=300)
    plt.close()

def plot_device_metrics(benchmark_run: parse.BenchmarkRun, out_dir: str, file_prefix: str, max_vals: dict, version_label: str = ""):
    os.makedirs(out_dir, exist_ok=True)
    data = benchmark_run.results.get("device", [])
    if not data: return
    
    timestamps = [d["timestamp"].split(" ")[1][:5] for d in data]
    wafs = [d["waf"] for d in data]
    
    plt.figure(figsize=(7, 5))
    plt.grid(axis="y", linestyle="--", alpha=0.7, zorder=0)
    plt.plot(timestamps, wafs, marker="o", color="purple", linestyle="-", zorder=3)
    
    plt.xticks(rotation=45, ha="right")
    plt.ylabel("Write Amplification Factor (WAF)")
    plt.title(build_title(benchmark_run, "WAF Over Time", version_label), pad=15, fontsize=10, wrap=True)
    
    if max_vals.get("waf") and max_vals["waf"] > 0:
        plt.ylim(0, max_vals["waf"] * 1.05) # Keeping a small 5% buffer for line plots
        
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, f"{file_prefix}_waf.png"), dpi=300)
    plt.close()

def plot_benchmark(benchmark_run: parse.BenchmarkRun, out_dir: str, file_prefix: str, max_vals: dict, version_label: str = ""):
    os.makedirs(out_dir, exist_ok=True)
    tasks = list(benchmark_run.results.keys())
    x_labels = [str(t) for t in tasks]
    
    # 1. Plot Time
    times = [np.mean(benchmark_run.results[t]) for t in tasks]
    time_errs = [np.std(benchmark_run.results[t]) if len(benchmark_run.results[t]) > 1 else 0 for t in tasks]
    make_bar_plot(
        x_labels, times, time_errs, "Time (ms)", build_title(benchmark_run, "Elapsed Time", version_label), 
        os.path.join(out_dir, f"{file_prefix}_time_ms.png"), "tab:blue", y_max=max_vals.get("time")
    )
    
    # 2. Plot Throughput
    if benchmark_run.throughputs and any(benchmark_run.throughputs.values()):
        tputs = [np.mean(benchmark_run.throughputs[t]) if t in benchmark_run.throughputs else 0 for t in tasks]
        tput_errs = [np.std(benchmark_run.throughputs[t]) if t in benchmark_run.throughputs and len(benchmark_run.throughputs[t]) > 1 else 0 for t in tasks]
        make_bar_plot(
            x_labels, tputs, tput_errs, "Throughput (ops/s)", build_title(benchmark_run, "Throughput", version_label), 
            os.path.join(out_dir, f"{file_prefix}_throughput.png"), "tab:green", y_max=max_vals.get("tput")
        )

    # 3. Plot NVMEFS Metrics
    if benchmark_run.metrics and any(benchmark_run.metrics.values()):
        wal_mb = [np.mean([m.get("total_wal_bytes", 0) for m in benchmark_run.metrics[t]]) / (1024**2) if t in benchmark_run.metrics else 0 for t in tasks]
        wal_errs = [np.std([m.get("total_wal_bytes", 0) for m in benchmark_run.metrics[t]]) / (1024**2) if t in benchmark_run.metrics and len(benchmark_run.metrics[t]) > 1 else 0 for t in tasks]
        if any(wal_mb):
            make_bar_plot(
                x_labels, wal_mb, wal_errs, "Total WAL (MB)", build_title(benchmark_run, "Total WAL Written", version_label), 
                os.path.join(out_dir, f"{file_prefix}_wal_bytes.png"), "tab:red", y_max=max_vals.get("wal")
            )
            
        spill_mb = [np.mean([m.get("total_spill_bytes", 0) for m in benchmark_run.metrics[t]]) / (1024**2) if t in benchmark_run.metrics else 0 for t in tasks]
        spill_errs = [np.std([m.get("total_spill_bytes", 0) for m in benchmark_run.metrics[t]]) / (1024**2) if t in benchmark_run.metrics and len(benchmark_run.metrics[t]) > 1 else 0 for t in tasks]
        if any(spill_mb):
            make_bar_plot(
                x_labels, spill_mb, spill_errs, "Total Spill (MB)", build_title(benchmark_run, "Total Spill Bytes", version_label), 
                os.path.join(out_dir, f"{file_prefix}_spill_bytes.png"), "tab:orange", y_max=max_vals.get("spill")
            )

def main(results_dir: str, output_base_dir: str, version_label: str = ""):
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    
    parsed_files = []
    global_maxes = defaultdict(lambda: defaultdict(float))

    for file_name in os.listdir(results_dir):
        if not file_name.endswith(".csv"): continue
            
        filepath = os.path.join(results_dir, file_name)
        file_prefix = file_name.replace(".csv", "")
        benchmark_group = file_name.split("-")[0]
        
        is_device = file_name.endswith("-device.csv")
        
        if is_device:
            run = parse.parse_device_results(filepath)
            data = run.results.get("device", [])
            if data:
                global_maxes[benchmark_group]["waf"] = max(global_maxes[benchmark_group]["waf"], max(d["waf"] for d in data))
        elif file_name.startswith("htap"):
            run = parse.parse_htap_results(filepath)
        elif file_name.startswith("ycsb"):
            run = parse.parse_ycsb_results(filepath)
        elif file_name.startswith("tpch"):
            run = parse.parse_tpch_results(filepath)
        else: continue
            
        if not is_device and run.results:
            tasks = list(run.results.keys())
            global_maxes[benchmark_group]["time"] = max(global_maxes[benchmark_group]["time"], max([np.mean(run.results[t]) for t in tasks]))
            
            if run.throughputs and any(run.throughputs.values()):
                global_maxes[benchmark_group]["tput"] = max(global_maxes[benchmark_group]["tput"], max([np.mean(run.throughputs[t]) for t in tasks if t in run.throughputs] or [0]))
                
            if run.metrics and any(run.metrics.values()):
                global_maxes[benchmark_group]["wal"] = max(global_maxes[benchmark_group]["wal"], max([np.mean([m.get("total_wal_bytes", 0) for m in run.metrics[t]]) / (1024**2) for t in tasks if t in run.metrics] or [0]))
                global_maxes[benchmark_group]["spill"] = max(global_maxes[benchmark_group]["spill"], max([np.mean([m.get("total_spill_bytes", 0) for m in run.metrics[t]]) / (1024**2) for t in tasks if t in run.metrics] or [0]))

        parsed_files.append((run, benchmark_group, file_prefix, is_device))

    # --- Hardcoded Limits ---
    # Manually override the limits for TPCH so they are strictly locked across all runs
    if "tpch" in global_maxes:
        global_maxes["tpch"]["time"] = 250000  # Cap latency to exactly 200,000 ms
        global_maxes["tpch"]["spill"] = 500000  # Cap spill to exactly 500,000 MB
    for run, benchmark_group, file_prefix, is_device in parsed_files:
        out_dir = os.path.join(output_base_dir, benchmark_group, timestamp)
        max_vals = global_maxes[benchmark_group]
        
        if is_device:
            plot_device_metrics(run, out_dir, file_prefix, max_vals, version_label)
        elif run.results:
            plot_benchmark(run, out_dir, file_prefix, max_vals, version_label)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 plot_benchmarks.py <results_dir> <output_dir> [version_label]")
        sys.exit(1)
        
    results_dir = sys.argv[1]
    output_dir = sys.argv[2]
    
    # Auto-detect version label if not provided explicitly
    if len(sys.argv) > 3:
        version_label = sys.argv[3]
    elif "old" in results_dir.lower():
        version_label = "DuckDB v1.2.0 (Old)"
    elif "new" in results_dir.lower():
        version_label = "DuckDB v1.5.2 (New)"
    else:
        version_label = ""
        
    main(results_dir, output_dir, version_label)
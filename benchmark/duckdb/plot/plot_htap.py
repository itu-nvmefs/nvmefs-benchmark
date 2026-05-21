import os
import sys
import datetime
import parse
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict
import pandas as pd

# --- GLOBAL PLOTTING COLORS ---
STORAGE_COLORS = {
    "ycsb_db": "darkolivegreen",
    "ycsb_wal": "darkorange",       
    "temp_spill": "darkred",
    "tpch_db": "midnightblue",      
    "tpch_spill_bar": "darkred"     
}

# --- LATEX-FRIENDLY PLOT SETTINGS ---
# Updates global Matplotlib settings for smaller, side-by-side figures
plt.rcParams.update({
    'font.size': 14,            # Base font size (up from 9)
    'axes.titlesize': 16,       # Title font size (up from 10)
    'axes.labelsize': 16,       # X and Y label font size (up from 9)
    'xtick.labelsize': 14,      # X tick labels (up from 8)
    'ytick.labelsize': 14,      # Y tick labels (up from 8)
    'legend.fontsize': 12,      # Legend font size (up from 7)
    'lines.linewidth': 2.5,     # Slightly thicker lines for visibility
    'lines.markersize': 6,      # Slightly larger markers
    'figure.autolayout': True   # Automatically adjust layout to prevent cropping
})

# Default figure size for 3-across in LaTeX
FIG_SIZE = (7, 5)

# --- Helper Functions for Safe Aggregation ---
def get_valid_values(values, key=None):
    valid = []
    for v in values:
        if key is not None:
            if isinstance(v, dict) and v:  
                valid.append(v.get(key, 0))
        else:
            if isinstance(v, (int, float)) and not np.isnan(v):
                valid.append(v)
            elif isinstance(v, str) and v.upper() != "FAIL":
                try:
                    valid.append(float(v))
                except ValueError:
                    pass
    return valid

def safe_mean(values, key=None, scale=1.0):
    valid = get_valid_values(values, key)
    return np.mean(valid) / scale if valid else np.nan

def safe_std(values, key=None, scale=1.0):
    valid = get_valid_values(values, key)
    return np.std(valid, ddof=1) / scale if valid and len(valid) > 1 else 0.0

def safe_percentile(values, q=99, key=None, scale=1.0):
    valid = get_valid_values(values, key)
    return np.percentile(valid, q) / scale if valid else np.nan

def build_title(run: parse.BenchmarkRun, metric_name: str, version_label: str = "") -> str:
    fdp_str = " (FDP)" if run.fdp else ""
    bench_name = run.benchmark.upper()
    if bench_name == "TPCH":
        bench_name = "TPC-H"
    version_str = f" [{version_label}]" if version_label else ""
    return f"{bench_name}{version_str} {metric_name}, {run.memory / 1000:.1f}GB, {run.threads}T, SF{run.scale_factor}, {run.backend}{fdp_str}"

# --- Plotting Functions ---
def make_bar_plot(x_labels, y_data, ylabel, title, out_path, color, hatch_pattern='//', y_max=None, yerr_data=None):
    plt.figure(figsize=FIG_SIZE)
    plt.grid(axis="y", linestyle=":", alpha=0.7, zorder=0)
    plot_y_data = [0 if np.isnan(y) else y for y in y_data]
    
    bars = plt.bar(x_labels, plot_y_data, yerr=yerr_data, capsize=2, color=color, edgecolor="black", 
                   hatch=hatch_pattern, zorder=3, width=0.7)
            
    for bar, original_y in zip(bars, y_data):
        if np.isnan(original_y):
            plt.text(bar.get_x() + bar.get_width() / 2, 0, 'FAIL', 
                     ha='center', va='bottom', color='red', rotation=90, fontweight='bold', zorder=5, fontsize=6)
            
    plt.xticks(rotation=45, ha="right")
    plt.ylabel(ylabel)
    
    if y_max is not None:
        plt.ylim(0, y_max)
        
    plt.savefig(out_path, dpi=300)
    plt.close()

def plot_tpch_footprint(benchmark_run: parse.BenchmarkRun, out_dir: str, file_prefix: str, version_label: str = ""):
    os.makedirs(out_dir, exist_ok=True)
    if not getattr(benchmark_run, "raw_series", None): return
    
    records = []
    for item in benchmark_run.raw_series:
        rec = {"query_name": item["query_name"]}
        metrics = item.get("metrics", {})
        rec.update(metrics)
        records.append(rec)
        
    df = pd.DataFrame(records)
    if "shared_temp.total_spill_bytes" not in df.columns: return
    
    df["spill_diff"] = df["shared_temp.total_spill_bytes"].diff().fillna(df["shared_temp.total_spill_bytes"]).clip(lower=0)
    df["spill_gb"] = df["spill_diff"] / (1024**3)
    
    unique_queries = df["query_name"].unique()
    
    spill_mean, spill_std, peak_gb = [], [], []
    first_iter_df = df.head(len(unique_queries))
    
    for q in unique_queries:
        query_spill_data = df[df["query_name"] == q]["spill_gb"]
        spill_mean.append(query_spill_data.mean())
        std_val = query_spill_data.std(ddof=1) if len(query_spill_data) > 1 else 0.0
        spill_std.append(std_val if pd.notna(std_val) else 0.0)
        first_iter_peak = first_iter_df[first_iter_df["query_name"] == q]["shared_temp.peak_temp_bytes"].iloc[0]
        peak_gb.append(first_iter_peak / (1024**3))
    
    x_labels = [str(q) for q in unique_queries]
    
    plt.figure(figsize=FIG_SIZE)
    plt.grid(axis="y", linestyle="--", alpha=0.7, zorder=0)
    x = np.arange(len(x_labels))

    plt.bar(x, spill_mean, yerr=spill_std, capsize=2, width=0.7, label='Mean Spilled (GB)', 
            color='tab:orange', edgecolor="black", hatch='//', zorder=3)
    plt.plot(x, peak_gb, label='Peak Temp (GB)', 
             color='gray', marker='o', linewidth=1.5, zorder=4)
    
    plt.ylabel("Size (GB)")
    plt.xticks(x, x_labels, rotation=45, ha="right")
    plt.ylim(0, 2500)
    plt.legend(loc='upper left', fontsize=6)
    plt.savefig(os.path.join(out_dir, f"{file_prefix}_tpch_footprint.png"), dpi=300)
    plt.close()

def plot_ycsb_storage_timeline(benchmark_run: parse.BenchmarkRun, out_dir: str, file_prefix: str, version_label: str = ""):
    os.makedirs(out_dir, exist_ok=True)
    if not benchmark_run.raw_series or not benchmark_run.metrics: return
    
    workload_keys = list(benchmark_run.metrics.keys())
    if not workload_keys: return
    key = workload_keys[0]
    
    time_mins = [d["offset_s"] / 60.0 for d in benchmark_run.raw_series]
    metrics = benchmark_run.metrics[key]
    
    ycsb_db = [m.get("ycsb.current_db_bytes", 0) / (1024**3) for m in metrics]
    wal = [m.get("ycsb.total_wal_bytes", 0) / (1024**3) for m in metrics]
    
    fig, ax1 = plt.subplots(figsize=FIG_SIZE)

    color1 = 'tab:green'
    ax1.set_xlabel('Time (Min)')
    ax1.set_ylabel('DB Size (GB)', color=color1)
    ax1.plot(time_mins, ycsb_db, color=color1, marker='o', label='DB Size')
    ax1.tick_params(axis='y', labelcolor=color1)
    ax1.grid(axis='y', linestyle='--', alpha=0.7)

    ax2 = ax1.twinx() 
    color2 = 'tab:red'
    ax2.set_ylabel('Total WAL (GB)', color=color2) 
    ax2.plot(time_mins, wal, color=color2, marker='s', label='WAL')
    ax2.tick_params(axis='y', labelcolor=color2)

    lines_1, labels_1 = ax1.get_legend_handles_labels()
    lines_2, labels_2 = ax2.get_legend_handles_labels()
    ax1.legend(lines_1 + lines_2, labels_1 + labels_2, loc='upper left', fontsize=6)

    plt.savefig(os.path.join(out_dir, f"{file_prefix}_ycsb_storage_timeline.png"), dpi=300)
    plt.close()

def plot_tpch_shared_storage_timeline(benchmark_run: parse.BenchmarkRun, out_dir: str, file_prefix: str, version_label: str = ""):
    os.makedirs(out_dir, exist_ok=True)
    if not benchmark_run.raw_series: return
    
    time_mins, ycsb_db, ycsb_wal, temp_spill = [], [], [], []
    cum_time_ms = 0
    
    for item in benchmark_run.raw_series:
        cum_time_ms += item.get("latency_ms", 0)
        time_mins.append(cum_time_ms / 60000.0)
        m = item.get("metrics", {})
        ycsb_db.append(m.get("ycsb.total_db_bytes", 0) / (1024**3))
        ycsb_wal.append(m.get("ycsb.total_wal_bytes", 0) / (1024**3))
        temp_spill.append(m.get("shared_temp.total_spill_bytes", 0) / (1024**3))
        
    plt.figure(figsize=FIG_SIZE)
    plt.grid(True, linestyle=":", alpha=0.6, zorder=0)
    
    plt.plot(time_mins, ycsb_db, label="DB Size", color=STORAGE_COLORS["ycsb_db"], marker="o", zorder=3)
    plt.plot(time_mins, temp_spill, label="Temp Spill", color=STORAGE_COLORS["temp_spill"], marker="^", zorder=3)
    plt.plot(time_mins, ycsb_wal, label="WAL Size", color=STORAGE_COLORS["ycsb_wal"], marker="s", zorder=4)
    
    plt.yscale('symlog', base=10, linthresh=1) 
    plt.xlabel("Time (Min)")
    plt.ylabel("Size (GB) - Log")
    plt.legend(loc='upper left')
    plt.savefig(os.path.join(out_dir, f"{file_prefix}_tpch_shared_timeline.png"), dpi=300)
    plt.close()

def plot_tpch_shared_storage_rate_timeline(benchmark_run: parse.BenchmarkRun, out_dir: str, file_prefix: str, version_label: str = ""):
    os.makedirs(out_dir, exist_ok=True)
    if not benchmark_run.raw_series: return
    
    time_mins, ycsb_db_rate, ycsb_wal_rate, temp_spill_rate = [], [], [], []
    cum_time_ms, prev_db, prev_wal, prev_spill = 0, 0, 0, 0
    
    for i, item in enumerate(benchmark_run.raw_series):
        lat_ms = item.get("latency_ms", 0)
        cum_time_ms += lat_ms
        time_mins.append(cum_time_ms / 60000.0)
        
        m = item.get("metrics", {})
        curr_db = m.get("ycsb.total_db_bytes", 0) / (1024**3)
        curr_wal = m.get("ycsb.total_wal_bytes", 0) / (1024**3)
        curr_spill = m.get("shared_temp.total_spill_bytes", 0) / (1024**3)
        
        step_min = lat_ms / 60000.0 if lat_ms > 0 else 1.0
        
        if i == 0:
            ycsb_db_rate.append(0); ycsb_wal_rate.append(0); temp_spill_rate.append(0)
        else:
            ycsb_db_rate.append(max(0, curr_db - prev_db) / step_min)
            ycsb_wal_rate.append(max(0, curr_wal - prev_wal) / step_min)
            temp_spill_rate.append(max(0, curr_spill - prev_spill) / step_min)
            
        prev_db, prev_wal, prev_spill = curr_db, curr_wal, curr_spill

    def smooth(y, box_pts=3):
        box = np.ones(box_pts) / box_pts
        return np.convolve(y, box, mode='same')
        
    ycsb_db_rate = smooth(ycsb_db_rate, 3)
    ycsb_wal_rate = smooth(ycsb_wal_rate, 3)
    temp_spill_rate = smooth(temp_spill_rate, 3)
        
    plt.figure(figsize=FIG_SIZE)
    plt.grid(True, linestyle=":", alpha=0.6, zorder=0)
    
    plt.plot(time_mins, ycsb_db_rate, label="DB Rate", color=STORAGE_COLORS["ycsb_db"], zorder=3)
    plt.fill_between(time_mins, ycsb_db_rate, facecolor="none", edgecolor=STORAGE_COLORS["ycsb_db"], hatch='//', alpha=0.8, zorder=3)
    
    plt.plot(time_mins, temp_spill_rate, label="Spill Rate", color=STORAGE_COLORS["temp_spill"], zorder=3, linestyle='-.')
    plt.fill_between(time_mins, temp_spill_rate, facecolor="none", edgecolor=STORAGE_COLORS["temp_spill"], hatch='xx', alpha=0.8, zorder=3)

    plt.plot(time_mins, ycsb_wal_rate, label="WAL Rate", color=STORAGE_COLORS["ycsb_wal"], linewidth=2, linestyle='--', zorder=4)
    plt.fill_between(time_mins, ycsb_wal_rate, facecolor="none", edgecolor=STORAGE_COLORS["ycsb_wal"], hatch='\\\\', alpha=1.0, zorder=4)
    
    plt.yscale('symlog', base=10, linthresh=0.1) 
    plt.xlabel("Time (Min)")
    plt.ylabel("Rate (GB/min)")
    plt.legend(loc='upper left')
    plt.savefig(os.path.join(out_dir, f"{file_prefix}_tpch_shared_rate_timeline.png"), dpi=300)
    plt.close()

def plot_ycsb_timeseries(benchmark_run: parse.BenchmarkRun, out_dir: str, file_prefix: str, version_label: str = ""):
    os.makedirs(out_dir, exist_ok=True)
    data = benchmark_run.raw_series
    if not data: return

    offsets_min = [d["offset_s"] / 60.0 for d in data] 
    tputs = [d["throughput_ops"] for d in data]

    plt.figure(figsize=FIG_SIZE)
    plt.grid(True, linestyle=":", alpha=0.6, zorder=0)
    
    plt.plot(offsets_min, tputs, marker='o', linestyle='-', color=STORAGE_COLORS.get("ycsb_db", "tab:purple"), zorder=3)
    plt.ylim(0, 2.5)
    plt.xlabel("Time (Min)")
    plt.ylabel("Throughput (ops/s)")
    
    plt.savefig(os.path.join(out_dir, f"{file_prefix}_throughput_timeline.png"), dpi=300)
    plt.close()

def plot_ycsb_data_throughput(benchmark_run: parse.BenchmarkRun, out_dir: str, file_prefix: str, version_label: str = ""):
    os.makedirs(out_dir, exist_ok=True)
    if not benchmark_run.raw_series: return
    
    time_mins, write_mbps = [], []
    prev_wal, prev_db = 0, 0
    
    for i, item in enumerate(benchmark_run.raw_series):
        offset_s = item.get("offset_s", 0)
        interval_s = item.get("interval_ms", 1000) / 1000.0 if item.get("interval_ms", 1000) > 0 else 1.0
        
        m = item.get("metrics", {})
        curr_wal, curr_db = m.get("ycsb.total_wal_bytes", 0), m.get("ycsb.total_db_bytes", 0)
        
        if i == 0:
            write_mbps.append(0)
        else:
            mb_per_sec = ((max(0, curr_wal - prev_wal) + max(0, curr_db - prev_db)) / (1024 * 1024)) / interval_s
            write_mbps.append(mb_per_sec)
            
        time_mins.append(offset_s / 60.0)
        prev_wal, prev_db = curr_wal, curr_db

    def smooth(y, box_pts=3):
        box = np.ones(box_pts) / box_pts
        return np.convolve(y, box, mode='same')
        
    write_mbps_smoothed = smooth(write_mbps, 3)

    plt.figure(figsize=FIG_SIZE)
    plt.grid(True, linestyle=":", alpha=0.6, zorder=0)
    
    plt.plot(time_mins, write_mbps_smoothed, label="Throughput", color=STORAGE_COLORS.get("ycsb_wal", "tab:orange"), zorder=3)
    plt.fill_between(time_mins, write_mbps_smoothed, facecolor="none", edgecolor=STORAGE_COLORS.get("ycsb_wal", "tab:orange"), hatch='//', alpha=0.8, zorder=3)
    
    plt.xlabel("Time (Min)")
    plt.ylabel("Write (MB/s)")
    plt.legend()
    plt.savefig(os.path.join(out_dir, f"{file_prefix}_ycsb_data_throughput.png"), dpi=300)
    plt.close()

def plot_device_metrics(benchmark_run: parse.BenchmarkRun, out_dir: str, file_prefix: str, version_label: str = ""):
    os.makedirs(out_dir, exist_ok=True)
    data = benchmark_run.results.get("device", [])
    if not data: return
    filtered_data = [d for d in data if d.get("phase") == "post-drain"]
    if not filtered_data: return
    
    time_mins = [d["elapsed_min"] for d in filtered_data]
    wafs = [d["waf"] if isinstance(d["waf"], (int, float)) else np.nan for d in filtered_data]
    
    plt.figure(figsize=(10, 6)) 
    plt.grid(True, linestyle=":", alpha=0.7, zorder=0)
    
    plt.plot(time_mins, wafs, marker="s", color="tab:cyan", zorder=3, label="Interval WAF", linewidth=3.0, markersize=8)
    plt.axhline(1.0, color='gray', linestyle='--', alpha=0.8, zorder=2, label="Ideal (1.0)")
    
    plt.ylim(0.8, 1.5)
    plt.xlabel("Time (Min)")
    plt.ylabel("WAF")
    
    plt.legend(loc="upper right")
    plt.savefig(os.path.join(out_dir, f"{file_prefix}_drained_waf.png"), dpi=300)
    plt.close()

def main(results_dir: str, output_base_dir: str, version_label: str = ""):
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    
    for file_name in os.listdir(results_dir):
        if not file_name.endswith(".csv"): continue
            
        filepath = os.path.join(results_dir, file_name)
        file_prefix = file_name.replace(".csv", "")
        benchmark_group = file_name.split("-")[0]
        
        # Route tpch,ycsb to the htap folder
        if benchmark_group == "tpch,ycsb":
            benchmark_group = "htap"
        
        is_device = file_name.endswith("-device.csv")
        out_dir = os.path.join(output_base_dir, benchmark_group, timestamp)
        os.makedirs(out_dir, exist_ok=True) 
        
        if is_device:
            run = parse.parse_device_results(filepath)
            plot_device_metrics(run, out_dir, file_prefix, version_label)
            
        elif "_ycsb" in file_name or file_name.startswith("ycsb-"):
            run = parse.parse_ycsb_results(filepath)
            plot_ycsb_timeseries(run, out_dir, file_prefix, version_label)
            plot_ycsb_storage_timeline(run, out_dir, file_prefix, version_label)
            plot_ycsb_data_throughput(run, out_dir, file_prefix, version_label)
            
        elif "_tpch" in file_name or file_name.startswith("tpch-"):
            run = parse.parse_tpch_results(filepath)
            tasks = list(run.results.keys())
            
            # Calculate means and standard deviations
            times = [safe_mean(run.results[t], scale=1000.0) for t in tasks]
            stds = [safe_std(run.results[t], scale=1000.0) for t in tasks]
            p99_times = [safe_percentile(run.results[t], q=99, scale=1000.0) for t in tasks]
            x_labels = [str(t) for t in tasks]
            
            # Mean latency with error bars for standard deviation
            make_bar_plot(x_labels, times, "Latency (s)", build_title(run, "Latency per Query", version_label), 
                          os.path.join(out_dir, f"{file_prefix}_latency.png"), 
                          color="tab:blue", hatch_pattern='//', y_max=1500, yerr_data=stds)
            
            # 2. Plot P99 Latency (no error bars needed for percentiles)
            make_bar_plot(x_labels, p99_times, "P99 Latency (s)", build_title(run, "P99 Latency per Query", version_label), 
                          os.path.join(out_dir, f"{file_prefix}_latency_p99.png"), 
                          color="tab:purple", hatch_pattern='\\\\', y_max=1500)
            
            plot_tpch_footprint(run, out_dir, file_prefix, version_label)
            plot_tpch_shared_storage_timeline(run, out_dir, file_prefix, version_label)
            plot_tpch_shared_storage_rate_timeline(run, out_dir, file_prefix, version_label)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 plot_benchmarks.py <results_dir> <output_dir> [version_label]")
        sys.exit(1)
        
    results_dir = sys.argv[1]
    output_dir = sys.argv[2]
    version_label = sys.argv[3] if len(sys.argv) > 3 else ""
    main(results_dir, output_dir, version_label)
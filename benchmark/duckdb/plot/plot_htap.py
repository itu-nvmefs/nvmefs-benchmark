import os
import sys
import datetime
import parse
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict
import pandas as pd


# --- GLOBAL PLOTTING COLORS ---
# Use these colors across ALL graphing functions to guarantee consistency.
STORAGE_COLORS = {
    "ycsb_db": "darkolivegreen",
    "ycsb_wal": "darkorange",       # Vibrant highlight color for WAL
    "temp_spill": "darkred",
    "tpch_db": "midnightblue",      # Included in case you plot TPC-H DB size later
    "tpch_spill_bar": "darkred"     # Re-using the spill color for bar charts
}

# --- Helper Functions for Safe Aggregation ---
def get_valid_values(values, key=None):
    """Filters out 'FAIL' strings, Nones, NaNs, and handles empty dicts."""
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

def build_title(run: parse.BenchmarkRun, metric_name: str, version_label: str = "") -> str:
    fdp_str = " (FDP)" if run.fdp else ""
    bench_name = run.benchmark.upper()
    if bench_name == "TPCH":
        bench_name = "TPC-H"
    version_str = f" [{version_label}]" if version_label else ""
    return f"{bench_name}{version_str} {metric_name}, {run.memory / 1000:.1f}GB, {run.threads} Threads, SF{run.scale_factor}, {run.backend}{fdp_str}"

# --- Plotting Functions ---
def make_bar_plot(x_labels, y_data, ylabel, title, out_path, color, hatch_pattern='//', y_max=None):
    plt.figure(figsize=(10, 5))
    plt.grid(axis="y", linestyle=":", alpha=0.7, zorder=0)
    plot_y_data = [0 if np.isnan(y) else y for y in y_data]
    
    bars = plt.bar(x_labels, plot_y_data, capsize=4, color=color, edgecolor="black", 
                   hatch=hatch_pattern, zorder=3, width=0.6)
            
    for bar, original_y in zip(bars, y_data):
        if np.isnan(original_y):
            plt.text(bar.get_x() + bar.get_width() / 2, 0, 'FAIL', 
                     ha='center', va='bottom', color='red', rotation=90, fontweight='bold', zorder=5)
            
    plt.xticks(rotation=45, ha="right")
    plt.ylabel(ylabel)
    #plt.title(title, pad=15, fontsize=10, wrap=True)
    
    # ---------------------------------------------------------
    # LOCK THE Y-AXIS IF PROVIDED
    # ---------------------------------------------------------
    if y_max is not None:
        plt.ylim(0, y_max)
        
    plt.tight_layout()
    plt.savefig(out_path, dpi=300)
    plt.close()

def plot_tpch_footprint(benchmark_run: parse.BenchmarkRun, out_dir: str, file_prefix: str, version_label: str = ""):
    """Plots total temporary spilling as bars and peak temporary bytes as a grey line per TPC-H query (First Iteration Only)."""
    os.makedirs(out_dir, exist_ok=True)
    
    if not getattr(benchmark_run, "raw_series", None): return
    
    records = []
    for item in benchmark_run.raw_series:
        rec = {"query_name": item["query_name"]}
        rec.update(item["metrics"])
        records.append(rec)
        
    df = pd.DataFrame(records)
    if "shared_temp.total_spill_bytes" not in df.columns: return
    
    # FIX: Slice the dataframe to grab ONLY the first iteration (first 22 queries)
    # This prevents the 'peak_temp_bytes' high-water mark from ruining queries 1-8 in subsequent iterations.
    num_queries = len(df["query_name"].unique()) # Should be 22
    df = df.head(num_queries).copy()
    
    # Chronological Diff: Calculate how much was spilled during THIS specific query execution
    df["spill_diff"] = df["shared_temp.total_spill_bytes"].diff().fillna(df["shared_temp.total_spill_bytes"]).clip(lower=0)
    
    # Prepare data for plotting
    x_labels = df["query_name"].astype(str).tolist()
    spill_gb = (df["spill_diff"] / (1024**3)).tolist()
    peak_gb = (df["shared_temp.peak_temp_bytes"] / (1024**3)).tolist()
    
    plt.figure(figsize=(12, 6))
    plt.grid(axis="y", linestyle="--", alpha=0.7, zorder=0)
    x = np.arange(len(x_labels))
    width = 0.6  

    # Plot Total Spilled as a Bar (reverted to original orange color)
    plt.bar(x, spill_gb, width, label='Total Spilled (GB)', 
            color='tab:orange', edgecolor="black", hatch='//', zorder=3)
            
    # Plot Peak Temp Bytes as a Grey Line
    plt.plot(x, peak_gb, label='Peak Temp Bytes (GB)', 
             color='gray', marker='o', linewidth=2.5, markersize=6, zorder=4)
    
    plt.ylabel("Size (GB)")
    #plt.title(build_title(benchmark_run, "TPC-H Per-Query Footprint (Spill vs Peak)", version_label), pad=15)
    plt.xticks(x, x_labels, rotation=45, ha="right")
    
    # Keep the Y-axis capped to 500 GB
    plt.ylim(0, 500)
    
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, f"{file_prefix}_tpch_footprint.png"), dpi=300)
    plt.close()

def plot_ycsb_storage_timeline(benchmark_run: parse.BenchmarkRun, out_dir: str, file_prefix: str, version_label: str = ""):
    """Plots YCSB DB Size and WAL Size on a dual-axis timeline."""
    os.makedirs(out_dir, exist_ok=True)
    if not benchmark_run.raw_series or not benchmark_run.metrics: return
    
    workload_keys = list(benchmark_run.metrics.keys())
    if not workload_keys: return
    key = workload_keys[0]
    
    time_mins = [d["offset_s"] / 60.0 for d in benchmark_run.raw_series]
    metrics = benchmark_run.metrics[key]
    
    ycsb_db = [m.get("ycsb.current_db_bytes", 0) / (1024**3) for m in metrics]
    wal = [m.get("ycsb.total_wal_bytes", 0) / (1024**3) for m in metrics]
    
    fig, ax1 = plt.subplots(figsize=(10, 5))

    # Left Y-Axis: DB Size
    color1 = 'tab:green'
    ax1.set_xlabel('Elapsed Time (Minutes)')
    ax1.set_ylabel('DB Size (GB)', color=color1)
    ax1.plot(time_mins, ycsb_db, color=color1, marker='o', label='YCSB DB Size', linewidth=2)
    ax1.tick_params(axis='y', labelcolor=color1)
    ax1.grid(axis='y', linestyle='--', alpha=0.7)

    # Right Y-Axis: WAL Size
    ax2 = ax1.twinx() 
    color2 = 'tab:red'
    ax2.set_ylabel('Total WAL Written (GB)', color=color2) 
    ax2.plot(time_mins, wal, color=color2, marker='s', label='Total WAL Written', linewidth=2)
    ax2.tick_params(axis='y', labelcolor=color2)

    #plt.title(build_title(benchmark_run, "YCSB Storage Footprint: DB Size and WAL", version_label), pad=15)

    # Combine legends from both axes
    lines_1, labels_1 = ax1.get_legend_handles_labels()
    lines_2, labels_2 = ax2.get_legend_handles_labels()
    ax1.legend(lines_1 + lines_2, labels_1 + labels_2, loc='upper left')

    fig.tight_layout()
    plt.savefig(os.path.join(out_dir, f"{file_prefix}_ycsb_storage_timeline.png"), dpi=300)
    plt.close()

def plot_tpch_shared_storage_timeline(benchmark_run: parse.BenchmarkRun, out_dir: str, file_prefix: str, version_label: str = ""):
    """Plots TPC-H Temp Spill, YCSB DB, and YCSB WAL on a timeline using consistent colors."""
    os.makedirs(out_dir, exist_ok=True)
    if not benchmark_run.raw_series: return
    
    time_mins = []
    cum_time_ms = 0
    ycsb_db = []
    ycsb_wal = []
    temp_spill = []
    
    for item in benchmark_run.raw_series:
        cum_time_ms += item.get("latency_ms", 0)
        time_mins.append(cum_time_ms / 60000.0)
        
        m = item.get("metrics", {})
        ycsb_db.append(m.get("ycsb.total_db_bytes", 0) / (1024**3))
        ycsb_wal.append(m.get("ycsb.total_wal_bytes", 0) / (1024**3))
        temp_spill.append(m.get("shared_temp.total_spill_bytes", 0) / (1024**3))
        
    plt.figure(figsize=(12, 6))
    plt.grid(True, linestyle=":", alpha=0.6, zorder=0)
    
    # --- USE GLOBAL COLORS HERE ---
    plt.plot(time_mins, ycsb_db, label="YCSB Total DB Size", 
             color=STORAGE_COLORS["ycsb_db"], marker="o", markersize=5, zorder=3)
             
    plt.plot(time_mins, temp_spill, label="Temp Spill Bytes", 
             color=STORAGE_COLORS["temp_spill"], marker="^", markersize=5, zorder=3)
             
    # Keep WAL highlighted with a thicker line and higher z-order
    plt.plot(time_mins, ycsb_wal, label="YCSB Total WAL Bytes", 
             color=STORAGE_COLORS["ycsb_wal"], marker="s", markersize=5, linewidth=2.5, zorder=4)
    
    plt.yscale('symlog', base=10, linthresh=1) 
    plt.xlabel("Elapsed TPC-H Time (Minutes)")
    plt.ylabel("Size (GB) - Log Scale")
    #plt.title(build_title(benchmark_run, "TPC-H Timeline: Temp vs YCSB Metrics", version_label), pad=15)
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, f"{file_prefix}_tpch_shared_timeline.png"), dpi=300)
    plt.close()

def plot_tpch_shared_storage_rate_timeline(benchmark_run: parse.BenchmarkRun, out_dir: str, file_prefix: str, version_label: str = ""):
    """Plots the Write Rate (GB/min) with LaTeX-friendly hatching and consistent colors."""
    os.makedirs(out_dir, exist_ok=True)
    if not benchmark_run.raw_series: return
    
    time_mins = []
    cum_time_ms = 0
    
    ycsb_db_rate = []
    ycsb_wal_rate = []
    temp_spill_rate = []
    
    prev_db = 0
    prev_wal = 0
    prev_spill = 0
    
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
            ycsb_db_rate.append(0)
            ycsb_wal_rate.append(0)
            temp_spill_rate.append(0)
        else:
            ycsb_db_rate.append(max(0, curr_db - prev_db) / step_min)
            ycsb_wal_rate.append(max(0, curr_wal - prev_wal) / step_min)
            temp_spill_rate.append(max(0, curr_spill - prev_spill) / step_min)
            
        prev_db = curr_db
        prev_wal = curr_wal
        prev_spill = curr_spill

    # Moving average for smoothing
    def smooth(y, box_pts=3):
        box = np.ones(box_pts) / box_pts
        return np.convolve(y, box, mode='same')
        
    ycsb_db_rate = smooth(ycsb_db_rate, 3)
    ycsb_wal_rate = smooth(ycsb_wal_rate, 3)
    temp_spill_rate = smooth(temp_spill_rate, 3)
        
    plt.figure(figsize=(12, 6))
    plt.grid(True, linestyle=":", alpha=0.6, zorder=0)
    
    # ---------------------------------------------------------
    # HATCHING SETUP WITH GLOBAL CONSISTENT COLORS
    # ---------------------------------------------------------
    
    # 1. YCSB DB (drawn underneath, normal thickness)
    plt.plot(time_mins, ycsb_db_rate, label="YCSB DB Write Rate", 
             color=STORAGE_COLORS["ycsb_db"], linewidth=2, linestyle='-', zorder=3)
    plt.fill_between(time_mins, ycsb_db_rate, facecolor="none", 
                     edgecolor=STORAGE_COLORS["ycsb_db"], hatch='//', alpha=0.8, zorder=3)
    
    # 2. Temp Spill (drawn underneath, normal thickness)
    plt.plot(time_mins, temp_spill_rate, label="Temp Spill Write Rate", 
             color=STORAGE_COLORS["temp_spill"], linewidth=2, linestyle='-.', zorder=3)
    plt.fill_between(time_mins, temp_spill_rate, facecolor="none", 
                     edgecolor=STORAGE_COLORS["temp_spill"], hatch='xx', alpha=0.8, zorder=3)

    # 3. YCSB WAL (Highlighted: Thicker line, denser hatch, drawn on top with zorder=4)
    plt.plot(time_mins, ycsb_wal_rate, label="YCSB WAL Write Rate", 
             color=STORAGE_COLORS["ycsb_wal"], linewidth=3.5, linestyle='--', zorder=4)
    plt.fill_between(time_mins, ycsb_wal_rate, facecolor="none", 
                     edgecolor=STORAGE_COLORS["ycsb_wal"], hatch='\\\\\\\\', alpha=1.0, zorder=4)
    
    plt.yscale('symlog', base=10, linthresh=0.1) 
    
    plt.xlabel("Elapsed TPC-H Time (Minutes)")
    plt.ylabel("Write Rate (GB / min)")
    #plt.title(build_title(benchmark_run, "TPC-H Timeline: Smoothed Write Rate", version_label), pad=15)
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, f"{file_prefix}_tpch_shared_rate_timeline.png"), dpi=300)
    plt.close()

def plot_ycsb_timeseries(benchmark_run: parse.BenchmarkRun, out_dir: str, file_prefix: str, version_label: str = ""):
    os.makedirs(out_dir, exist_ok=True)
    data = benchmark_run.raw_series
    if not data: return

    offsets_min = [d["offset_s"] / 60.0 for d in data] 
    tputs = [d["throughput_ops"] for d in data]

    plt.figure(figsize=(12, 6))
    plt.grid(True, linestyle=":", alpha=0.6, zorder=0)
    
    # Plotted line (mapped to your global ycsb_db color for consistency)
    plt.plot(offsets_min, tputs, marker='o', markersize=4, linestyle='-', 
             color=STORAGE_COLORS.get("ycsb_db", "tab:purple"), zorder=3)
    
    # ---------------------------------------------------------
    # LOCK THE Y-AXIS TO 2.5
    # ---------------------------------------------------------
    plt.ylim(0, 2.5)
    
    plt.xlabel("Elapsed Time (Minutes)")
    plt.ylabel("Throughput (ops/s)")
    #plt.title(build_title(benchmark_run, "YCSB Throughput Timeline", version_label), pad=15)
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, f"{file_prefix}_throughput_timeline.png"), dpi=300)
    plt.close()

def plot_ycsb_data_throughput(benchmark_run: parse.BenchmarkRun, out_dir: str, file_prefix: str, version_label: str = ""):
    """Plots the YCSB physical data write throughput (MB/s) based on actual step duration."""
    os.makedirs(out_dir, exist_ok=True)
    if not benchmark_run.raw_series: return
    
    time_mins = []
    write_mbps = []
    
    prev_wal = 0
    prev_db = 0
    
    for i, item in enumerate(benchmark_run.raw_series):
        offset_s = item.get("offset_s", 0)
        
        # This is the actual time this specific step took to execute
        interval_ms = item.get("interval_ms", 1000)
        interval_s = interval_ms / 1000.0 if interval_ms > 0 else 1.0
        
        m = item.get("metrics", {})
        curr_wal = m.get("ycsb.total_wal_bytes", 0)
        curr_db = m.get("ycsb.total_db_bytes", 0)
        
        if i == 0:
            write_mbps.append(0)
        else:
            # 1. Calculate the cumulative byte growth since the last row
            delta_wal = max(0, curr_wal - prev_wal)
            delta_db = max(0, curr_db - prev_db)
            total_bytes_written = delta_wal + delta_db
            
            # 2. Divide by the actual step duration (interval_s)
            mb_per_sec = (total_bytes_written / (1024 * 1024)) / interval_s
            write_mbps.append(mb_per_sec)
            
        time_mins.append(offset_s / 60.0)
        
        # Update byte trackers for the next loop
        prev_wal = curr_wal
        prev_db = curr_db

    # Apply a moving average to smooth the line for better readability
    def smooth(y, box_pts=3):
        box = np.ones(box_pts) / box_pts
        return np.convolve(y, box, mode='same')
        
    write_mbps_smoothed = smooth(write_mbps, 3)

    plt.figure(figsize=(12, 6))
    plt.grid(True, linestyle=":", alpha=0.6, zorder=0)
    
    # Plot throughput using your global colors
    plt.plot(time_mins, write_mbps_smoothed, label="YCSB Data Throughput (MB/s)", 
             color=STORAGE_COLORS.get("ycsb_wal", "tab:orange"), linewidth=2, zorder=3)
             
    plt.fill_between(time_mins, write_mbps_smoothed, facecolor="none", 
                     edgecolor=STORAGE_COLORS.get("ycsb_wal", "tab:orange"), hatch='//', alpha=0.8, zorder=3)
    
    plt.xlabel("Elapsed Time (Minutes)")
    plt.ylabel("Data Write Throughput (MB/s)")
    #plt.title(build_title(benchmark_run, "YCSB Data Write Throughput (MB/s)", version_label), pad=15)
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, f"{file_prefix}_ycsb_data_throughput.png"), dpi=300)
    plt.close()

def plot_device_metrics(benchmark_run: parse.BenchmarkRun, out_dir: str, file_prefix: str, version_label: str = ""):
    os.makedirs(out_dir, exist_ok=True)
    data = benchmark_run.results.get("device", [])
    if not data: return
    filtered_data = [d for d in data if d.get("phase") == "post-drain"]
    if not filtered_data: return
    
    # Extract the new elapsed minutes for the X-axis
    time_mins = [d["elapsed_min"] for d in filtered_data]
    wafs = [d["waf"] if isinstance(d["waf"], (int, float)) else np.nan for d in filtered_data]
    
    plt.figure(figsize=(10, 5))
    plt.grid(True, linestyle=":", alpha=0.7, zorder=0)
    
    plt.plot(time_mins, wafs, marker="s", color="tab:cyan", linestyle="-", linewidth=2, zorder=3, label="Interval WAF")
    plt.axhline(1.0, color='gray', linestyle='--', alpha=0.8, linewidth=1.5, zorder=2, label="Ideal WAF (1.0)")
    
    plt.ylim(0.8, 1.5)
    
    # Updated X-axis label and removed the 45-degree rotation since it's just numbers now
    plt.xlabel("Elapsed Time (Minutes)")
    plt.ylabel("Write Amplification Factor (WAF)")
    #plt.title(build_title(benchmark_run, "Device WAF (Drained Phases Only)", version_label), pad=15)
    
    plt.legend(loc="upper right")
    plt.tight_layout()
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
            times = [safe_mean(run.results[t], scale=1000.0) for t in tasks]
            x_labels = [str(t) for t in tasks]
            
            # Pass custom hatch patterns if desired here, default is '//'
            make_bar_plot(x_labels, times, "Latency (s)", build_title(run, "Latency per Query", version_label), 
                          os.path.join(out_dir, f"{file_prefix}_latency.png"), 
                          color="tab:blue", hatch_pattern='//', y_max=600)
            
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
#!/bin/bash

# ==========================================
# Global Settings
# ==========================================
DEVICE="/dev/nvme0"
INPUT_DIR="/mnt/data/benchmark/"
THREADS=64
REPETITIONS=10

# WAF Drain
ENABLE_DRAIN=1
DRAIN_INTERVAL=1800    # seconds of active work between drains
DRAIN_DURATION=660    # seconds to wait at each drain
DRAIN_FINAL_DURATION=1800
DRAIN_ARGS=()
if [ "$ENABLE_DRAIN" -eq 1 ]; then
    DRAIN_ARGS=(--drain --drain-interval "$DRAIN_INTERVAL" --drain-duration "$DRAIN_DURATION" --drain-final-duration "$DRAIN_FINAL_DURATION")
fi

# Filler
ENABLE_FILLER=1
FILLER_ARGS=()
if [ "$ENABLE_FILLER" -eq 1 ]; then
    FILLER_ARGS=(--filler)
fi

# DuckDB / nvmefs
DUCKDB_PATH="$HOME/nvmefs2"
EXTENSION_PATH="$DUCKDB_PATH/build/release/extension/nvmefs/nvmefs.duckdb_extension"
VENV_DIR=".venv_v2_new"

# FDP Strategies
FDP_STRATEGIES=("baseline" "temp-isolated") # "wal-isolated" "fully-isolated")

# ==========================================
# Environment Setup
# ==========================================
echo "=========================================="
echo "Initializing v2_new environment"
echo "=========================================="

if [ -e "$VENV_DIR" ]; then
    echo "Activating existing environment $VENV_DIR..."
    source "$VENV_DIR/bin/activate"
else
    echo "Creating environment using Python 3.13..."
    python3.13 -m venv "$VENV_DIR"
    source "$VENV_DIR/bin/activate"
    pip3 install pybind11
    echo "Installing DuckDB 1.5.2 via pip..."
    pip3 install duckdb==1.5.2
fi

# ==========================================
# Workload Configurations
# ==========================================

# TPCH Configs: SF MEM_LIMIT WORKLOAD_SIZE_GB TEMP_SIZE_GB
CONFIGS=(
    # "1000 12000 385 90"
    "3000 38000 1135 255"
)

# Precondition
PRECOND_STATES=(0)
FIO_FILE="fio/uniform.fio"
SETTLE_SECONDS=900

echo "Starting TPCH Benchmarks"

SUITE_START_TIMESTAMP=$(date +%s)
SUITE_START_STR=$(date '+%Y-%m-%d %H:%M:%S')

# ==========================================
# TPC-H (Baseline No-FDP: Precond vs No-Precond)
# ==========================================
echo "Starting TPCH Benchmarks (Baseline)..."

for config in "${CONFIGS[@]}"; do
    read -r TPCH_SF MEM_LIMIT WORKLOAD_GB TEMP_SIZE <<< "$config"
    WORKLOAD_NS_SIZE=$(( WORKLOAD_GB * 1000 * 1000 * 1000 / 4096 ))

    for precond in "${PRECOND_STATES[@]}"; do
        
        if [ "$precond" -eq 1 ]; then
            LABEL="precond"
            PRECOND_ARGS=(--precondition --fio_file "$FIO_FILE" --settle_seconds "$SETTLE_SECONDS" --dsm_after_preconditioning)
        else
            LABEL="no-precond"
            PRECOND_ARGS=()
        fi

        echo "Running TPC-H | SF: ${TPCH_SF} | Mem: ${MEM_LIMIT}MB | Namespace: ${WORKLOAD_GB}GB | Temp Limit: ${TEMP_SIZE}"
        TIMESTAMP=$(date '+%Y%m%d_%H%M%S')
        RUN_ID="${LABEL}_tpch_sf${TPCH_SF}_mem${MEM_LIMIT}_size_${WORKLOAD_NS_SIZE}_${TIMESTAMP}"
        
        python3 -u benchmark.py tpch \
            --run_id "$RUN_ID" \
            --repetitions $REPETITIONS \
            --input_directory $INPUT_DIR \
            --device_path $DEVICE \
            --generic_device \
            --backend "io_uring_cmd" \
            --memory_limit $MEM_LIMIT \
            --tpch_sf $TPCH_SF \
            --threads $THREADS \
            --namespace_size $WORKLOAD_NS_SIZE \
            --max_temp_size $TEMP_SIZE \
            --extension_path "$EXTENSION_PATH" \
            "${FILLER_ARGS[@]}" \
            "${PRECOND_ARGS[@]}" \
            "${DRAIN_ARGS[@]}"
            
        sleep 1
    done
done

echo "Finished TPCH benchmark"
deactivate

# ==========================================
# Wrap-up
# ==========================================
SUITE_END_TIMESTAMP=$(date +%s)
ELAPSED=$(( SUITE_END_TIMESTAMP - SUITE_START_TIMESTAMP ))
printf "Total Elapsed Time: %02d:%02d:%02d\n" $((ELAPSED/3600)) $(( (ELAPSED%3600)/60 )) $((ELAPSED%60))
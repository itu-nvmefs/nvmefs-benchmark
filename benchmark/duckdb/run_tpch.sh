#!/bin/bash

# ==========================================
# Global Settings
# ==========================================
DEVICE="/dev/nvme0"
INPUT_DIR="/mnt/data/benchmark/"
THREADS=64
REPETITIONS=10

# DuckDB / nvmefs
DUCKDB_PATH="$HOME/nvmefs2"
EXTENSION_PATH="$DUCKDB_PATH/build/release/extension/nvmefs/nvmefs.duckdb_extension"
VENV_DIR=".venv_v2_new"

USE_MOUNT=0
BACKEND_ARGS=()
if [ "$USE_MOUNT" -eq 1 ]; then
    BACKEND_LABEL="mount"
    BACKEND_ARGS=(--mount)
else
    BACKEND_LABEL="nvmefs"
    BACKEND_ARGS=(--generic_device --backend "io_uring_cmd" --extension_path "$EXTENSION_PATH")
fi

# WAF Drain
ENABLE_DRAIN=0
DRAIN_INTERVAL=1800
DRAIN_DURATION=660
DRAIN_FINAL_DURATION=1800
DRAIN_ARGS=()
if [ "$ENABLE_DRAIN" -eq 1 ]; then
    DRAIN_ARGS=(--drain --drain-interval "$DRAIN_INTERVAL" --drain-duration "$DRAIN_DURATION" --drain-final-duration "$DRAIN_FINAL_DURATION")
fi

# Filler
ENABLE_FILLER=0
FILLER_ARGS=()
if [ "$ENABLE_FILLER" -eq 1 ]; then
    FILLER_ARGS=(--filler)
fi

# FDP strategies for TPC-H: name -> mapping string.
# Empty mapping means "FDP disabled" — we omit --fdp in that case.
# TPC-H is single-DB, so suffix-only keys are sufficient.
declare -A FDP_MAPPINGS=(
    ["nofdp"]=""
    ["baseline"]=".db:1,.wal:1,.tmp:1"
    ["temp-isolated"]=".db:1,.wal:1,.tmp:2"
    ["wal-isolated"]=".db:1,.wal:2,.tmp:1"
    ["fully-isolated"]=".db:1,.wal:2,.tmp:3"
)

# Which strategies to run this suite.
FDP_STRATEGIES=("nofdp" "baseline" "temp-isolated")

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
    echo "Installing DuckDB 1.5.2 via pip..."
    pip3 install duckdb==1.5.2
fi

# ==========================================
# Workload Configurations
# ==========================================

# TPCH Configs: SF MEM_LIMIT WORKLOAD_SIZE_GB TEMP_SIZE_GB
CONFIGS=(
    "1000 12000 385 90"
    # "3000 38000 1150 270"
)

PRECOND_STATES=(0)
FIO_FILE="fio/uniform.fio"
SETTLE_SECONDS=900

echo "Starting TPCH Benchmarks"

SUITE_START_TIMESTAMP=$(date +%s)
SUITE_START_STR=$(date '+%Y-%m-%d %H:%M:%S')

# ==========================================
# TPC-H Suite
# ==========================================
for config in "${CONFIGS[@]}"; do
    read -r TPCH_SF MEM_LIMIT WORKLOAD_GB TEMP_SIZE <<< "$config"
    WORKLOAD_NS_SIZE=$(( WORKLOAD_GB * 1000 * 1000 * 1000 / 4096 ))

    for precond in "${PRECOND_STATES[@]}"; do
        if [ "$precond" -eq 1 ]; then
            PRECOND_LABEL="precond"
            PRECOND_ARGS=(--precondition --fio_file "$FIO_FILE" --settle_seconds "$SETTLE_SECONDS" --dsm_after_preconditioning)
        else
            PRECOND_LABEL="no-precond"
            PRECOND_ARGS=()
        fi

        for strategy in "${FDP_STRATEGIES[@]}"; do
            MAPPING="${FDP_MAPPINGS[$strategy]}"
            FDP_ARGS=()
            if [ -n "$MAPPING" ]; then
                FDP_ARGS=(--fdp --fdp_mapping "$MAPPING")
            fi

            echo "Running TPC-H | SF: ${TPCH_SF} | Mem: ${MEM_LIMIT}MB | Namespace: ${WORKLOAD_GB}GB | Temp: ${TEMP_SIZE}GB | FDP: ${strategy}"
            TIMESTAMP=$(date '+%Y%m%d_%H%M%S')
            RUN_ID="${BACKEND_LABEL}_${PRECOND_LABEL}_tpch_sf${TPCH_SF}_mem${MEM_LIMIT}_size_${WORKLOAD_NS_SIZE}_fdp-${strategy}_${TIMESTAMP}"

            python3 -u benchmark.py tpch \
                --run_id "$RUN_ID" \
                --parallel 1 \
                --repetitions $REPETITIONS \
                --input_directory $INPUT_DIR \
                --device_path $DEVICE \
                --memory_limit "$MEM_LIMIT" \
                --tpch_sf $TPCH_SF \
                --threads $THREADS \
                --namespace_size $WORKLOAD_NS_SIZE \
                --max_temp_size $TEMP_SIZE \
                "${BACKEND_ARGS[@]}" \
                "${FILLER_ARGS[@]}" \
                "${PRECOND_ARGS[@]}" \
                "${DRAIN_ARGS[@]}" \
                "${FDP_ARGS[@]}"

            sleep 1
        done
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
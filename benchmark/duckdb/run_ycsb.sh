#!/bin/bash

# ==========================================
# Global Settings
# ==========================================
DEVICE="/dev/nvme0"
INPUT_DIR="/mnt/data/benchmark/"
THREADS=64

# WAF Drain
ENABLE_DRAIN=1
DRAIN_INTERVAL=1800        # seconds of active work between drains
DRAIN_DURATION=660         # seconds to wait at each drain
DRAIN_FINAL_DURATION=1800
DRAIN_ARGS=()
if [ "$ENABLE_DRAIN" -eq 1 ]; then
    DRAIN_ARGS=(--drain --drain-interval "$DRAIN_INTERVAL" --drain-duration "$DRAIN_DURATION" --drain-final-duration "$DRAIN_FINAL_DURATION")
fi

ENABLE_FILLER=1
FILLER_ARGS=()
if [ "$ENABLE_FILLER" -eq 1 ]; then
    FILLER_ARGS=(--filler)
fi

# DuckDB / nvmefs
DUCKDB_PATH="$HOME/nvmefs2"
EXTENSION_PATH="$DUCKDB_PATH/build/release/extension/nvmefs/nvmefs.duckdb_extension"
VENV_DIR=".venv_v2_new"
YCSB_ENGINE_PATH="runner/ycsb_lib/build_engine.sh"

# FDP Strategies 
FDP_STRATEGIES=("baseline") # "temp-isolated" "wal-isolated" "fully-isolated"

# ==========================================
# Environment Setup
# ==========================================
echo "=========================================="
echo "Initializing v2_new environment"
echo "=========================================="

if [ -e "$VENV_DIR" ]; then
    source "$VENV_DIR/bin/activate"
else
    python3.13 -m venv "$VENV_DIR"
    source "$VENV_DIR/bin/activate"
    pip3 install pybind11
    pip3 install duckdb==1.5.2
fi

echo "Building YCSB Engine..."
./$YCSB_ENGINE_PATH || { echo "Building failed. Aborting."; exit 1; }

# ==========================================
# Workload Configurations
# ==========================================

# YCSB Configs: SF MEM_LIMIT NAMESPACE_GB TEMP_GB DURATION_MIN
CONFIGS=(
    "200 45000 400 5 480"
)

CHECKPOINT_MODES=("auto") # manual
PRECOND_STATES=(0) # Change to 1 if u want precond

FIO_FILE="fio/uniform.fio"
SETTLE_SECONDS=900

SUITE_START_TIMESTAMP=$(date +%s)
SUITE_START_STR=$(date '+%Y-%m-%d %H:%M:%S')
echo "Starting YCSB Benchmarks at $SUITE_START_STR"

# ==========================================
# YCSB
# ==========================================
GENERATOR="runner/ycsb_lib/generate_ycsb_db.py"
declare -A SEEN_SFS
for config in "${CONFIGS[@]}"; do
    read -r YCSB_SF _ _ _ _ <<< "$config"
    if [ -z "${SEEN_SFS[$YCSB_SF]}" ]; then
        SEEN_SFS[$YCSB_SF]=1
        echo "Ensuring YCSB DB for SF=${YCSB_SF}..."
        python3 "$GENERATOR" --sf "$YCSB_SF" \
            || { echo "Generation failed for SF=${YCSB_SF}. Aborting."; exit 1; }
    fi
done

for config in "${CONFIGS[@]}"; do
    read -r YCSB_SF MEM_LIMIT WORKLOAD_GB TEMP_SIZE DURATION_MIN <<< "$config"
    WORKLOAD_NS_SIZE=$(( WORKLOAD_GB * 1000 * 1000 * 1000 / 4096 ))

    for precond in "${PRECOND_STATES[@]}"; do
        if [ "$precond" -eq 1 ]; then
            PRECOND_LABEL="precond"
            PRECOND_ARGS=(--precondition --fio_file "$FIO_FILE" --settle_seconds "$SETTLE_SECONDS" --dsm_after_preconditioning)
        else
            PRECOND_LABEL="no-precond"
            PRECOND_ARGS=()
        fi
    
     for ckpt in "${CHECKPOINT_MODES[@]}"; do
            for strategy in "${FDP_STRATEGIES[@]}"; do
                echo "Running YCSB | SF: ${YCSB_SF} | Mem: ${MEM_LIMIT}MB | Namespace: ${WORKLOAD_GB}GB | Duration: ${DURATION_MIN}min | Ckpt: ${ckpt} | Precond: ${PRECOND_LABEL} | FDP: ${strategy}"
                TIMESTAMP=$(date '+%Y%m%d_%H%M%S')
                RUN_ID="${PRECOND_LABEL}_ycsb_sf${YCSB_SF}_mem${MEM_LIMIT}_size_${WORKLOAD_NS_SIZE}_temp${TEMP_SIZE}_ckpt-${ckpt}_fdp-${strategy}_${TIMESTAMP}"

                python3 -u benchmark.py ycsb \
                    --run_id "$RUN_ID" \
                    --duration $DURATION_MIN \
                    --input_directory $INPUT_DIR \
                    --device_path $DEVICE \
                    --generic_device \
                    --backend "io_uring_cmd" \
                    --memory_limit $MEM_LIMIT \
                    --ycsb_sf $YCSB_SF \
                    --threads $THREADS \
                    --namespace_size $WORKLOAD_NS_SIZE \
                    --max_temp_size $TEMP_SIZE \
                    --checkpoint_mode "$ckpt" \
                    --extension_path "$EXTENSION_PATH" \
                    "${FILLER_ARGS[@]}" \
                    "${PRECOND_ARGS[@]}" \
                    "${DRAIN_ARGS[@]}"
                # Add `--fdp --fdp_strategy "$strategy"` once you turn FDP on.

                sleep 1
            done
        done
    done
done

echo "Finished YCSB benchmark"
deactivate

# ==========================================
# Wrap-up
# ==========================================
SUITE_END_TIMESTAMP=$(date +%s)
ELAPSED=$(( SUITE_END_TIMESTAMP - SUITE_START_TIMESTAMP ))
printf "Total Elapsed Time: %02d:%02d:%02d\n" $((ELAPSED/3600)) $(( (ELAPSED%3600)/60 )) $((ELAPSED%60))


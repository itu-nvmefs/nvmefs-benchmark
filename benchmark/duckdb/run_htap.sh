#!/bin/bash

# ==========================================
# Global Settings
# ==========================================
DEVICE="/dev/nvme0"
INPUT_DIR="/mnt/data/benchmark/"
THREADS=64

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

# Wal Skip Threshold
ENABLE_WAL_SKIP_THRESHOLD=0
WAL_SKIP_ARGS=()
if [ "$ENABLE_WAL_SKIP_THRESHOLD" -eq 1 ]; then
    WAL_SKIP_ARGS=(--wal_skip_threshold_bytes 107374182400)   # 100 GiB
fi

# Fragmentation Script
FRAGMENTATION_FILE="scripts/fragmentation.sh"

ENABLE_FILLER=0

# ==========================================
# HTAP FDP mappings — per-database keys so TPC-H and YCSB streams never collide
# ==========================================
declare -A FDP_MAPPINGS=(
    ["nofdp"]=""
    ["baseline"]="tpch.db:1,tpch.wal:1,ycsb.db:1,ycsb.wal:1,.tmp:1"
    ["temp-isolated"]="tpch.db:1,tpch.wal:1,ycsb.db:1,ycsb.wal:1,.tmp:2"
    ["wal-isolated"]="tpch.db:1,tpch.wal:2,ycsb.db:1,ycsb.wal:3,.tmp:1"
    ["db-isolated"]="tpch.db:1,tpch.wal:1,ycsb.db:2,ycsb.wal:2,.tmp:3"
    ["fully-isolated"]="tpch.db:1,tpch.wal:2,.tmp:3,ycsb.db:4,ycsb.wal:5"
)

FDP_STRATEGIES=("fully-isolated") # "fully-isolated"

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
    pip3 install duckdb==1.5.2
fi

# ==========================================
# Workload Configurations
# ==========================================
CONFIGS=(
    # TPCH_SF YCSB_SF TPCH_MEM YCSB_MEM TPCH_DB TPCH_TEMP YCSB_DB YCSB_TEMP DUR
    "3000     200      38000    45000    820     250       380     5        480"
)

CHECKPOINT_MODES=("auto")
PRECOND_STATES=(0)
SETTLE_SECONDS=900

SUITE_START_TIMESTAMP=$(date +%s)
SUITE_START_STR=$(date '+%Y-%m-%d %H:%M:%S')
echo "Starting HTAP Benchmarks at $SUITE_START_STR"

# ==========================================
# Ensure YCSB seed databases exist
# ==========================================
GENERATOR="runner/ycsb_lib/generate_ycsb_db.py"
declare -A SEEN_SFS
for config in "${CONFIGS[@]}"; do
    read -r _ YCSB_SF _ _ _ _ _ _ _ <<< "$config"
    if [ -z "${SEEN_SFS[$YCSB_SF]}" ]; then
        SEEN_SFS[$YCSB_SF]=1
        echo "Ensuring YCSB DB for SF=${YCSB_SF}..."
        python3 "$GENERATOR" --sf "$YCSB_SF" \
            || { echo "Generation failed for SF=${YCSB_SF}. Aborting."; exit 1; }
    fi
done

# ==========================================
# Block sizing constants
# ==========================================
BLOCK_SIZE=4096
BLOCKS_PER_MB=$(( 1024 * 1024 / BLOCK_SIZE ))
BLOCKS_PER_GB=$(( 1024 * 1024 * 1024 / BLOCK_SIZE ))
WAL_MB_PER_DB=32       # 32 MB/WAL the original assumed
SLACK_PCT=1            # +1% slack on TOTAL_BLOCKS

# ==========================================
# HTAP Suite
# ==========================================
for config in "${CONFIGS[@]}"; do
    read -r TPCH_SF YCSB_SF \
            TPCH_MEM_MB YCSB_MEM_MB \
            TPCH_DB_GB TPCH_TEMP_GB \
            YCSB_DB_GB YCSB_TEMP_GB \
            DURATION_MIN <<< "$config"

    TPCH_BASE_BLOCKS=$(( TPCH_DB_GB * BLOCKS_PER_GB \
                       + TPCH_TEMP_GB * BLOCKS_PER_GB \
                       + WAL_MB_PER_DB * BLOCKS_PER_MB ))
    TPCH_WORKLOAD_BLOCKS=$(( TPCH_BASE_BLOCKS + (TPCH_BASE_BLOCKS * SLACK_PCT / 100) ))

    YCSB_BASE_BLOCKS=$(( YCSB_DB_GB * BLOCKS_PER_GB \
                       + YCSB_TEMP_GB * BLOCKS_PER_GB \
                       + WAL_MB_PER_DB * BLOCKS_PER_MB ))
    YCSB_WORKLOAD_BLOCKS=$(( YCSB_BASE_BLOCKS + (YCSB_BASE_BLOCKS * SLACK_PCT / 100) ))

    DB_CONFIGS="tpch:${TPCH_DB_GB}GB,ycsb:${YCSB_DB_GB}GB"
    WORKLOAD_BLOCKS="tpch:${TPCH_WORKLOAD_BLOCKS},ycsb:${YCSB_WORKLOAD_BLOCKS}"
    TEMP_SIZES="tpch:${TPCH_TEMP_GB},ycsb:${YCSB_TEMP_GB}"
    MEM_LIMITS="tpch:${TPCH_MEM_MB},ycsb:${YCSB_MEM_MB}"

    if [ "$ENABLE_FILLER" -eq 1 ]; then
        NS_SIZES="tpch:${TPCH_WORKLOAD_BLOCKS}"
        FILLER_LABEL="filler"
    else
        NS_SIZES="tpch:${TPCH_WORKLOAD_BLOCKS},ycsb:${YCSB_WORKLOAD_BLOCKS}"
        FILLER_LABEL="nofiller"
    fi

    for precond in "${PRECOND_STATES[@]}"; do
        if [ "$precond" -eq 1 ]; then
            PRECOND_LABEL="precond"
            PRECOND_ARGS=(--precondition --settle_seconds "$SETTLE_SECONDS" --dsm_after_preconditioning)
        else
            PRECOND_LABEL="no-precond"
            PRECOND_ARGS=()
        fi

        for ckpt in "${CHECKPOINT_MODES[@]}"; do
            for strategy in "${FDP_STRATEGIES[@]}"; do
                MAPPING="${FDP_MAPPINGS[$strategy]}"
                FDP_ARGS=()
                if [ -n "$MAPPING" ]; then
                    FDP_ARGS=(--fdp --fdp_mapping "$MAPPING")
                fi

                echo "Running HTAP | TPCH_SF: ${TPCH_SF} | YCSB_SF: ${YCSB_SF} | tpch_mem: ${TPCH_MEM_MB}MB | ycsb_mem: ${YCSB_MEM_MB}MB | ${FILLER_LABEL} | Duration: ${DURATION_MIN}min | FDP: ${strategy}"
                TIMESTAMP=$(date '+%Y%m%d_%H%M%S')
                RUN_ID="run_${BACKEND_LABEL}_${PRECOND_LABEL}_${FILLER_LABEL}_htap_tsf${TPCH_SF}_ysf${YCSB_SF}_tmem${TPCH_MEM_MB}_ymem${YCSB_MEM_MB}_ckpt-${ckpt}_fdp-${strategy}_${TIMESTAMP}"

                python3 -u benchmark.py tpch,ycsb \
                    --run_id "$RUN_ID" \
                    --parallel 2 \
                    --duration $DURATION_MIN \
                    --input_directory $INPUT_DIR \
                    --device_path $DEVICE \
                    --memory_limit "$MEM_LIMITS" \
                    --tpch_sf $TPCH_SF \
                    --ycsb_sf $YCSB_SF \
                    --threads $THREADS \
                    --db_configs "$DB_CONFIGS" \
                    --ns_sizes "$NS_SIZES" \
                    --workload_blocks "$WORKLOAD_BLOCKS" \
                    --temp_sizes "$TEMP_SIZES" \
                    --checkpoint_mode "$ckpt" \
                    --frag_script_path $FRAGMENTATION_FILE \
                    "${BACKEND_ARGS[@]}" \
                    "${PRECOND_ARGS[@]}" \
                    "${DRAIN_ARGS[@]}" \
                    "${FDP_ARGS[@]}" 

                sleep 1
            done
        done
    done
done

echo "Finished HTAP benchmark"
deactivate

# ==========================================
# Wrap-up
# ==========================================
SUITE_END_TIMESTAMP=$(date +%s)
ELAPSED=$(( SUITE_END_TIMESTAMP - SUITE_START_TIMESTAMP ))
printf "Total Elapsed Time: %02d:%02d:%02d\n" $((ELAPSED/3600)) $(( (ELAPSED%3600)/60 )) $((ELAPSED%60))
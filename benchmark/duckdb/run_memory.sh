#!/bin/bash

# ==========================================
# Global Settings
# ==========================================
REPETITIONS=5
YCSB_DURATION=1
HTAP_DURATION=1

DEVICE="/dev/nvme0"
INPUT_DIR="/mnt/data/benchmark/"

# Max device blocks ~458984375 (1.88 TB / 4096)
M_SIZE_PRECONDITION=413085937 # ~1.69TB, 90% of device
HTAP_NS_SIZE=$(( M_SIZE_PRECONDITION / 2 ))
THREADS=16
CHECKPOINT_MODE="auto"

# Scale Factors
TPCH_SF=1000
YCSB_SF=100
HTAP_SF=10

# Memory Limits
TPCH_MEM_LIMITS=(10000 14000 18000 20000) 
YCSB_MEM_LIMITS=(20000)
HTAP_MEM_LIMIT=20000

YCSB_ENGINE_PATH="runner/ycsb_lib/build_engine.sh"

# ==========================================
# Initialization
# ==========================================
echo "Building YCSB Engine..."
./$YCSB_ENGINE_PATH || { echo "Building failed. Aborting."; exit 1; }
echo "YCSB Engine built successfully."

SUITE_START_TIMESTAMP=$(date +%s)
SUITE_START_STR=$(date '+%Y-%m-%d %H:%M:%S')

# ==========================================
# TPC-H 
# ==========================================

echo "Starting TPCH Benchmarks..."

for mem in "${TPCH_MEM_LIMITS[@]}"; do
    echo "Running TPC-H No-FDP - Memory Limit: ${mem}MB"
    python3 benchmark.py tpch \
        --repetitions $REPETITIONS \
        --input_directory $INPUT_DIR \
        --device_path $DEVICE \
        --generic_device \
        --backend "io_uring_cmd" \
        --memory_limit $mem \
        --sf $TPCH_SF \
        --threads $THREADS \
        --namespace_size $M_SIZE_PRECONDITION
done

echo "Finished TPCH benchmark"

# ==========================================
# 2. YCSB 
# ==========================================
: '

echo "Starting YCSB Benchmarks..."

for mem in "${YCSB_MEM_LIMITS[@]}"; do
    echo "Running YCSB No-FDP - Memory Limit: ${mem}MB"
    python3 benchmark.py ycsb \
        --duration $YCSB_DURATION \
        --input_directory $INPUT_DIR \
        --device_path $DEVICE \
        --generic_device \
        --backend "io_uring_cmd" \
        --memory_limit $mem \
        --sf $YCSB_SF \
        --threads $THREADS \
        --namespace_size $M_SIZE_PRECONDITION \
        --checkpoint_mode $CHECKPOINT_MODE
done

echo "Finished YCSB benchmark"

# ==========================================
# 3. HTAP 
# ==========================================
echo "Starting HTAP Benchmarks..."

echo "Running HTAP No-FDP - Memory Limit: ${HTAP_MEM_LIMIT}MB"
python3 benchmark.py htap \
    --duration $HTAP_DURATION \
    --input_directory $INPUT_DIR \
    --device_path $DEVICE \
    --generic_device \
    --backend "io_uring_cmd" \
    --memory_limit $HTAP_MEM_LIMIT \
    --sf $HTAP_SF \
    --threads $THREADS \
    --namespace_size $HTAP_NS_SIZE \
    --checkpoint_mode $CHECKPOINT_MODE

echo "Finished HTAP benchmark"

' 
# ==========================================
# Wrap-up
# ==========================================
SUITE_END_TIMESTAMP=$(date +%s)
SUITE_END_STR=$(date '+%Y-%m-%d %H:%M:%S')
ELAPSED=$(( SUITE_END_TIMESTAMP - SUITE_START_TIMESTAMP ))

HOURS=$(( ELAPSED / 3600 ))
MINUTES=$(( (ELAPSED % 3600) / 60 ))
SECS=$(( ELAPSED % 60 ))

echo "Benchmark Started at: $SUITE_START_STR; Benchmark Ended at: $SUITE_END_STR"
printf "Total Elapsed Time: %02d:%02d:%02d\n" $HOURS $MINUTES $SECS
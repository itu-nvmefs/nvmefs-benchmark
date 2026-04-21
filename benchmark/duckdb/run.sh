#!/bin/bash
DURATION=60
REPETITIONS=6
MEMORY_LIMIT=40000

DEVICE="/dev/nvme0"
INPUT_DIR="/mnt/data/benchmark/"

TPCH_SIZES=(1000)
YCSB_SIZES=(100) # SF10 = 1M Rows
YCSB_THREADS=16
YCSB_ENGINE_PATH="runner/ycsb_lib/build_engine.sh"

# Max device blocks ~458984375 (1.88 TB / 4096)
M_SIZE_PRECONDITION=413085937 # ~1.69TB, 90% of device
L_SIZE_PRECONDITION=380957031 # ~1.56TB, 83% of device
XL_SIZE_PRECONDITION=252441406 # ~1.03TB, 55% of device

FDP_STRATEGIES=("baseline" "temp-isolated" "wal-isolated" "fully-isolated")

# source ./init.sh

SUITE_START_TIMESTAMP=$(date +%s)
SUITE_START_STR=$(date '+%Y-%m-%d %H:%M:%S')


###################################
# YCSB
###################################

echo "Building YCSB Engine..."
./$YCSB_ENGINE_PATH || { echo "Building failed. Aborting."; exit 1; }
echo "YCSB Engine built successfully."

echo "Starting YCSB Benchmarks..."

# NVMe io_uring_cmd with FDP enabled (all 4 strategies)
for strategy in "${FDP_STRATEGIES[@]}"; do
    for sf in "${YCSB_SIZES[@]}"; do
        echo "Running YCSB FDP ($strategy) - Scale Factor: $sf"
        python3 benchmark.py ycsb --duration $DURATION --input_directory $INPUT_DIR --device_path $DEVICE --generic_device --backend "io_uring_cmd" --memory_limit $MEMORY_LIMIT --sf $sf --threads $YCSB_THREADS --fdp --fdp_strategy $strategy --namespace_size $M_SIZE_PRECONDITION --precondition
    done
done

# NVMe io_uring_cmd without FDP
for sf in "${YCSB_SIZES[@]}"; do
    echo "Running YCSB No-FDP - Scale Factor: $sf"
    python3 benchmark.py ycsb --duration $DURATION --input_directory $INPUT_DIR --device_path $DEVICE --generic_device --backend "io_uring_cmd" --memory_limit $MEMORY_LIMIT --sf $sf --threads $YCSB_THREADS --namespace_size $M_SIZE_PRECONDITION --precondition
done

#Baseline 
for sf in "${YCSB_SIZES[@]}"; do
    echo "Running YCSB Baseline - Scale Factor: $sf"
    python3 benchmark.py ycsb --duration $DURATION --mount --device_path $DEVICE --input_directory $INPUT_DIR --memory_limit $MEMORY_LIMIT --sf $sf --threads $YCSB_THREADS --namespace_size $M_SIZE_PRECONDITION --precondition
done

echo "Finished YCSB benchmark"

###################################
# TPCH
###################################

# NVMe io_uring_cmd with FDP enabled (all 4 strategies)
for strategy in "${FDP_STRATEGIES[@]}"; do
    for sf in "${TPCH_SIZES[@]}"; do
        echo "Running TPCH FDP ($strategy) - Scale Factor: $sf"
        python3 benchmark.py tpch --repetitions $REPETITIONS --input_directory $INPUT_DIR --device_path $DEVICE --generic_device --backend "io_uring_cmd" --memory_limit $MEMORY_LIMIT --sf $sf --threads 16 --fdp --fdp_strategy $strategy --namespace_size $M_SIZE_PRECONDITION --precondition
    done
done

# NVMe io_uring_cmd without FDP
for sf in "${TPCH_SIZES[@]}"; do
    echo "Running TPCH No-FDP - Scale Factor: $sf"
    python3 benchmark.py tpch --repetitions $REPETITIONS --input_directory $INPUT_DIR --device_path $DEVICE --generic_device --backend "io_uring_cmd" --memory_limit $MEMORY_LIMIT --sf $sf --threads 16 --namespace_size $M_SIZE_PRECONDITION --precondition
done

# Baseline
for sf in "${TPCH_SIZES[@]}"; do
    echo "Running TPCH Baseline - Scale Factor: $sf"
    python3 benchmark.py tpch --repetitions $REPETITIONS --mount --device_path $DEVICE --input_directory $INPUT_DIR --memory_limit $MEMORY_LIMIT --sf $sf --threads 16 --namespace_size $M_SIZE_PRECONDITION --precondition
done

echo "Finished TPCH benchmark"

SUITE_END_TIMESTAMP=$(date +%s)
SUITE_END_STR=$(date '+%Y-%m-%d %H:%M:%S')
ELAPSED=$(( SUITE_END_TIMESTAMP - SUITE_START_TIMESTAMP ))

HOURS=$(( ELAPSED / 3600 ))
MINUTES=$(( (ELAPSED % 3600) / 60 ))
SECS=$(( ELAPSED % 60 ))

echo "Benchmark Started at: $SUITE_START_STR; Benchmark Ended at: $SUITE_END_STR"
printf "Total Elapsed Time: %02d:%02d:%02d\n" $HOURS $MINUTES $SECS

: '
###################################
# Run all out-of-core benchmarks with focus on the individual elasped times:wq
#
###################################
OOCHA_SIZES=(2 8 32 128)

for sf in "${OOCHA_SIZES[@]}"
do
    setup_precondition_ns_fdp $DEVICE $M_SIZE_PRECONDITION
    python3 benchmark.py -r $REPETITIONS --input_directory $INPUT_DIR --device_path $DEVICE --generic_device -b "io_uring_cmd" -m 20000 --sf $sf -t 16 --fdp oocha
    remove_precondition_device $DEVICE $M_SIZE_PRECONDITION
done

for sf in "${OOCHA_SIZES[@]}"
do
    setup_precondition_ns $DEVICE $M_SIZE_PRECONDITION
    python3 benchmark.py -r $REPETITIONS --input_directory $INPUT_DIR --device_path $DEVICE --generic_device -b "io_uring_cmd" -m 20000 --sf $sf -t 16 oocha
    remove_precondition_device $DEVICE $M_SIZE_PRECONDITION
done

# Base line for the out-of-core elapsed benchmark
for sf in "${OOCHA_SIZES[@]}"
do
    setup_precondition_ns $DEVICE $M_SIZE_PRECONDITION
    python3 benchmark.py -r $REPETITIONS --mount --device_path $DEVICE --input_directory $INPUT_DIR -m 20000 --sf $sf -t 16 oocha
    remove_precondition_device $DEVICE $M_SIZE_PRECONDITION
done

###################################
# Run all out-of-core benchmarks with focus on WAF
###################################

## normal
precondition_device $DEVICE $M_SIZE_PRECONDITION
precondition_device
python3 benchmark.py -d $DURATION --mount --device_path $DEVICE --input_directory $INPUT_DIR -m 20000 --sf 1000 -t 24 oocha-spill
remove_precondition_device $DEVICE $M_SIZE_PRECONDITION


precondition_device $DEVICE $M_SIZE_PRECONDITION
precondition_device
python3 benchmark.py -d $DURATION --mount --device_path $DEVICE --input_directory $INPUT_DIR -m 40000 --sf 1000 -t 96 -par 4 oocha-spill
remove_precondition_device $DEVICE $M_SIZE_PRECONDITION

# nvme
setup_precondition_ns_fdp $DEVICE $M_SIZE_PRECONDITION
precondition_device
python3 benchmark.py -d $DURATION --input_directory $INPUT_DIR --device_path $DEVICE --generic_device -b "io_uring_cmd" -m 20000 --sf 1000 -t 24 --fdp oocha-spill
remove_precondition_device $DEVICE $M_SIZE_PRECONDITION

setup_precondition_ns_fdp $DEVICE $M_SIZE_PRECONDITION
precondition_device
python3 benchmark.py -d $DURATION --input_directory $INPUT_DIR --device_path $DEVICE --generic_device -b "io_uring_cmd" -m 14000 --sf 1000 -t 96 -par 4 --fdp oocha-spill
remove_precondition_device $DEVICE $M_SIZE_PRECONDITION

precondition_device $DEVICE $M_SIZE_PRECONDITION
precondition_device
python3 benchmark.py -d $DURATION --input_directory $INPUT_DIR --device_path $DEVICE --generic_device -b "io_uring_cmd" -m 3500 --sf 1000 -t 24 oocha-spill
remove_precondition_device $DEVICE $M_SIZE_PRECONDITION

setup_precondition_ns_fdp $DEVICE $M_SIZE_PRECONDITION
precondition_device
python3 benchmark.py -d $DURATION --input_directory $INPUT_DIR --device_path $DEVICE --generic_device -b "io_uring_cmd" -m 14000 --sf 1000 -t 96 -par 4 oocha-spill
remove_precondition_device $DEVICE $M_SIZE_PRECONDITION
'

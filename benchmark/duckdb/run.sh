#!/bin/bash
DURATION=240
REPETITIONS=6
DEVICE="/dev/nvme0"
# TODO: The input dir and mount needs to updated!
INPUT_DIR="/mnt/data"
MOUNT="/mnt/itu/duckdb"

# Max device blocks ~458984375 (1.88 TB / 4096)
M_SIZE_PRECONDITION=413085937 # ~1.69TB, 90% of device
L_SIZE_PRECONDITION=380957031 # ~1.56TB, 83% of device
XL_SIZE_PRECONDITION=252441406 # ~1.03TB, 55% of device

FDP_STRATEGIES=("baseline" "temp-isolated" "wal-isolated" "fully-isolated")

source ./init.sh

###################################
# TPCH
###################################
TPCH_SIZES=(1 10 100 1000)

# NVMe io_uring_cmd with FDP enabled (all 4 strategies)
for strategy in "${FDP_STRATEGIES[@]}"; do
    for sf in "${TPCH_SIZES[@]}"; do
        echo "Running TPCH FDP ($strategy) - Scale Factor: $sf"
        python3 benchmark.py tpch --repetitions $REPETITIONS --input_directory $INPUT_DIR --device_path $DEVICE --generic_device --backend "io_uring_cmd" --memory_limit 20000 --sf $sf --threads 16 --fdp --fdp_strategy $strategy --namespace_size $XL_SIZE_PRECONDITION --precondition
    done
done

# NVMe io_uring_cmd without FDP
for sf in "${TPCH_SIZES[@]}"; do
    echo "Running TPCH No-FDP - Scale Factor: $sf"
    python3 benchmark.py tpch --repetitions $REPETITIONS --input_directory $INPUT_DIR --device_path $DEVICE --generic_device --backend "io_uring_cmd" --memory_limit 20000 --sf $sf --threads 16 --namespace_size $XL_SIZE_PRECONDITION --precondition
done

# Baseline
for sf in "${TPCH_SIZES[@]}"; do
    echo "Running TPCH Baseline - Scale Factor: $sf"
    python3 benchmark.py tpch --repetitions $REPETITIONS --mount_path $MOUNT --device_path $DEVICE --input_directory $INPUT_DIR --memory_limit 20000 --sf $sf --threads 16 --namespace_size $XL_SIZE_PRECONDITION --precondition
done

: '
###################################
# Run all out-of-core benchmarks with focus on the individual elasped times
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
    python3 benchmark.py -r $REPETITIONS --mount_path $MOUNT --device_path $DEVICE --input_directory $INPUT_DIR -m 20000 --sf $sf -t 16 oocha
    remove_precondition_device $DEVICE $M_SIZE_PRECONDITION
done

###################################
# Run all out-of-core benchmarks with focus on WAF
###################################

## normal
precondition_device $DEVICE $M_SIZE_PRECONDITION
precondition_device
python3 benchmark.py -d $DURATION --mount_path $MOUNT --device_path $DEVICE --input_directory $INPUT_DIR -m 20000 --sf 1000 -t 24 oocha-spill
remove_precondition_device $DEVICE $M_SIZE_PRECONDITION


precondition_device $DEVICE $M_SIZE_PRECONDITION
precondition_device
python3 benchmark.py -d $DURATION --mount_path $MOUNT --device_path $DEVICE --input_directory $INPUT_DIR -m 40000 --sf 1000 -t 96 -par 4 oocha-spill
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
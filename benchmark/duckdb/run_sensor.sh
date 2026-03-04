#!/bin/bash

# Define the absolute path to your venv
VENV_PATH="$(pwd)/.venv/bin/python3"

# Configuration
DURATION=60          # How long to run (seconds)
PARALLEL=20          # Number of concurrent sensor threads
SCALE_FACTOR=1       # Batch size multiplier (1 batch = 100 ops)
DEVICE="/dev/ng0n1"  # Char device
BACKEND="io_uring"   # Backend
INPUT_DIR="./"       # Directory for data (ignored by sensor, but required by arg parser)

echo "----------------------------------------------------------------"
echo "Running SENSOR Benchmark"
echo "Device:   $DEVICE"
echo "FDP:      DISABLED"
echo "Duration: $DURATION s"
echo "Clients:  $PARALLEL"
echo "----------------------------------------------------------------"

# Run Command
sudo PYTHONPATH="" "$VENV_PATH" benchmark.py sensor \
    --duration $DURATION \
    --device_path $DEVICE \
    --backend $BACKEND \
    --parallel $PARALLEL \
    --sf $SCALE_FACTOR \
    --input_directory $INPUT_DIR \
    --threads 1 \
    --memory_limit 2000

echo "----------------------------------------------------------------"
echo "Benchmark Finished"
echo "----------------------------------------------------------------"
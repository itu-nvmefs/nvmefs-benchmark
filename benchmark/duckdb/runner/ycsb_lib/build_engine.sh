#!/bin/bash
set -e

# 1. Get the directory where this script is located
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)

# 2. Path to the DuckDB source and build
DUCKDB_SOURCE_DIR=$(realpath "$SCRIPT_DIR/../../../../../nvmefs/duckdb")
DUCKDB_BUILD_DIR=$(realpath "$SCRIPT_DIR/../../../../../nvmefs/build/release")

# 3. Define the specific Include and Library paths
DUCKDB_INC="$DUCKDB_SOURCE_DIR/src/include"
DUCKDB_LIB_PATH="$DUCKDB_BUILD_DIR/src"

echo "Using DuckDB Source: $DUCKDB_INC"
echo "Using DuckDB Library: $DUCKDB_LIB_PATH"

# 4. Define compilation details
# SUFFIX=$(python3-config --extension-suffix)
# OUTPUT_NAME="$SCRIPT_DIR/ycsb_engine${SUFFIX}"
OUTPUT_NAME="$SCRIPT_DIR/ycsb_engine.so"

# 5. Compile
g++ -O3 -shared -std=c++11 -fPIC \
    $(python3 -m pybind11 --includes) \
    -I"$DUCKDB_INC" \
    -I"$SCRIPT_DIR" \
    "$SCRIPT_DIR/ycsb_engine.cpp" \
    -L"$DUCKDB_LIB_PATH" \
    -lduckdb \
    -Wl,-rpath,"$DUCKDB_LIB_PATH" \
    -o "$OUTPUT_NAME"

echo "---------------------------------------"
echo "Success! Created: $OUTPUT_NAME"

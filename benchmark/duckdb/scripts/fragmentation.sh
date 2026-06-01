#!/usr/bin/env bash

TARGET_DIR="${1:-.}"

if [ ! -d "$TARGET_DIR" ]; then
    echo "Error: '$TARGET_DIR' is not a valid directory." >&2
    exit 1
fi

# Use 'find' to recursively get all files, avoiding symlinks and directories
find "$TARGET_DIR" -type f | while read -r file; do
    echo "File: $file"
    filefrag -v "$file"
    echo "----------------------------------------------------------------------"
done
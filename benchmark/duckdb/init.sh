#!/bin/bash

init_environment() {
    if [ -e ".venv" ]; then
        source .venv/bin/activate
        echo "Activating environment..."
    else
        echo "Creating environment and installing dependencies..."
        python3 -m venv .venv
        source .venv/bin/activate
        pip3 install ~/nvmefs/duckdb/tools/pythonpkg
        pip3 install -r requirements.txt
    fi
}

init_environment
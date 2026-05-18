#!/bin/bash

python3 plot_benchmarks.py ../results/tpch ./plots
python3 plot_benchmarks.py ../results/htap ./plots
python3 plot_benchmarks.py ../results/ycsb ./plots
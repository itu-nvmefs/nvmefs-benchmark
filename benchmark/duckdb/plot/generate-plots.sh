#!/bin/bash

#HTAP
# python3 plot_htap.py ../results/mount_precond_htap_tsf3000_ysf200_mem70000_ns1619_ckpt-auto_fdp-nofdp_20260517_232629 ./plots/htap/baseline
# python3 plot_htap.py ../results/nvmefs_precond_htap_tsf3000_ysf200_mem70000_ns1486_ckpt-auto_fdp-nofdp_20260515_202910 ./plots/htap/nofdp
# python3 plot_htap.py ../results/nvmefs_precond_htap_tsf3000_ysf200_mem70000_ns1517_ckpt-auto_fdp-fully-isolated_20260517_091819 ./plots/htap/fully-isolated

#TPC-H set latency:1500, set storage: 2500
# python3 plot_htap.py ../results/run_nvmefs_precond_tpch_sf3000_mem38000_size_304611328_fdp-nofdp_20260519_042239 ./plots/
# python3 plot_htap.py ../results/run_nvmefs_precond_tpch_sf3000_mem38000_size_304611328_fdp-temp-isolated_20260519_153048 ./plots/

#YCSB
python3 plot_htap.py ../results/run_nvmefs_precond_ycsb_sf200_mem45000_size_105914449_ckpt-auto_fdp-nofdp_20260520_065422 ./plots/
python3 plot_htap.py ../results/run_nvmefs_precond_ycsb_sf200_mem45000_size_105914449_ckpt-auto_fdp-wal-isolated_20260520_183915 ./plots/
      



#python3 plot_benchmarks.py ../results/no-precond_tpch_sf3000_mem38000_size_277099609_20260509_230349/tpch ./plots
#python3 plot_benchmarks.py ../results/no-precond_tpch_sf3000_mem38000_size_277099609_20260511_154721/tpch ./plots

#python3 plot_benchmarks.py ../results/no-precond_tpch_sf3000_mem38000_20260509_042743/tpch ./plots
# python3 plot_benchmarks.py ../results/htap ./plots
# python3 plot_benchmarks.py ../results/ycsb ./plots


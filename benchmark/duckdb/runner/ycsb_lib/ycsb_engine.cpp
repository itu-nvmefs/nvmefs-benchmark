#include "duckdb.hpp"
#include <chrono>
#include <pybind11/pybind11.h>
#include <string>

namespace py = pybind11;

double run_native_ycsb(std::string db_path, std::string dev_path,
                       std::string backend, std::string fdp_map, int iterations,
                       int row_count) {
  duckdb::DBConfig config;
  config.SetOption("allow_unsigned_extensions", true);

  // 1. Start in-memory to load the extension first
  duckdb::DuckDB db(nullptr, &config);
  duckdb::Connection conn(db);

  // 2. Load the nvmefs extension
  std::string ext_path =
      "/home/itu/nvmefs/build/release/extension/nvmefs/nvmefs.duckdb_extension";
  auto load_res = conn.Query("LOAD '" + ext_path + "';");
  if (load_res->HasError())
    throw std::runtime_error("Extension Load Failed: " + load_res->GetError());

  // 3. Setup the hardware secret
  std::string secret = "CREATE OR REPLACE PERSISTENT SECRET nvmefs (TYPE "
                       "NVMEFS, nvme_device_path '" +
                       dev_path + "', backend '" + backend + "'";
  if (!fdp_map.empty())
    secret += ", fdp_mapping '" + fdp_map + "'";
  secret += ", meta 'use_default_async|no_memory_manager');";

  auto sec_res = conn.Query(secret);
  if (sec_res->HasError())
    throw std::runtime_error("Secret Setup Failed: " + sec_res->GetError());

  // 4. Attach the NVMe database
  auto attach_res =
      conn.Query("ATTACH DATABASE '" + db_path + "' AS ycsb_db (READ_WRITE);");
  if (attach_res->HasError())
    throw std::runtime_error("Database Attach Failed: " +
                             attach_res->GetError());
  conn.Query("USE ycsb_db;");

  // --- START YCSB LOOP ---
  auto start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < iterations; i++) {
    int key = rand() % row_count;
    std::string s_key = "user" + std::to_string(key);
    if (i % 2 == 0) {
      conn.Query("SELECT FIELD0 FROM usertable WHERE YCSB_KEY = '" + s_key +
                 "';");
    } else {
      conn.Query("UPDATE usertable SET FIELD0 = 'updated' WHERE YCSB_KEY = '" +
                 s_key + "';");
    }
  }
  auto end = std::chrono::high_resolution_clock::now();
  // --- END YCSB LOOP ---

  return std::chrono::duration<double, std::milli>(end - start).count();
}

PYBIND11_MODULE(ycsb_engine, m) {
  m.def("run_native_ycsb", &run_native_ycsb, "Run the native YCSB loop");
}

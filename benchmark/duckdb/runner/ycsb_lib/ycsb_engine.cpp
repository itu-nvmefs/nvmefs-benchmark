#include "duckdb.hpp"
#include <chrono>
#include <memory>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <string>

namespace py = pybind11;

class YCSBRunner {
private:
  std::unique_ptr<duckdb::DuckDB> db;
  std::unique_ptr<duckdb::Connection> conn;

public:
  YCSBRunner(const std::string db_path, const std::string dev_path,
             const std::string backend, const std::string fdp_map,
             bool use_nvmefs, int memory_limit_mb, const std::string checkpoint_mode) {

    duckdb::DBConfig config;
    config.SetOption("allow_unsigned_extensions", true);

    db = std::make_unique<duckdb::DuckDB>(nullptr, &config);
    conn = std::make_unique<duckdb::Connection>(*db);

    if (use_nvmefs) {
      std::string ext_path = "/home/itu/nvmefs/build/release/extension/nvmefs/"
                             "nvmefs.duckdb_extension";
      auto load_res = conn->Query("LOAD '" + ext_path + "';");
      if (load_res->HasError())
        throw std::runtime_error("Extension Load Failed: " +
                                 load_res->GetError());
      
      std::string secret = "CREATE OR REPLACE PERSISTENT SECRET nvmefs (TYPE "
                           "NVMEFS, nvme_device_path '" +
                           dev_path + "', backend '" + backend + "'";
      if (!fdp_map.empty())
        secret += ", fdp_mapping '" + fdp_map + "'";
      secret += ", meta 'use_default_async|no_memory_manager');";

      auto sec_res = conn->Query(secret);
      if (sec_res->HasError())
        throw std::runtime_error("Secret Setup Failed: " + sec_res->GetError());
    }

    conn->Query("PRAGMA memory_limit='" + std::to_string(memory_limit_mb) + "MB';");
    if (checkpoint_mode == "manual") {
      conn->Query("PRAGMA wal_autocheckpoint='1TB';");
    } else {
      conn->Query("PRAGMA wal_autocheckpoint='16MB';");
    }

    auto attach_res = conn->Query("ATTACH DATABASE '" + db_path +
                                  "' AS ycsb_db (READ_WRITE);");
    if (attach_res->HasError())
      throw std::runtime_error("Database Attach Failed: " +
                               attach_res->GetError());

    conn->Query("USE ycsb_db;");
  }

  std::pair<double, int> run(int iterations, int row_count, int duration_seconds) {
    py::gil_scoped_release release;
    auto start = std::chrono::high_resolution_clock::now();

    int actual_iterations = 0;

    for (int i = 0; i < iterations; i++) {
      int key = rand() % row_count;
      std::string s_key = "user" + std::to_string(key);
      if (i % 2 == 0) {
        conn->Query("SELECT FIELD0 FROM usertable WHERE YCSB_KEY = '" + s_key +
                    "';");
      } else {
        conn->Query(
            "UPDATE usertable SET FIELD0 = 'updated' WHERE YCSB_KEY = '" +
            s_key + "';");
      }

      actual_iterations++;

      // Only check the clock every 10,000 operations to avoid busy wait
      if (duration_seconds > 0 && (i % 10000 == 0) && i > 0) {
          auto current = std::chrono::high_resolution_clock::now();
          auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(current - start).count();

          if (elapsed >= duration_seconds) {
              break; 
          }
      }
    }
    auto end = std::chrono::high_resolution_clock::now();
    double time_ms = std::chrono::duration<double, std::milli>(end - start).count();
    
    return std::make_pair(time_ms, actual_iterations);
  }

  std::map<std::string, int64_t> get_metrics() {
    std::map<std::string, int64_t> metrics;
    auto res = conn->Query("SELECT * FROM print_nvmefs_metrics();");
    
    if (!res->HasError()) {
      for (size_t r = 0; r < res->RowCount(); r++) {
        std::string key = res->GetValue(0, r).ToString();
        auto val = res->GetValue(1, r);
        metrics[key] = val.IsNull() ? 0 : val.GetValue<int64_t>();
      }
    }
    return metrics;
  }
};

// Bind the class to Python
PYBIND11_MODULE(ycsb_engine, m) {
  py::class_<YCSBRunner>(m, "YCSBRunner")
      .def(py::init<const std::string &, const std::string &, const std::string &, const std::string &, bool, int, const std::string &>())
      .def("run", &YCSBRunner::run)
      .def("get_metrics", &YCSBRunner::get_metrics); // Bind new method
}

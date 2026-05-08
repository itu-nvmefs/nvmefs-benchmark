#include "duckdb.hpp"
#include <chrono>
#include <memory>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/functional.h>
#include <random>
#include <string>
#include <map>

namespace py = pybind11;

// Generate a random alphanumeric string of given length using RNG
static std::string random_value(std::mt19937 &rng, int length) {
  static const char charset[] =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  static const int charset_size = sizeof(charset) - 1;
  std::uniform_int_distribution<int> dist(0, charset_size - 1);

  std::string s;
  s.reserve(length);
  for (int i = 0; i < length; i++) {
    s += charset[dist(rng)];
  }
  return s;
}

class YCSBRunner {
private:
  std::unique_ptr<duckdb::DuckDB> db;
  std::unique_ptr<duckdb::Connection> conn;
  std::unique_ptr<duckdb::PreparedStatement> read_stmt;
  std::unique_ptr<duckdb::PreparedStatement> update_stmt;
  int num_fields;
  int field_length;
  bool use_nvmefs;

  std::atomic<bool> drain_requested{false};
  std::atomic<bool> drain_paused{false};
  std::atomic<bool> drain_release{false};

  std::map<std::string, int64_t> sample_metrics() {
    std::map<std::string, int64_t> metrics;
    if (!use_nvmefs) return metrics;

    auto res = conn->Query("SELECT * FROM print_nvmefs_metrics();");
    if (res->HasError()) return metrics;
    for (size_t r = 0; r < res->RowCount(); r++) {
      std::string key = res->GetValue(0, r).ToString();
      auto val = res->GetValue(1, r);
      metrics[key] = val.IsNull() ? 0 : val.GetValue<int64_t>();
    }
    return metrics;
  }

public:
  YCSBRunner(const std::string db_path, const std::string dev_path,
             const std::string backend, const std::string fdp_map,
             bool use_nvmefs, int memory_limit_mb,
             const std::string checkpoint_mode,
             int num_fields_ = 10, int field_length_ = 100)
      : num_fields(num_fields_), field_length(field_length_), use_nvmefs(use_nvmefs) {

    duckdb::DBConfig config;
    config.SetOption("allow_unsigned_extensions", true);

    db = std::make_unique<duckdb::DuckDB>(nullptr, &config);
    conn = std::make_unique<duckdb::Connection>(*db);

    if (use_nvmefs) {
      setenv("NVMEFS_DEVICE_PATH", dev_path.c_str(), 1);
      setenv("NVMEFS_BACKEND", backend.c_str(), 1);
      setenv("NVMEFS_META", "use_default_async|no_memory_manager", 1);
      if (!fdp_map.empty()) {
        setenv("NVMEFS_FDP_MAPPING", fdp_map.c_str(), 1);
      }

      std::string ext_path = "/home/itu/nvmefs2/build/release/extension/nvmefs/"
                             "nvmefs.duckdb_extension";
      auto load_res = conn->Query("LOAD '" + ext_path + "';");
      if (load_res->HasError())
        throw std::runtime_error("Extension Load Failed: " +
                                 load_res->GetError());
    }

    conn->Query("PRAGMA memory_limit='" + std::to_string(memory_limit_mb) +
                "MB';");

    auto attach_res = conn->Query("ATTACH DATABASE '" + db_path +
                                  "' AS ycsb_db (READ_WRITE);");
    if (attach_res->HasError())
      throw std::runtime_error("Database Attach Failed: " +
                               attach_res->GetError());

    conn->Query("USE ycsb_db;");
    if (checkpoint_mode == "manual") {
      conn->Query("PRAGMA wal_autocheckpoint='256MB';");
    } else {
      conn->Query("PRAGMA wal_autocheckpoint='16MB';");
    }

    
    read_stmt = conn->Prepare("SELECT * FROM usertable WHERE YCSB_KEY = $1");
    if (read_stmt->HasError())
      throw std::runtime_error("Prepare read failed: " + read_stmt->GetError());

    std::string set_clause;
    for (int i = 0; i < num_fields; i++) {
    if (i > 0) set_clause += ", ";
    set_clause += "FIELD" + std::to_string(i) + "=$" + std::to_string(i + 1);
    }
    std::string update_sql =
    "UPDATE usertable SET " + set_clause +
    " WHERE YCSB_KEY=$" + std::to_string(num_fields + 1);

    update_stmt = conn->Prepare(update_sql);
    if (update_stmt->HasError())
      throw std::runtime_error("Prepare update failed: " + update_stmt->GetError());
  }

  // Coordination
  void request_checkpoint() {
    drain_release.store(false);
    drain_paused.store(false);
    drain_requested.store(true);
  }

  bool is_checkpoint_paused() {
    return drain_paused.load();
  }

  void release_checkpoint() {
    drain_requested.store(false);
    drain_release.store(true);
  }

  // Returns one tuple per interval:
  //   (interval_start_offset_seconds, interval_duration_ms, iterations_in_interval)
  std::vector<std::tuple<double, double, int, std::map<std::string, int64_t>>>
  run(int iterations, int row_count, int duration_seconds, int interval_seconds,
    std::function<void(double, double, int, std::map<std::string, int64_t>)> callback = nullptr) {
    py::gil_scoped_release release;

    drain_requested.store(false);
    drain_paused.store(false);
    drain_release.store(false);

    std::mt19937 key_rng(std::random_device{}());
    std::mt19937 val_rng(std::random_device{}() ^ 0x9E3779B9u);
    std::uniform_int_distribution<int> key_dist(0, row_count - 1);
    std::uniform_int_distribution<int> op_dist(1, 1);

    std::vector<std::tuple<double, double, int, std::map<std::string, int64_t>>> results;

    auto emit_interval = [&](double offset_s, double interval_ms, int iters, std::map<std::string, int64_t> metrics) {
        results.emplace_back(offset_s, interval_ms, iters, metrics);
        if (callback) {
            py::gil_scoped_acquire acquire; // Must re-acquire GIL to talk to Python
            callback(offset_s, interval_ms, iters, metrics);
        }
    };

    std::cout << "[RUN] Start Benchmark loop for " << iterations << " iterations" << std::endl;

    const int BATCH_SIZE = 30;

    auto run_start = std::chrono::high_resolution_clock::now();
    auto interval_start = run_start;
    int interval_iterations = 0;
    int batch_count = 0;

    conn->Query("BEGIN TRANSACTION;");

    for (int i = 0; i < iterations; i++) {
      int key = key_dist(key_rng);
      std::string s_key = "user" + std::to_string(key);

      if (op_dist(key_rng) == 0) {
        read_stmt->Execute(s_key);
      } else {
        duckdb::vector<duckdb::Value> params;
        params.reserve(num_fields + 1);
        for (int f = 0; f < num_fields; f++) {
          params.push_back(duckdb::Value(random_value(val_rng, field_length)));
        }
        params.push_back(duckdb::Value(s_key));

        auto pending = update_stmt->PendingQuery(params);
        auto result = pending->Execute();
        if (result->HasError()) {
          throw std::runtime_error("Update failed: " + result->GetError());
        }
      }
      interval_iterations++;
      batch_count++;

      if (batch_count >= BATCH_SIZE) {
        conn->Query("COMMIT;");
        std::cout << "Forcing COMMIT..." << std::endl;
        conn->Query("BEGIN TRANSACTION;");
        batch_count = 0;
      }

      if ((i % 500 == 0) && i > 0) {
        auto now = std::chrono::high_resolution_clock::now();

        if (drain_requested.load()) {
          if (interval_iterations > 0) {
            double interval_ms = std::chrono::duration<double, std::milli>(now - interval_start).count();
            double offset_s   = std::chrono::duration<double>(interval_start - run_start).count();
            auto nvmefs_metrics = sample_metrics();
            emit_interval(offset_s, interval_ms, interval_iterations, nvmefs_metrics);
            interval_iterations = 0;
          }

          auto pause_begin = now;
          drain_paused.store(true);
          while (!drain_release.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
          }
          drain_paused.store(false);

          auto pause_end = std::chrono::high_resolution_clock::now();
          auto pause_duration = pause_end - pause_begin;

          // Exclude pause from total runtime and from the next interval
          run_start += pause_duration;
          interval_start = pause_end;
          now = pause_end;
        }

        auto elapsed_total = std::chrono::duration_cast<std::chrono::seconds>(now - run_start).count();
        auto elapsed_interval = std::chrono::duration_cast<std::chrono::seconds>(now - interval_start).count();

        if (interval_seconds > 0 && elapsed_interval >= interval_seconds) {
          double interval_ms = std::chrono::duration<double, std::milli>(now - interval_start).count();
          double offset_s    = std::chrono::duration<double>(interval_start - run_start).count();
          auto nvmefs_metrics = sample_metrics();
          emit_interval(offset_s, interval_ms, interval_iterations, nvmefs_metrics);
          interval_start = now;
          interval_iterations = 0;
        }

        if (duration_seconds > 0 && elapsed_total >= duration_seconds) {
          break;
        }
      }
    }
    conn->Query("COMMIT;");

    // Emit any remaining iterations as a final (possibly partial) interval
    auto run_end = std::chrono::high_resolution_clock::now();
    if (interval_iterations > 0) {
      double interval_ms = std::chrono::duration<double, std::milli>(run_end - interval_start).count();
      double offset_s = std::chrono::duration<double>(interval_start - run_start).count();
      auto nvmefs_metrics = sample_metrics();
      emit_interval(offset_s, interval_ms, interval_iterations, nvmefs_metrics);
    }

    return results;
  }

};

PYBIND11_MODULE(ycsb_engine, m) {
  py::class_<YCSBRunner>(m, "YCSBRunner")
      .def(py::init<const std::string &, const std::string &,
                    const std::string &, const std::string &, bool, int,
                    const std::string &, int, int>(),
           py::arg("db_path"), py::arg("dev_path"), py::arg("backend"),
           py::arg("fdp_map"), py::arg("use_nvmefs"),
           py::arg("memory_limit_mb"), py::arg("checkpoint_mode"),
           py::arg("num_fields") = 10, py::arg("field_length") = 100)
      .def("run", &YCSBRunner::run,
           py::arg("iterations"), py::arg("row_count"),
           py::arg("duration_seconds"), py::arg("interval_seconds") = 0,
           py::arg("callback") = nullptr)
      .def("request_checkpoint", &YCSBRunner::request_checkpoint)
      .def("is_checkpoint_paused", &YCSBRunner::is_checkpoint_paused)
      .def("release_checkpoint", &YCSBRunner::release_checkpoint);
}
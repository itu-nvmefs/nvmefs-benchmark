#include "duckdb.hpp"
#include <chrono>
#include <pybind11/pybind11.h>
#include <string>
#include <memory>

namespace py = pybind11;

class YCSBRunner {
private:
    std::unique_ptr<duckdb::DuckDB> db;
    std::unique_ptr<duckdb::Connection> conn;

public:
    YCSBRunner(const std::string db_path, const std::string dev_path,
               const std::string backend, const std::string fdp_map) {

        duckdb::DBConfig config;
        config.SetOption("allow_unsigned_extensions", true);

        db = std::make_unique<duckdb::DuckDB>(nullptr, &config);
        conn = std::make_unique<duckdb::Connection>(*db);

        std::string ext_path = "/home/itu/nvmefs/build/release/extension/nvmefs/nvmefs.duckdb_extension";
        auto load_res = conn->Query("LOAD '" + ext_path + "';");
        if (load_res->HasError())
            throw std::runtime_error("Extension Load Failed: " + load_res->GetError());

        std::string secret = "CREATE OR REPLACE PERSISTENT SECRET nvmefs (TYPE NVMEFS, nvme_device_path '" + dev_path + "', backend '" + backend + "'";
        if (!fdp_map.empty())
            secret += ", fdp_mapping '" + fdp_map + "'";
        secret += ", meta 'use_default_async|no_memory_manager');";

        auto sec_res = conn->Query(secret);
        if (sec_res->HasError())
            throw std::runtime_error("Secret Setup Failed: " + sec_res->GetError());

        auto attach_res = conn->Query("ATTACH DATABASE '" + db_path + "' AS ycsb_db (READ_WRITE);");
        if (attach_res->HasError())
            throw std::runtime_error("Database Attach Failed: " + attach_res->GetError());

        conn->Query("USE ycsb_db;");
    }

    double run(int iterations, int row_count) {
        auto start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < iterations; i++) {
            int key = rand() % row_count;
            std::string s_key = "user" + std::to_string(key);
            if (i % 2 == 0) {
                conn->Query("SELECT FIELD0 FROM usertable WHERE YCSB_KEY = '" + s_key + "';");
            } else {
                conn->Query("UPDATE usertable SET FIELD0 = 'updated' WHERE YCSB_KEY = '" + s_key + "';");
            }
        }
        auto end = std::chrono::high_resolution_clock::now();

        return std::chrono::duration<double, std::milli>(end - start).count();
    }
};

// Bind the class to Python
PYBIND11_MODULE(ycsb_engine, m) {
    py::class_<YCSBRunner>(m, "YCSBRunner")
        .def(py::init<const std::string&, const std::string&, const std::string&, const std::string&>())
        .def("run", &YCSBRunner::run, "Run the native YCSB loop");
}

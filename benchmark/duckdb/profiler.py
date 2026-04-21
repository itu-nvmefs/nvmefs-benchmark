import time
from database.database import Database

class QueryProfiler:
    """
    Wraps around DuckDB queries to extract latency and I/O metrics via nvmefs extension
    """
    def __init__(self, db: Database, query_name: str, use_nvmefs: bool = False):
        self.db = db
        self.query_name = query_name
        self.use_nvmefs = use_nvmefs

        self.latency_ms = 0
        self.nvmefs_metrics = {}

    def __enter__(self):
        self.start_metrics = self._get_nvmefs_metrics()
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.perf_counter()
        self.latency_ms = (self.end_time - self.start_time) * 1000

        # Calculate deltas for nvmefs metrics
        end_metrics = self._get_nvmefs_metrics()

        for key, end_value in end_metrics.items():
            if isinstance(end_value, int) and key.startswith("total_"):
                start_value = self.start_metrics.get(key, 0)
                self.nvmefs_metrics[key] = end_value - start_value
            else:
                self.nvmefs_metrics[key] = end_value

    def _get_nvmefs_metrics(self) -> dict:
        if not self.use_nvmefs:
            return {}

        metrics = {}
        try:
            res_metrics = self.db.execute("SELECT * FROM print_nvmefs_metrics()").fetchall()
            for row in res_metrics:
                metrics[row[0]] = int(row[1])
        except Exception as e:
            print(f"{self.query_name}: print_nvmefs_metrics() error -> {e}")
        return metrics

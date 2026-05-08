import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
import duckdb

@dataclass
class ConnectionConfig:
    device: str = ""
    backend: str = ""
    use_fdp: bool = False
    fdp_strategy: str = ""
    memory: int = 0
    threads: int = 0
    ns_id: int = 1
    extension_path: str = ""

    def get_fdp_mapping(self) -> str:
        mappings = {
            "baseline":       ".db:1,.wal:1,.tmp:1",
            "temp-isolated":  ".db:1,.wal:1,.tmp:2",
            "wal-isolated":   ".db:1,.wal:2,.tmp:1",
            "fully-isolated": ".db:1,.tmp:2,.wal:3"
        }
        strategy = (self.fdp_strategy or "baseline").lower()
        return mappings.get(strategy, mappings["baseline"])

class Database(ABC):

    def __init__(self, db_path: str, threads:int, memory: int, temp_size: int):
        self.db_path = db_path
        self.connection: duckdb.DuckDBPyConnection = None
        self.memory = memory
        self.threads = threads
        self.temp_size = temp_size
        self._setup()
    
    @abstractmethod
    def _setup(self):
        pass

    @property
    def get_is_connected(self):
        return self.connection is not None

    def _connect(self):
        if not self.get_is_connected:
            self.connection: duckdb.DuckDBPyConnection = duckdb.connect(
                config={"allow_unsigned_extensions": "true",
                        "max_temp_directory_size": f"{self.temp_size}GB",
                        "memory_limit": f"{self.memory}MB",
                        "threads": self.threads
                    }
                )

    def get_cursor(self):
        """Helper to get a cursor for concurrent threads"""
        return self.connection.cursor()

    def create_concurrent_connection(self):
        return ConcurrentDatabase(self.db_path, self.threads, self.memory, self.temp_size, self.connection.cursor())

    def query(self, query: str):
        return self.connection.query(query).fetchall()

    def execute(self, query: str):
        return self.connection.execute(query)

    def add_extension(self, name: str):
        self.connection.install_extension(name)
        self.connection.load_extension(name)
    
    def install_extension(self, name: str):
        self.connection.install_extension(name)

    def disable_object_cache(self):
        self.execute("PRAGMA disable_object_cache;")

    def set_memory_limit(self, memory_mb: int):
        self.execute(f"PRAGMA memory_limit='{memory_mb}MB';")

    def enable_profiling(self):
        self.execute("PRAGMA enable_profiling='json';")
        self.execute("PRAGMA profiling_output='profile.json';")
        self.execute("PRAGMA profiling_mode='detailed';")

    def close(self):
        self.connection.close()
        self.connection = None
    
class QuackDatabase(Database):
    """
    QuackDatabase is just a normal Database wrapper of DuckDB
    """

    def __init__(self, db_path: str, threads: int, memory: int, temp_size: int):
        super().__init__(db_path, threads, memory, temp_size)
        super()._connect()

        self.execute(f"ATTACH DATABASE '{self.db_path}' AS bench (READ_WRITE);")
        self.execute("USE bench;")
    
    def _setup(self):
        print("Setting up QuackDatabase")
        return

class ConcurrentDatabase(Database):
    """
    ConcurrentDatabase is a Database wrapper of DuckDB that uses the concurrent connection
    """

    def __init__(self, db_path: str, threads:int, memory: int, temp_size: int, connection: duckdb.DuckDBPyConnection):
        super().__init__(db_path, threads, memory, temp_size)
        self.connection = connection

    
    def _setup(self):
        print("Setting up ConcurrentDatabase")
        return

class SPDKDatabase(Database):
    def __init__(self, db_path: str, threads:int, memory: int, temp_size: int, config: ConnectionConfig):
        super().__init__(db_path, threads, memory, temp_size)
        self.config = config
        self.device_path = config.device
        self.use_fdp = config.use_fdp
        self.backend = config.backend

    def _setup(self):
        print("Setting up SPDKDatabase")
        install_extension(self.config.extension_path, self)
        super()._connect()
        self.add_extension("nvmefs")

        secret_name = f"nvmefs_{self.config.ns_id}"
        secret = f"""CREATE OR REPLACE PERSISTENT SECRET {secret_name} (
                     TYPE NVMEFS,
                     nvme_device_path '{self.device_path}',
                     backend          '{self.backend}',
                     meta             'use_default_async|no_memory_manager'"""
        if self.use_fdp:
            secret += f",\n fdp_mapping '{self.config.get_fdp_mapping()}'"
        secret += "\n                    );"
        self.execute(secret)
        self.execute(f"PRAGMA activate_nvmefs('{secret_name}');")
        self.execute(f"ATTACH DATABASE '{self.db_path}' AS bench (READ_WRITE);")
        self.execute("USE bench;")
        

class NvmeDatabase(Database):
    """
    NvmeDatabase is a Database wrapper of DuckDB that uses NVMe as the storage backend
    """

    def __init__(self, db_path: str, threads: int, memory: int, temp_size: int, config: ConnectionConfig):
        self.config = config
        self.device_path = config.device
        self.backend = config.backend
        self.use_fdp = config.use_fdp
        self.fdp_strategy = config.fdp_strategy

        super().__init__(db_path, threads, memory, temp_size)
    
    def _setup(self):
        extension_path = os.path.abspath(self.config.extension_path)
        super()._connect()
        self.install_extension(extension_path)
        self.add_extension(extension_path)
        secret_name = f"nvmefs_{self.config.ns_id}"
        secret = f"""CREATE OR REPLACE PERSISTENT SECRET {secret_name} (
                     TYPE NVMEFS,
                     nvme_device_path '{self.device_path}',
                     backend          '{self.backend}',
                     meta             'use_default_async|no_memory_manager'"""
        if self.use_fdp:
            secret += f",\n fdp_mapping '{self.config.get_fdp_mapping()}'"
        secret += "\n                    );"
        self.execute(secret)
        self.execute(f"PRAGMA activate_nvmefs('{secret_name}');")
        self.execute(f"ATTACH DATABASE '{self.db_path}' AS bench (READ_WRITE);")
        self.execute("USE bench;")
        self.disable_object_cache()

def add_extension(name: str, db: Database = None):
    if db is None or not db.get_is_connected:
        duckdb.load_extension(name)
    else:
        db.add_extension(name)

def install_extension(name: str, db: Database = None):
    if db is None or not db.get_is_connected:
        duckdb.install_extension(name)
    else:
        db.install_extension(name)

def run_query(query: str, db: Database = None):
    if db is None or not db.get_is_connected:
        return duckdb.execute(query)
    else:
        db.query(query)

def connect(db_path:str, threads: int, memory: int, temp_size: int = 200, config: ConnectionConfig = None) -> Database:
    # TODO: Use parameters and insert them into the connection string
    db: Database = None
    print(config)
    if db_path.startswith("nvmefs://") and config.device.startswith("/dev/"):
        db = NvmeDatabase(db_path, threads, memory, temp_size, config)
    elif db_path.startswith("nvmefs://") and config.device.startswith("0000:"):
        db = SPDKDatabase(db_path, threads, memory, temp_size, config)
    else:
        db = QuackDatabase(db_path, threads, memory, temp_size)

    return db

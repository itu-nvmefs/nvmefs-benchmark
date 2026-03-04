import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
import duckdb

@dataclass
class ConnectionConfig:
    device: str = ""
    backend: str = ""
    use_fdp: bool = False
    memory: int = 0
    threads: int = 0

class Database(ABC):

    def __init__(self, db_path: str, threads:int, memory: int):
        self.db_path = db_path
        self.connection: duckdb.DuckDBPyConnection = None
        self.memory = memory
        self.threads = threads
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
                        "max_temp_directory_size": "150GB",
                        "memory_limit": f"{self.memory}MB",
                        "threads": self.threads
                    }
                )

    def get_cursor(self):
        """Helper to get a cursor for concurrent threads"""
        return self.connection.cursor()

    def create_concurrent_connection(self):
        return ConcurrentDatabase(self.db_path, self.threads, self.memory, self.connection.cursor())

    def query(self, query: str):
        return self.connection.query(query).fetchall()

    def execute(self, query: str):
        return self.connection.execute(query)

    def add_extension(self, name: str):
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

    def __init__(self, db_path: str, threads: int, memory: int):
        super().__init__(db_path, threads, memory)
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

    def __init__(self, db_path: str, threads:int, memory: int, connection: duckdb.DuckDBPyConnection):
        super().__init__(db_path, threads, memory)
        self.connection = connection

    
    def _setup(self):
        print("Setting up ConcurrentDatabase")
        return

class SPDKDatabase(Database):
    def __init__(self, db_path: str, threads:int, memory: int, config: ConnectionConfig):
        super().__init__(db_path, threads, memory)
        self.number_of_fdp_handles = 7
        self.device_path = config.device
        self.use_fdp = config.use_fdp
        self.backend = config.backend

    def _setup(self):
        print("Setting up SPDKDatabase")
        install_extension("../../nvmefs/build/release/extension/nvmefs/nvmefs.duckdb_extension", self)
        super()._connect()
        self.add_extension("nvmefs")
        self.execute(f"""CREATE OR REPLACE PERSISTENT SECRET nvmefs (
                        TYPE NVMEFS,
                        nvme_device_path '{self.device_path}',
                        fdp_plhdls       '{self.number_of_fdp_handles}',
                        backend          '{self.backend}'
                    );""")
        
        self.execute(f"ATTACH DATABASE '{self.db_path}' AS bench (READ_WRITE);")
        self.execute("USE bench;")
        

class NvmeDatabase(Database):
    """
    NvmeDatabase is a Database wrapper of DuckDB that uses NVMe as the storage backend
    """

    def __init__(self, db_path: str, threads: int, memory: int, config: ConnectionConfig):
        self.device_path = config.device
        self.backend = config.backend
        self.use_fdp = config.use_fdp
        self.number_of_fdp_handles = 7
        super().__init__(db_path, threads, memory)
    
    def _setup(self):
        extension_path = os.path.abspath(f"/home/group01/nvmefs/build/release/extension/nvmefs/nvmefs.duckdb_extension")
        super()._connect()
        self.install_extension(extension_path)
        self.add_extension("nvmefs")
        self.execute(f"""CREATE OR REPLACE PERSISTENT SECRET nvmefs (
                        TYPE NVMEFS,
                        nvme_device_path '{self.device_path}',
                        backend          '{self.backend}'
                    );""")
        
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

def connect(db_path:str, threads: int, memory: int, config: ConnectionConfig = None) -> Database:
    # TODO: Use parameters and insert them into the connection string
    db: Database = None
    print(config)
    if db_path.startswith("nvmefs://") and config.device.startswith("/dev/"):
        db = NvmeDatabase(db_path, threads, memory, config)
    elif db_path.startswith("nvmefs://") and config.device.startswith("0000:"):
        db = SPDKDatabase(db_path, threads, memory, config)
    else:
        db = QuackDatabase(db_path, threads, memory)

    return db

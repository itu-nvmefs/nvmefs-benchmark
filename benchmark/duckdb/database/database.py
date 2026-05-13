import os
from dataclasses import dataclass, field
import duckdb


@dataclass
class ConnectionConfig:
    device: str = ""
    backend: str = ""
    use_fdp: bool = False
    fdp_mapping: str = ""
    memory: int = 0
    threads: int = 0
    ns_id: int = 1
    extension_path: str = ""
    db_configs: dict = field(default_factory=dict)


class Database:
    def __init__(self, threads: int, memory: int, temp_size: int,
                 config: ConnectionConfig = None):
        self.threads = threads
        self.memory = memory
        self.temp_size = temp_size
        self.config = config
        self.connection = duckdb.connect(config={
            "allow_unsigned_extensions": "true",
            "max_temp_directory_size": f"{temp_size}GB",
            "memory_limit": f"{memory}MB",
            "threads": threads,
        })
        self.attached: list[str] = []

        if config and config.device and config.extension_path:
            self._activate_nvmefs()

    def _activate_nvmefs(self):
        ext = os.path.abspath(self.config.extension_path)
        self.connection.install_extension(ext)
        self.connection.load_extension(ext)

        secret_name = f"nvmefs_{self.config.ns_id}"
        secret = (
            f"CREATE OR REPLACE PERSISTENT SECRET {secret_name} (\n"
            f"  TYPE NVMEFS,\n"
            f"  nvme_device_path '{self.config.device}',\n"
            f"  backend          '{self.config.backend}',\n"
            f"  meta             'use_default_async|no_memory_manager',\n"
            f"  use_fdp          '{'on' if self.config.use_fdp else 'off'}'"
        )
        if self.config.use_fdp and self.config.fdp_mapping:
            secret += f",\n  fdp_mapping '{self.config.fdp_mapping}'"
        if self.config.db_configs:
            cfg = ",".join(f"{n}:{s}GB" for n, s in self.config.db_configs.items())
            secret += f",\n  db_configs '{cfg}'"
        secret += "\n);"

        self.connection.execute(secret)
        self.connection.execute(f"PRAGMA activate_nvmefs('{secret_name}');")

    def attach(self, db_name: str, mount_path: str = None) -> "Cursor":
        if self.config and self.config.device:
            db_path = f"nvmefs://{db_name}.db"
        else:
            assert mount_path, "non-nvmefs attach needs a mount_path"
            db_path = os.path.join(mount_path, f"{db_name}.db")

        self.connection.execute(
            f"ATTACH DATABASE '{db_path}' AS {db_name} (READ_WRITE);"
        )
        self.attached.append(db_name)

        if not self.config or not self.config.device:
            self.connection.execute(
                f"SET temp_directory = '{os.path.join(mount_path, '.tmp')}';"
            )

        return Cursor(self.connection.cursor(), db_name, self, db_path)

    def execute(self, sql: str):
        return self.connection.execute(sql)

    def query(self, sql: str):
        return self.connection.query(sql).fetchall()

    def close(self):
        if self.connection:
            self.connection.close()
            self.connection = None


class Cursor:
    def __init__(self, raw_cursor, db_name: str, parent: Database, db_path: str):
        self._cursor = raw_cursor
        self.db_name = db_name
        self.parent = parent
        self._db_path = db_path
        self._cursor.execute(f"USE {db_name};")

    def execute(self, sql: str):
        return self._cursor.execute(sql)

    def executemany(self, query: str, parameters: list):
        return self._cursor.executemany(query, parameters)

    def query(self, sql: str):
        return self._cursor.execute(sql).fetchall()

    def add_extension(self, name: str):
        self._cursor.execute(f"INSTALL '{name}';")
        self._cursor.execute(f"LOAD '{name}';")

    @property
    def db_path(self) -> str:
        return self._db_path

    @property
    def memory(self) -> int:
        return self.parent.memory

    @property
    def threads(self) -> int:
        return self.parent.threads

    @property
    def config(self):
        return self.parent.config

    @property
    def device_path(self) -> str:
        return self.parent.config.device if self.parent.config else ""

    @property
    def backend(self) -> str:
        return self.parent.config.backend if self.parent.config else ""

    @property
    def use_fdp(self) -> bool:
        return self.parent.config.use_fdp if self.parent.config else False
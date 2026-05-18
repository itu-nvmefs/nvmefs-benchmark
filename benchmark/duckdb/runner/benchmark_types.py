from typing import Callable

from database.database import Database

type BenchmarkRunnerFunc = Callable[[list[Database], int], list[str]]
type BenchmarkEpochFunc = Callable[[list[Database], int], list[str]]
type BenchmarkSetupFunc = Callable[[list[Database], str, int], None]
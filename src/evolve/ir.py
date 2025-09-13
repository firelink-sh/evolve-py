from __future__ import annotations

import abc
from typing import Union

import duckdb
import polars as pl
import pyarrow as pa

# Internal representation of data - depends on the chosen backend.
IR = Union[pl.DataFrame, pa.Table, duckdb.DuckDBPyConnection]


class BackendMismatchWarning(Warning):
    """When global backend does not match set backend."""

    pass


class BaseBackend(abc.ABC):
    """Abstract base class for a in-memory backend."""

    @abc.abstractmethod
    def ir_from_arrow_table(self, table: pa.Table) -> IR:
        pass


class ArrowBackend(BaseBackend):
    """Implementation of an `Arrow` in-memory backend."""

    def ir_from_arrow_table(self, table: pa.Table) -> IR:
        return table


class PolarsBackend(BaseBackend):
    """Implementation of a `Polars` in-memory backend."""

    def ir_from_arrow_table(self, table: pa.Table) -> IR:
        return pl.from_arrow(table)


class DuckdbBackend(BaseBackend):
    """Implementation of a `DuckDB` in-memory backend."""

    def __init__(self) -> None:
        """Initialize in-memory database connection."""
        self._conn = duckdb.connect(database=":memory:")

    def ir_from_arrow_table(self, table: pa.Table) -> IR:
        return self._conn.register("tmp_arrow_data", table)


class LazyIR:
    """Implementation of a Lazy Internal Representation (IR)."""

    @classmethod
    def from_arrow_table(cls, table: pa.Table) -> LazyIR:
        """Initialize a new LazyIR."""
        return cls(ir=pl.from_arrow(table))

    def __init__(self, ir: IR) -> None:
        """Initialize a new Lazy IR."""
        self._ir = ir

    def get_ir(self) -> IR:
        """Get the IR."""
        return self._ir

    def set_ir(self, ir: IR) -> None:
        """Update the IR."""
        self._ir = ir


_current_backend: BaseBackend = PolarsBackend()


def set_global_backend(backend: BaseBackend) -> None:
    """Set the default global backend to use."""
    global _current_backend
    _current_backend = backend


def get_global_backend() -> BaseBackend:
    """Get the default global backend currently in use."""
    return _current_backend

from __future__ import annotations

import abc
from typing import Union

import duckdb
import polars as pl
import pyarrow as pa
from pyarrow import ipc

# Internal representation of data - depends on the chosen backend.
IR = Union[bytes, pl.Series, pl.DataFrame, pa.Table, duckdb.DuckDBPyConnection]


class BackendMismatchWarning(Warning):
    """When global backend does not match set backend."""

    pass


class BaseBackend(abc.ABC):
    """Abstract base class for a in-memory backend."""

    @abc.abstractmethod
    def ir_from_arrow_table(self, table: pa.Table) -> IR:
        pass

    @abc.abstractmethod
    def ir_to_arrow_table(self, data: IR) -> pa.Table:
        pass


class ArrowBackend(BaseBackend):
    """Implementation of an arrow in-memory table backend."""

    def ir_from_arrow_table(self, data: pa.Table) -> pa.Table:
        """This is a no-op."""
        return data

    def ir_to_arrow_table(self, data: pa.Table) -> pa.Table:
        """This is a no-op."""
        return data

    def ir_to_polars_df(self, data: pa.Table) -> pl.DataFrame:
        return pl.from_arrow(data)

    def ir_from_polars(self, data: pl.DataFrame | pl.Series) -> pa.Table:
        return data.to_arrow()


class PolarsBackend(BaseBackend):
    """Implementation of a polars in-memory dataframe backend."""

    def ir_from_arrow_table(self, table: pa.Table) -> IR:
        return pl.from_arrow(table)

    def ir_to_arrow_table(self, data: pl.DataFrame) -> pa.Table:
        return data.to_arrow()

    def ir_to_polars_df(self, data: pl.DataFrame) -> pl.DataFrame:
        return data


class DuckdbBackend(BaseBackend):
    """Implementation of a duckdb in-memory database backend."""

    def __init__(self) -> None:
        """Initialize in-memory database connection."""
        self._conn = duckdb.connect(database=":memory:")

    def ir_from_arrow_table(self, table: pa.Table) -> IR:
        return self._conn.register("tmp_arrow_data", table)

    def ir_to_arrow_table(self, data: duckdb.DuckDBPyConnection) -> pa.Table:
        return data.execute("SELECT * FROM tmp_arrow_data;").fetch_arrow_table()


class BytesBackend(BaseBackend):
    """Implementation of a bytes in-memory backend."""

    def bytes_from_ir(self, _bytes: bytes) -> bytes:
        return _bytes

    def ir_from_bytes(self, _bytes: bytes) -> IR:
        return _bytes

    def ir_from_arrow_table(self, table: pa.Table) -> IR:
        # We need to serialize the table to bytes using ipc
        sink = pa.BufferOutputStream()
        with ipc.new_stream(sink, table.schema) as stream:
            stream.write_table(table)

        return sink.getvalue().to_pybytes()


_current_backend: BaseBackend = PolarsBackend()


def set_global_backend(backend: BaseBackend) -> None:
    """Set the default global backend to use."""
    global _current_backend
    _current_backend = backend


def get_global_backend() -> BaseBackend:
    """Get the default global backend currently in use."""
    return _current_backend

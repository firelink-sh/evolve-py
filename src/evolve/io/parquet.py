from pathlib import Path

import pyarrow.parquet as pq

from .._utils import _try_get_file_system_from_uri
from ..ir import IR, BaseBackend, get_global_backend
from ._base import BaseIO


class ParquetFile(BaseIO):
    """Implementation of a parquet file."""

    def __init__(
        self,
        uri: str | Path,
        *,
        backend: BaseBackend | None = None,
        **options,
    ) -> None:
        """Initialize a new `ParquetFile`."""
        super().__init__(
            name=self.__class__.__name__,
            backend=backend or get_global_backend(),
        )

        file_system, file_path = _try_get_file_system_from_uri(
            uri=uri,
            **options,
        )

        self._file_system = file_system
        self._file_path = file_path
        self._read_options = options.get("read_options", {})
        self._write_options = options.get("write_options", {})

    def read(self) -> IR:
        """Read the parquet file to the backend IR."""
        with self._file_system.open_input_file(self._file_path) as source:
            return self._backend.ir_from_arrow_table(
                pq.read_table(source=source, **self._read_options)
            )

    def write(self, data: IR) -> None:
        """Write backend IR to parquet file."""
        with self._file_system.open_output_stream(self._file_path) as destination:
            pq.write_table(
                table=self._backend.ir_to_arrow_table(data),
                where=destination,
                **self._write_options,
            )

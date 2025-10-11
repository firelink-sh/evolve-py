from pathlib import Path

import polars as pl

from .._utils import _try_get_file_system_from_uri
from ..ir import IR, BaseBackend, get_global_backend
from ._base import BaseIO


class JsonLinesFile(BaseIO):
    """Implementation of a JSON lines file."""

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
        self._read_options = options.get("read_options")
        self._parse_options = options.get("parse_options")
        self._write_options = options.get("write_options")

    def read(self) -> IR:
        with self._file_system.open_input_file(self._file_path) as source:
            return self._backend.ir_from_arrow_table(
                pl.read_ndjson(
                    source=source,
                ).to_arrow()
            )

    def write(self, data: IR) -> None:
        with self._file_system.open_output_stream(self._file_path) as destination:
            self._backend.ir_to_polars_df(data).write_ndjson(file=destination)

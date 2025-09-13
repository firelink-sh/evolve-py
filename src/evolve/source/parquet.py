from pathlib import Path
from typing import (
    Any,
    Mapping,
)

import pyarrow.parquet as pq

from evolve.ir import (
    IR,
    _current_backend,
)

from .base import (
    BaseSource,
    _try_get_file_system_from_uri,
)


class ParquetSource(BaseSource):
    """Implementation of a parquet file source."""

    def __init__(self, uri: str | Path, **options: Mapping[str, Any]) -> None:
        """Initialize a new `ParquetSource` with the provided file system specific options."""
        super().__init__(name=self.__class__.__name__)

        file_system, file_path = _try_get_file_system_from_uri(
            uri,
            **options,
        )

        self._file_system = file_system
        self._file_path = file_path
        self._options = options

    def load(self) -> IR:
        """Load the parquet file from the source to IR."""
        with self._file_system.open_input_file(self._file_path) as f:
            return _current_backend.ir_from_arrow_table(pq.read_table(source=f))

    def validate_config(self) -> None:
        """Validate the provided parquet file reading options."""
        pass

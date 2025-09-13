from pathlib import Path
from typing import Mapping

from pyarrow import csv

from .._utils import _try_get_file_system_from_uri
from ..ir import (
    IR,
    BaseBackend,
    get_global_backend,
)
from .base import BaseTarget


class CsvTarget(BaseTarget):
    """Implementation of a csv target."""

    def __init__(
        self,
        uri: str | Path,
        *,
        backend: BaseBackend | None = None,
        **options: Mapping[str, str],
    ) -> None:
        """Initialize a new `CsvTarget` with the provided file system options."""
        super().__init__(
            name=self.__class__.__name__,
            backend=backend or get_global_backend(),
        )

        file_system, file_path = _try_get_file_system_from_uri(
            uri,
            **options,
        )

        self._file_system = file_system
        self._file_path = file_path
        self._write_options = options.get("write_options")

    def write(self, data: IR) -> None:
        with self._file_system.open_output_stream(self._file_path) as f:
            csv.write_csv(
                data=self._backend.ir_to_arrow_table(data),
                output_file=f,
                write_options=self._write_options,
            )

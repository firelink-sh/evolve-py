from pathlib import Path

from pyarrow import csv

from ._base import BaseIO
from .._utils import _try_get_file_system_from_uri
from ..ir import (
    IR,
    BaseBackend,
    get_global_backend,
)


class CsvFile(BaseIO):
    """Implementation of a csv file."""

    def __init__(self, uri: str | Path, *, backend: BaseBackend | None = None, **options) -> None:
        """Initialize a new `CsvFile` with the provided options."""
        super().__init__(name=self.__class__.__name__, backend=backend or get_global_backend())
        
        file_system, file_path = _try_get_file_system_from_uri(
            uri,
            **options,
        )

        self._file_system = file_system
        self._file_path = file_path
        self._read_options = options.get("read_options")
        self._parse_options = options.get("parse_options")
        self._convert_options = options.get("convert_options")

    def load(self) -> IR:
        """Load the csv file from the source path to the configured backend IR."""
        with self._file_system.open_input_file(self._file_path) as source:
            return self._backend.ir_from_arrow_table(
                csv.read_csv(
                    input_file=source,
                    read_options=self._read_options,
                    parse_options=self._parse_options,
                    convert_options=self._convert_options,
                )
            )

    def write(self, data: IR) -> None:
        """Write the backend IR data as a csv to the target path."""
        with self._file_system.open_output_stream(self._file_path) as target:
            csv.write_csv(
                data=self._backend.ir_to_arrow_table(data),
                output_file=target,
                write_options=self._write_options,
            )

import warnings
from pathlib import Path
from typing import Mapping

from pyarrow import csv

from .._utils import _try_get_file_system_from_uri
from ..ir import (
    IR,
    BackendMismatchWarning,
    BaseBackend,
    get_global_backend,
)
from .base import BaseSource


class CsvSource(BaseSource):
    """Implementation of a csv file source."""

    def __init__(
        self,
        uri: str | Path,
        *,
        backend: BaseBackend | None = None,
        **options: Mapping[str, str],
    ) -> None:
        """Initialize a new `CsvSource` with the provided file system options."""
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
        self._read_options = options.get("read_options")
        self._parse_options = options.get("parse_options")
        self._convert_options = options.get("convert_options")

    def load(self) -> IR:
        """Load the csv file from the source to IR."""
        if self._backend != get_global_backend():
            warnings.warn(
                "the defined backend for the source is not equal to the global backend "
                "currently in use, be aware of potential IR mismatches",
                BackendMismatchWarning,
                2,
            )
        with self._file_system.open_input_file(self._file_path) as f:
            return self._backend.ir_from_arrow_table(
                csv.read_csv(
                    input_file=f,
                    read_options=self._read_options,
                    parse_options=self._parse_options,
                    convert_options=self._convert_options,
                )
            )

    def validate_config(self) -> None:
        pass

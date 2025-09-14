from pathlib import Path

from pyarrow import json

from .._utils import _try_get_file_system_from_uri
from ..ir import IR, BaseBackend, get_global_backend
from ._base import BaseIO


class JsonFile(BaseIO):
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
        self._read_options = options.get("read_options")
        self._parse_options = options.get("parse_options")
        self._write_options = options.get("write_options")

    def read(self) -> IR:
        """
        Read the json file to the backend IR.

        Returns
        -------
        IR
            The json file loaded into the backend specific IR.

        Automatic Type Inference
        ------------------------
        Arrow data types are inferred from the JSON types and values of each
        column:
        - JSON null values are converted to the `null` type, but can fall back
        to any other type,
        - JSON booleans convert to `bool_`,
        - JSON numbers convert to `int64`, failling back to `float64` if a
          non-integer is encountered,
        - JSON strings of the kind "YYYY-MM-DD hh:mm:ss" convert to
          `timestamp(s)`, falling back to `utf8` if a conversion occurs,
        - JSON arrays convert to a `list` type, and inference proceeds
          recursively on the JSON arrays' values,
        - Nested JSON objects convert to a `struct` type, and inference
          proceeds recursively on the JSON object's values.

        """
        with self._file_system.open_input_file(self._file_path) as source:
            return self._backend.ir_from_arrow_table(
                json.read_json(
                    input_file=source,
                    read_options=self._read_options,
                    parse_options=self._parse_options,
                )
            )

    def write(self, data: IR) -> None:
        """Write backend IR to parquet file."""
        with self._file_system.open_output_stream(self._file_path) as destination:
            self._backend.ir_to_polars_df(data).write_json(file=destination)

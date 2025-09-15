from pathlib import Path
from typing import Iterable, Tuple

import polars as pl

from ..ir import IR, BaseBackend, get_global_backend
from ._base import BaseIO
from ._utils import _try_get_file_system_from_uri


class MultiFixedWidthFile(BaseIO):
    """Implementation of a fixed width file (fwf)."""

    def __init__(
        self,
        uri: str | Path,
        schema_map: dict[str, dict[str, Iterable[str] | Iterable[tuple[int, int]]]],
        schema_spec_len: int,
        schema_spec_offset: int = 0,
        pad_char: str = " ",
        encoding: str = "utf-8",
        backend: BaseBackend | None = None,
    ) -> None:
        """Initialize the `FixedWidthFile`."""
        super().__init__(
            name=self.__class__.__name__,
            backend=backend or get_global_backend(),
        )

        file_system, file_path = _try_get_file_system_from_uri(uri=uri)
        self._file_system = file_system
        self._file_path = file_path
        self._schema_map = schema_map
        self._schema_spec_len = schema_spec_len
        self._schema_spec_offset = schema_spec_offset
        self._pad_char = pad_char
        self._encoding = encoding

    def read(self) -> IR:
        """Read the fixed width file."""
        with self._file_system.open_input_file(self._file_path) as source:
            df = pl.read_csv(
                source=source,
                has_header=False,
                skip_rows=0,
                new_columns=["full_str"],
            )

        df = df.with_columns(
            [
                pl.col("full_str")
                .str.slice(self._schema_spec_offset, self._schema_spec_len)
                .alias("_schema_id")
            ]
        )

        dfs = []
        skip_n_chars = self._schema_spec_len + self._schema_spec_offset

        for schema_id, schema_def in self._schema_map.items():
            colspecs = schema_def["colspecs"]
            colnames = schema_def["colnames"]

            df_schema_spec = df.filter(pl.col("_schema_id") == schema_id)
            df_schema_spec = df_schema_spec.with_columns(
                [
                    pl.col("full_str")
                    .str.slice(start + skip_n_chars, width)
                    .str.strip_chars(self._pad_char)
                    .alias(col)
                    for (start, width), col in zip(colspecs, colnames, strict=True)
                ]
            ).drop(["full_str", "_schema_id"])
            dfs.append(df_schema_spec)

        return dfs

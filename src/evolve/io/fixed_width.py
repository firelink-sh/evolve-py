"""
Note to self: the evolve.io module should not be the implementation of each
different type of file, database, etc.

IO should represent different data FORMATS to read/write data,
i.e., it could be file, adbc, jdbc, kafka, ipc, gRPC, etc.

So - how would that look like?

We would have


evolve/
    io/
        base.py
        file.py
        adbc.py? or just database.py
        kafka?
        gRPC
    source/
        parquet.py
        ...


and the parquet implementation could look like:

```
class ParquetSource(evolve.io.File):
    def __init__(self, uri, **options) -> None:
        super().__init__(uri=uri, **options)

    def read(self) -> IR:
        pass

    def write(self, data: IR) -> None:
        pass
```

is this a reasonable abstraction? or too much?
what do I mean by I/O vs source?...

...

Hm, what do other frameworks do?
pandas does io -> csv, fwf, json, ...

"""

from pathlib import Path
from typing import Iterable, Tuple

import polars as pl

from ..ir import IR, BaseBackend, get_global_backend
from ._base import BaseIO
from ._utils import _try_get_file_system_from_uri


class FixedWidthFile(BaseIO):
    """Implementation of a fixed width file (fwf)."""

    def __init__(
        self,
        uri: str | Path,
        colspecs: Iterable[Tuple[int, int]],
        colnames: Iterable[str],
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
        self._colspecs = colspecs
        self._colnames = colnames
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
                pl.col("full_str").str.slice(st[0], st[1]).str.strip_chars().alias(col)
                for st, col in zip(self._colspecs, self._colnames, strict=True)
            ]
        ).drop("full_str")

        return self._backend.ir_from_polars(df)

    def write(self, data: IR) -> None:
        pass

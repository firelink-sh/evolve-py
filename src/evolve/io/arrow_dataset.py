import os
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
from pyarrow import fs

from .._utils import _try_get_file_system_from_uri
from ..ir import BaseBackend, get_global_backend, IR

from ._base import BaseIO


def write_partitioned_parquet_embedded(
    df: pa.Table | pl.DataFrame,
    base_dir: str,
    partition_cols: list[str],
    filename: str = "data.parquet",
    compression: str = "zstd",
    filesystem: fs.FileSystem | None = None,
) -> None:
    """
    Write partitioned Parquet files with embedded partition columns using Polars or PyArrow.

    Args:
        df: Polars DataFrame or PyArrow Table
        base_dir: Output directory (local or S3)
        partition_cols: List of column names to partition by
        filename: Name of each output file
        compression: Parquet compression codec
        filesystem: Optional PyArrow filesystem (e.g., S3FileSystem)
    """
    if isinstance(df, pa.Table):
        df = pl.from_arrow(df)

    if not all(col in df.columns for col in partition_cols):
        raise ValueError("All partition columns must be present in the DataFrame")

    # Group by partition columns
    groups = df.group_by(partition_cols, maintain_order=True)

    for keys, group_df in groups:
        # Build partition path
        parts = [f"{col}={val}" for col, val in zip(partition_cols, keys)]
        path = os.path.join(base_dir, *parts)

        # Create directory if local
        if filesystem is None:
            os.makedirs(path, exist_ok=True)

        # Convert to Arrow
        arrow_table = group_df.to_arrow()

        # Write Parquet file
        full_path = os.path.join(path, filename)
        pq.write_table(
            arrow_table,
            full_path,
            compression=compression,
            filesystem=filesystem,
        )


class ArrowDataset(BaseIO):
    def __init__(
        self,
        uri: str | Path,
        *,
        backend: BaseBackend | None = None,
        **options,
    ) -> None:
        super().__init__(
            name=self.__class__.__name__,
            backend=backend or get_global_backend(),
        )

        file_system, base_dir = _try_get_file_system_from_uri(uri, **options)

        self._file_system = file_system
        self._base_dir = base_dir

        self._schema = options.get("schema")
        self._format = options.get("format", "parquet")
        self._partitioning = options.get("partitioning")
        self._existing_data_behaviour = options.get("existing_data_behaviour", "error")

    def read(self) -> IR:
        return self._backend.ir_from_arrow_table(
            ds.dataset(
                self._base_dir,
                format=self._format,
                filesystem=self._file_system,
            ).to_table()
        )

    def write(self, data: IR) -> None:
        ds.write_dataset(
            data=self._backend.ir_to_arrow_table(data),
            base_dir=self._base_dir,
            format=self._format,
            partitioning=self._partitioning,
            existing_data_behavior=self._existing_data_behaviour,
            filesystem=self._file_system,
        )

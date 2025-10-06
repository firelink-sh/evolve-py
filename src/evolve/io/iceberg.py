import pyiceberg.catalog as ibc

import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from ..ir import BaseBackend, get_global_backend, IR
from ._base import BaseIO

def read_iceberg_with_pyarrow(catalog: ibc.Catalog, table_name: str) -> pl.DataFrame:
    table = catalog.load_table(table_name)
    scan = table.scan()
    tasks = scan.plan_files()
    tables = []
    for task in tasks:
        file_path = task.file.file_path
        partition = task.file.partition
        data_table = pq.read_table(file_path)

        if partition:
            partition_arrays = {
                key: pa.array([value] * len(data_table))
                for key, value in partition.items()
            }
            partition_table = pa.table(partition_arrays)
            enriched_table = pa.concat_tables([data_table, partition_table],
                                              promote=True,)
        else:
            enriched_table = data_table
        tables.append(enriched_table)
    return pa.concat_tables(table, promote=True)

def read_iceberg_with_polars(catalog: ibc.Catalog, table_name: str) -> pl.DataFrame:
    table = catalog.load_table(table_name)
    scan = table.scan()
    files = [scan_task.file.file_path for scan_task in scan.plan_files()]
    df = pl.read_parquet(files)
    return df


def read_iceberg_with_duckdb(
    catalog: ibc.Catalog, table_name: str
) -> duckdb.DuckDBPyConnection:
    table = catalog.load_table(table_name)
    scan = table.scan()
    files = [scan_task.file.file_path for scan_task in scan.plan_files()]
    conn = duckdb.connect(database=":memory:")
    conn.execute(f"CREATE VIEW iceberg_data AS SELECT * FROM parquet_scan({files})")
    return conn


class IcebergTable(BaseIO):
    """Implementation of an Iceberg table."""

    def __init__(
        self,
        table: str,
        namespace: str,
        catalog_name: str,
        *,
        backend: BaseBackend | None = None,
        **options,
    ) -> None:
        """Initialize the `IcebergTable`."""
        super().__init__(
            name=self.__class__.__name__,
            backend=backend or get_global_backend(),
        )

        catalog = ibc.load_catalog(name=catalog_name, **options)

        self._fqn = f"{namespace}.{table}"
        self._table = table
        self._namespace = namespace
        self._catalog = catalog

    def read(self) -> IR:
        """
        load_table()
            schema: column names, types, nullability
            partition spec: how the table is partitioned
            snapshots: versions of the table over time
            manifests: lists of data files and their metadata
            file paths: location of parquet/ORC files
            scan planner: lets you plan which files to read based on filters
        """
        return self._backend.

## Notes on working with Iceberg Tables


Do embedded partitioning to much easier read the full data in the tables,
otherwise have to manually concat the data in the parquet files with the
partitioning column data, which is tedious and prone to errors.

**Let Spark write the Iceberg tables and embed the partitioning column(s).**

### Getting file paths with `pyiceberg`

```python
import pyiceberg.catalog as ibc

catalog = ibc.load_catalog("default")
table = catalog.load_table("namespace.table")

scan = table.scan()
file_paths = [task.file.file_path for task in scan.plan_files()]
```


### Reading with Polars

- Fast, memory-efficient
- Partition columns already present
- Great for columnar analytics and transformations

```python
import polars as pl

df = pl.read_parquet(file_paths)
```


### Reading with DuckDB

- SQL support
- Easy conversion to Pandas, Arrow, Polars...
- Can filter, project, and aggregate before materializing!

```python
import duckdb

conn = duckdb.read_parquet(file_paths)
arrow_table = conn.arrow()

# or filter before materializing
filtered = conn.filter("event_type = 'click' AND region = 'EU'")
filtered_table = filtered.arrow()
```

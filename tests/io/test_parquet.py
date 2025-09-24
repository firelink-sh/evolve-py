import io
from pathlib import Path

from duckdb import DuckDBPyConnection
import pyarrow as pa
import pyarrow.parquet as pq
from testcontainers.minio import MinioContainer

from evolve.ir import (
    ArrowBackend,
    DuckdbBackend,
    PolarsBackend,
    set_global_backend,
)
from evolve.io import ParquetFile


def test_parquet_source_local_file_arrow_backend():
    source = ParquetFile(
        "examples/data/weather.parquet",
        backend=ArrowBackend(),
    )
    ir = source.read()
    print("============ LOCAL Parquet (Backend: PyArrow) ============")
    print(ir)
    assert isinstance(ir, pa.Table)


def test_parquet_source_s3_minio_duckdb_backend():
    with MinioContainer() as minio:
        client = minio.get_client()
        client.make_bucket("evolve-test")

        table = pa.table({"name": ["banana", "coffee"], "yes": [1, 4]})
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        client.put_object(
            bucket_name="evolve-test",
            object_name="evolve_created/test.parquet",
            data=buffer,
            length=len(buffer.getvalue()),
            content_type="application/octet-stream",
        )

        host = minio.get_container_host_ip()
        port = minio.get_exposed_port(minio.port)
        endpoint = f"http://{host}:{port}"
        uri = "s3://evolve-test/evolve_created/test.parquet"

        source = ParquetFile(
            uri,
            access_key=minio.access_key,
            secret_key=minio.secret_key,
            endpoint_override=endpoint,
            backend=DuckdbBackend(),
        )
        ir = source.read()
        assert isinstance(ir, DuckDBPyConnection)
        t = ir.execute("SELECT * FROM tmp_arrow_data;").fetch_arrow_table()
        print("============ LOCAL Parquet (Backend: DuckDB) ============")
        print(t)
        assert isinstance(t, pa.Table)


def test_parquet_source_local_file_polars_backend():
    set_global_backend(PolarsBackend())
    source = ParquetFile(
        Path.cwd() / "examples" / "data" / "weather.parquet",
    )
    ir = source.read()
    print("========== LOCAL PARQUET (Backend: Polars) ============")
    print(ir.head())


def test_parquet_source_s3_minio():
    with MinioContainer() as minio:
        client = minio.get_client()
        client.make_bucket("evolve-test")

        table = pa.table({"name": ["banana", "coffee"], "yes": [1, 4]})
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        # NOTE on `len(buffer.getvalue())`:
        # io.BytesIO.getvalue() does not move the pointer into the buffer, it
        # just return a `bytes` COPY of the buffer contents. The pointer
        # `buffer.tell()` only moves when we do `read()` or `write()`.
        # SO if the buffer is large - `getvalue()` duplicates it in memory
        # which is not good, in that case we should do
        # buffer.seek(0, io.SEEK_END)
        # length = buffer.tell()
        # buffer.seek(0)
        # which doesn't make a copy of the memory

        # What is 'octet-stream'?
        # refers to a MIME type, specifically used for binary files that do not
        # have a specific type - file contents are unknown.
        client.put_object(
            bucket_name="evolve-test",
            object_name="evolve_created/test.parquet",
            data=buffer,
            length=len(buffer.getvalue()),
            content_type="application/octet-stream",
        )

        host = minio.get_container_host_ip()
        port = minio.get_exposed_port(minio.port)
        endpoint = f"http://{host}:{port}"
        uri = "s3://evolve-test/evolve_created/test.parquet"

        source = ParquetFile(
            uri,
            access_key=minio.access_key,
            secret_key=minio.secret_key,
            endpoint_override=endpoint,
        )
        ir = source.read()
        print("============ S3 parquet ============")
        print(ir.head())

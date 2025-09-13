import io

from duckdb import DuckDBPyConnection
import polars as pl
import pyarrow as pa
from pyarrow import csv
from testcontainers.minio import MinioContainer

from evolve.ir import ArrowBackend, DuckdbBackend
from evolve.source import CsvSource


def test_csv_source_local_file_duckdb_backend():
    source = CsvSource("examples/data/dummy.csv", backend=DuckdbBackend())
    ir = source.load()
    print("========== LOCAL CSV (Backend: DuckDB) ===========")
    assert isinstance(ir, DuckDBPyConnection)
    t = ir.execute("SELECT * FROM tmp_arrow_data;").fetch_arrow_table()
    print(t)
    assert isinstance(t, pa.Table)


def test_csv_source_local_file_arrow_backend():
    source = CsvSource("examples/data/dummy.csv", backend=ArrowBackend())
    ir = source.load()
    print("========== LOCAL CSV (Backend: PyArrow) ===========")
    print(ir)
    assert isinstance(ir, pa.Table)


def test_csv_source_local_file_polars_backend():
    source = CsvSource("examples/data/dummy.csv")
    ir = source.load()
    print("========== LOCAL CSV (Backend: Polars) ===========")
    print(ir.head())
    assert isinstance(ir, pl.DataFrame)


def test_csv_source_s3_minio():
    with MinioContainer() as minio:
        client = minio.get_client()
        client.make_bucket("evolve-test")

        table = pa.table(
            {"guh": ["max", "oscar", "lando"], "winner": [True, True, False]}
        )
        buffer = io.BytesIO()
        csv.write_csv(table, buffer)
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
            object_name="evolve_created/test.csv",
            data=buffer,
            length=len(buffer.getvalue()),
            content_type="application/octet-stream",
        )

        host = minio.get_container_host_ip()
        port = minio.get_exposed_port(minio.port)
        endpoint = f"http://{host}:{port}"
        uri = "s3://evolve-test/evolve_created/test.csv"

        source = CsvSource(
            uri,
            access_key=minio.access_key,
            secret_key=minio.secret_key,
            endpoint_override=endpoint,
        )
        ir = source.load()
        print("============ S3 csv ============")
        print(ir.head())

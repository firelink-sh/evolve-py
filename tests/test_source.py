import io
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import sqlalchemy
from testcontainers.minio import MinioContainer
from testcontainers.postgres import PostgresContainer

from evolve.old_source import PostgresSource
from evolve.source import ParquetSource


def test_parquet_source_local_file():
    source = ParquetSource(
        Path.cwd() / "examples" / "data" / "weather.parquet",
    )
    ir = source.load()
    print(ir)


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

        source = ParquetSource(
            uri,
            access_key=minio.access_key,
            secret_key=minio.secret_key,
            endpoint_override=endpoint,
        )
        ir = source.load()
        print(ir)


def test_postgres_source():
    with PostgresContainer(
        "postgres:latest", username="xd", password="xdd", dbname="test"
    ) as postgres:
        engine = sqlalchemy.create_engine(postgres.get_connection_url())

        with engine.begin() as conn:
            conn.execute(sqlalchemy.text("CREATE SCHEMA IF NOT EXISTS test;"))
            conn.execute(
                sqlalchemy.text(
                    "CREATE TABLE IF NOT EXISTS test.bananas (name text, size integer);"
                )
            )
            conn.execute(
                sqlalchemy.text(
                    "INSERT INTO test.bananas (name, size) VALUES ('chiquita', 149), ('dark soul', 1894718);"
                )
            )
            conn.commit()

        source = PostgresSource(
            host=postgres.get_container_host_ip(),
            port=postgres.get_exposed_port(postgres.port),
            db="test",
            schema="test",
            table="bananas",
            user="xd",
            password="xdd",
        )

        ir = source.load()
        print(ir.get_ir().head())

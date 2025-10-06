import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
from testcontainers.minio import MinioContainer

from evolve.ir import PolarsBackend
from evolve.io import ArrowDataset, ParquetFile


def test_write_and_read_s3():
    with MinioContainer() as minio:
        client = minio.get_client()
        client.make_bucket("evolve-test")

        host = minio.get_container_host_ip()
        port = minio.get_exposed_port(minio.port)
        endpoint = f"http://{host}:{port}"

        df = pl.DataFrame(
            {"a": [1, 2, 3], "b": [-0.4, 23.14, 9184.49], "c": [True, False, True]}
        )

        print(df.head())

        schema = df.drop("a").drop("b").to_arrow().schema
        print(schema)

        target = ArrowDataset(
            "s3fs://evolve-test/clean/events/",
            partitioning=ds.partitioning(schema=schema, flavor="hive"),
            access_key=minio.access_key,
            secret_key=minio.secret_key,
            endpoint_override=endpoint,
            backend=PolarsBackend(),
        )

        target.write(df)

        df2 = target.read()
        assert len(df) == len(df2)
        # partitioning column is not embedded in the parquet
        assert len(df2.columns) == 2

        print(df2.head())

        files = [
            o.object_name
            for o in client.list_objects(
                "evolve-test",
                recursive=True,
            )
        ]

        fname = [f for f in files if ".parquet" in f][0]

        pp = ParquetFile(
            "s3://evolve-test/" + fname,
            access_key=minio.access_key,
            secret_key=minio.secret_key,
            endpoint_override=endpoint,
            backend=PolarsBackend(),
        )

        dff = pp.read()
        print(dff.head())

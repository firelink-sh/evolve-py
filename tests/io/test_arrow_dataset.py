import polars as pl
from testcontainers.minio import MinioContainer

from evolve.ir import PolarsBackend
from evolve.io import ArrowDataset


def test_write_to_s3():
    with MinioContainer() as minio:
        client = minio.get_client()
        client.make_bucket("evolve-test")

        host = minio.get_container_host_ip()
        port = minio.get_exposed_port(minio.port)
        endpoint = f"http://{host}:{port}"

        df = pl.DataFrame(
            {"a": [1, 2, 3], "b": [-0.4, 23.14, 9184.49], "c": [True, False, True]}
        )

        target = ArrowDataset(
            "s3fs://evolve-test/clean/events/",
            partitioning=["c"],
            access_key=minio.access_key,
            secret_key=minio.secret_key,
            endpoint_override=endpoint,
            backend=PolarsBackend(),
        )

        target.write(df)

import uuid
import time
from datetime import datetime
from pathlib import Path

import polars as pl
import evolve as ev
from evolve import (
    PolarsBackend,
    set_global_backend,
)
from minio import Minio
from loguru import logger

set_global_backend(PolarsBackend())

job_start_time = datetime.now()
job_unique_id = str(uuid.uuid4())
job_name = Path(__file__)


@logger.catch(reraise=True)
def main() -> None:
    """Entry point for daily ingest job."""

    config = {...}
    client = Minio(config)
    files = client.list_objects(...)
    n_files = len(files)

    for idx, file in enumerate(files, start=1):
        logger.info(f"processing file {idx}/{n_files}...")
        source = ev.io.CsvSource(file)
        target = ev.io.ArrowDataset(
            config.DATASET_NAME,
            partitioning=["big_fat_column"],
            access_key=config.S3_ACCESS_KEY,
            secret_key=config.S3_SECRET_KEY,
            endpoint_override=config.S3_ENDPOINT,
        )

        t_read_start = time.perf_counter()
        df = source.read()
        t_read_elapsed = time.perf_counter() - t_read_start
        logger.info(f"reading {len(df}) rows took {t_read_elapsed:.6f} seconds")

        df.with_columns(
            [
                pl.lit(job_start_time).alias("internal_loaded_time"),
                pl.lit(job_name).alias("internal_job_name"),
            ]
        )

        t_write_start = time.perf_counter()
        target.write(df)
        t_write_elapsed = time.perf_counter() - t_write_start
        logger.info(f"writing {len(df)} took {t_write_elapsed:.6f} seconds")


if __name__ == "__main__":
    main()

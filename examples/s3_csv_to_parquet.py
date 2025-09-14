from evole import Pipeline
from evolve.source import CsvSource
from evolve.target import ParquetTarget


if __name__ == "__main__":
    source = CsvSource("s3://bucket/path/file.csv")
    target = ParquetTarget("s3://bucket/cleaned/file.parquet")
    pipeline = (
        Pipeline("example: S3 csv -> S3 parquet")
        .with_source(source)
        .with_target(target)
    )

    # Pipelines are lazy by design
    pipeline.run()

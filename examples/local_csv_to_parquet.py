import evolve as ev

if __name__ == "__main__":
    source = ev.io.CsvFile("examples/data/dummy.csv")
    target = ev.io.ParquetFile("examples/data/dummy.parquet")
    pipeline = (
        ev.Pipeline("example: S3 csv -> S3 parquet")
        .with_source(source)
        .with_target(target)
    )

    # Pipelines are lazy by design
    pipeline.run()

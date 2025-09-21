import json
import src.evolve as ev

ev.ir.set_global_backend(ev.PolarsBackend())


@ev.monitor_usage(
    interval=0.01,
    logfile="usage.log",
    normalize=True,
    network=True,
    disk=True,
)
def main() -> None:
    print("================== fwf -> csv ==================")
    schema_file = "examples/schemas/example-200MB-schema.json"

    with open(schema_file, "rb") as f:
        schema = json.load(f)

    colspecs = []
    colnames = []
    for col in schema["columns"]:
        colspecs.append((col["offset"], col["length"]))
        colnames.append(col["name"])

    source = ev.io.FixedWidthFile(
        "examples/data/example-200MB.fwf",
        colspecs=colspecs,
        colnames=colnames,
    )

    target = ev.io.CsvFile("example-200MB.csv")

    pipeline = (
        ev.Pipeline("example: S3 csv -> S3 parquet")
        .with_source(source)
        .with_target(target)
    )

    # Pipelines are lazy by design
    pipeline.run()

    print("Reading result:")
    res = ev.io.CsvFile("example-200MB.csv")
    print(res.read().head())


if __name__ == "__main__":
    main()

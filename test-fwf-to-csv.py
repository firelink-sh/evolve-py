import json
from pathlib import Path

from src.evolve.pipeline import Pipeline
from src.evolve.source import CsvSource, FixedWidthSource
from src.evolve.target import CsvTarget

if __name__ == "__main__":
    print(" ========== READING 200MB FWF CONVERTING TO CSV ==========")

    with open("example-fwf-schema.json", "rb") as f:
        schema = json.loads(f.read())

    colspecs = []
    colnames = []
    for col in schema["columns"]:
        colspecs.append((col["offset"], col["length"]))
        colnames.append(col["name"])

    pipeline = (
        Pipeline()
        .with_source(
            FixedWidthSource(
                Path.cwd() / "examples/data/example-200MB.fwf",
                colspecs=colspecs,
                colnames=colnames,
            )
        )
        .with_target(CsvTarget("./test-output-from-fwf.csv"))
    )

    pipeline.run()

    source = CsvSource("test-output-from-fwf.csv")
    print(source.load().get_ir())

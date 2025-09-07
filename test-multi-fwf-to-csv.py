import json

from src.evolve.pipeline import Pipeline
from src.evolve.source import CsvSource, MultiFixedWidthSource
from src.evolve.target import CsvTarget

if __name__ == "__main__":
    print(" ========== READING 200MB FWF CONVERTING TO CSV ==========")

    with open("example-fwf-schema.json", "rb") as f:
        schema1 = json.loads(f.read())

    with open("example-fwf-schema2.json", "rb") as f:
        schema2 = json.loads(f.read())

    colspecs1 = []
    colnames1 = []
    for col in schema1["columns"]:
        colspecs1.append((col["offset"], col["length"]))
        colnames1.append(col["name"])

    colspecs2 = []
    colnames2 = []
    for col in schema2["columns"]:
        colspecs2.append((col["offset"], col["length"]))
        colnames2.append(col["name"])

    schema_map = {
        "SCH1": {
            "colspecs": colspecs1,
            "colnames": colnames1,
        },
        "SCH2": {
            "colspecs": colspecs2,
            "colnames": colnames2,
        },
    }

    pipeline = (
        Pipeline()
        .with_source(
            MultiFixedWidthSource(
                "./examples/data/example-multi-schema.fwf",
                schema_map=schema_map,
                schema_spec_len=4,
            )
        )
        .with_target(CsvTarget("./test-output-from-multi-fwf.csv"))
    )

    pipeline.run()

    source1 = CsvSource("test-output-from-multi-fwf_0.csv")
    print(source1.load().get_ir())

    source2 = CsvSource("test-output-from-multi-fwf_1.csv")
    print(source2.load().get_ir())

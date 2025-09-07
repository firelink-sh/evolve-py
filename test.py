from src.evolve.pipeline import Pipeline
from src.evolve.source import (
    JsonSource,
    ParquetSource,
)
from src.evolve.target import (
    ParquetTarget,
)
from src.evolve.transform import DropColumns

if __name__ == "__main__":
    print("READING JSON CONVERTING TO PARQUET")

    pipeline = (
        Pipeline()
        .with_source(JsonSource("./examples/data/dummy.json"))
        .with_target(
            ParquetTarget(
                "file:///home/tony/git/firelink-sh/evolve-py/xd.parquet",
            ),
        )
        .with_transform(DropColumns("b"))
    )

    pipeline.run()

    source = ParquetSource("xd.parquet")
    print(source.load().get_ir())

    """
    print("======================= JSON =======================")
    # json_source = JsonSource("./examples/data/dummy.json")
    json_source = JsonSource(Path.cwd() / "examples" / "data" / "dummy.json")
    json_table = json_source.load().get_ir().to_arrow()
    print(json_table)
    print(pl.from_arrow(json_table))
    time.sleep(2)

    print("MODIFYING ARROW TABLE")
    even_filter = pc.bit_wise_and(pc.field("b"), pc.scalar(1)) == pc.scalar(0)
    print(pl.from_arrow(json_table))
    json_table = json_table.filter(even_filter)
    print(pl.from_arrow(json_table))
    time.sleep(100)

    print("Writing this JSON to csv...")
    csv_target = CsvTarget("./xd.csv")
    csv_target.write(json_table)
    print("DONE!")
    time.sleep(2)

    print("Writing this JSON to parquet...")
    pq_target = ParquetTarget(
        "file:///home/tony/git/firelink-sh/evolve-py/xd.parquet",
    )
    pq_target.write(json_table)
    print("DONE!")
    time.sleep(2)

    print("======================= CSV (tiny) =======================")
    csv_source = CsvSource("examples/data/dummy.csv")
    csv_table = csv_source.load()
    print(csv_table)
    print(pl.from_arrow(csv_table))
    time.sleep(2)

    print("======================= CSV (small) =======================")
    csv_source = CsvSource(Path.cwd() / "examples" / "data" / "weather.csv")
    csv_table = csv_source.load()
    print(csv_table)
    print(pl.from_arrow(csv_table))
    time.sleep(2)

    print("======================= Parquet =======================")
    pq_source = ParquetSource(
        "file:///home/tony/git/firelink-sh/evolve-py/examples/data/weather.parquet",
    )
    pq_table = pq_source.load()
    print(pq_table)
    print(pl.from_arrow(pq_table))

    print("WRITING BIG PARQUET TO JSON")
    json_target = JsonTarget("./xd.json")
    json_target.write(pq_table)
    print("DONE!")
    time.sleep(2)

    print("READING THE PARQUET WE CREATED FROM THE JSON")
    pq_source = ParquetSource("./xd.parquet")
    pq_table = pq_source.load()
    print(pq_table)
    print(pl.from_arrow(pq_table))
    """

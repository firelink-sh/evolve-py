import time

from pathlib import Path
from src.evolve.source import (
    CsvSource,
    JsonSource,
    ParquetSource,
)

import polars as pl


if __name__ == "__main__":
    print("======================= JSON =======================")
    json_source = JsonSource("./examples/data/dummy.json")
    # source = JsonSource(Path.cwd() / "examples" / "data" / "dummy.json")
    json_table = json_source.load()
    print(json_table)
    print(pl.from_arrow(json_table))
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
        "file:///home/tony/git/firelink-sh/evolve-py/examples/data/weather.parquet"
    )
    pq_table = pq_source.load()
    print(pq_table)
    print(pl.from_arrow(pq_table))

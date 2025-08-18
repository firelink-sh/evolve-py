from pathlib import Path
from src.evolve.source import JsonSource


if __name__ == "__main__":
    source = JsonSource("./examples/data/dummy.json")
    # source = JsonSource(Path.cwd() / "examples" / "data" / "dummy.json")
    df = source.load()
    print(df)

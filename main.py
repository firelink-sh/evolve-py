import time
import json
import pandas as pd

from src.evolve import read_fwf


def main():
    with open("example-schema.json", "rb") as f:
        schema = json.loads(f.read())

    colspecs = []
    colnames = []
    coltypes = {}
    for col in schema["columns"]:
        colspecs.append((col["offset"], col["length"]))
        colnames.append(col["name"])
        coltypes[col["name"]] = col["dtype"].lower()

    t = time.time()
    py_df = read_fwf("example.fwf", colspecs=colspecs, colnames=colnames,
                  coltypes=coltypes)
    print(f"PYTHON NATIVE: {time.time() - t:.4f}s")

    t = time.time()
    pd_df = pd.read_fwf("example.fwf", colspecs=colspecs, names=colnames,
                    converters={
                        "id": int,
                        "name": str,
                        "city": str,
                        "employed": bool,
                        })
    print(f"PANDAS NATIVE: {time.time() - t:.4f}s")

    print(f"python native: {len(py_df)} rows")
    print(f"pandas native: {len(pd_df)} rows")


if __name__ == "__main__":
    main()

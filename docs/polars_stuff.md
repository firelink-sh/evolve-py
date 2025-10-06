## `pl.read_csv` VS `pl.scan_csv`

|**Method**|**Behavior**|**Memory**|**Use case**|
|:--|:--|:--|:--|
|`pl.read_csv("file.csv")`|Reads the **entire CSV into memory immediately**|High for large files|Good for small to medium datasets, fast access|
|`pl.scan_csv("file.csv")`|Creates a **lazy frame**: operations are deferred until `.collect()`|Low initially, computations optimized|Good for large datasets or multiple chained operations|


Good docs on polars + arrow

https://docs.pola.rs/user-guide/misc/arrow/#exporting-data-from-polars-to-pyarrow

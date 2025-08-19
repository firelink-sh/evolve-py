## ETL internal representation considerations

What internal representation should we have?

Arrow Tables vs Polars DataFrames

We are not that compute heavy - mostly I/O bound moving data
from place A to place B, with optional transforms on the single
data sources.

**Arrow Tables** are rock-solid, zero-copy *interchange* format
with a fairly low-level API; **Polars DataFrames** are a high-level,
optimized *compute* engine (built on Arrow memory) with a great UX
for transformations. For a fully fletched ETL framework, standard
is to use **Polars for transforms** and **Arrow at the boundaries** (I/O, interchange).

## PyArrow Tables

- Strengths
    - Standard interchange: everything talks Arrow.
    - Immutable columnar: predictable memory layout, great for scanning.
    - Streaming and chunked data: streaming IPC, good for large incremental pipelines,
    - Stable spec: long-term compatibility guarantees.

- Trade-offs
    - Ergonomics: transform API is low-level; complex transforms become verbose
    - Feature velocity: fewer 'batteries included' for joins/windows functions,
    - Mutations: tables are immutable, many ops create new array/tables - clunky for iterative transforms

## Polars DataFrame

- Strengths
    - Excellent transform UX: expressive *lazy* API with optimizer (predicate/projection pushdown, join reordering, etc.)
    - Performance: multi-threaded Rust engine; very fast group-bys/joins/window ops; good string/date/time-tooling
    - Memory behaviour: columnar, Arrow-backed buffers, copy-on-write (CoW) semantics minimize actual copying in common cases,
    - Interoperability: zero-copy to/from Arrow in many cases.

- Trade-offs
    - UDFs: Python UDFs break optimization and can be slower,
    - Ecosystem: very healthy, but Arrow is still the 'lingua franca' when integrating with everything under the sun.

## Architecture recommendation

A hybrid approach! :)

1. Sources (Frontends)
    - Read using Arrow readers.
    - Convert **zero-copy** into Polars as IR.
2. Transforms (IR modification)
    - Build the transform graph in Polars (lazy).
3. Targets (Backends)
    - Emit Arrow (`df.to_arrow()`)


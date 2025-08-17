<div align="center">

# evolve-py
##### A highly efficient, composable, and lightweight ETL framework, Apache Arrow native.

</div>

> [!IMPORTANT]
> THIS IS CURRENTLY AN EXPERIMENTAL LIBRARY AND UNDERGOES BREAKING CHANGES
> ON A DAILY BASIS. SOON™ evolve WILL BE STABLE AND PROD READY TO USE
> FOR ALL YOUR ETL NEEDS, not all needs (scheduling, job dependency, etc.)
> but this is planned to be solved with another program (munin?)


## Project outline/brainstorming

What is `evolve`?

One-size fits-all framework for taking data from format A to format B and
applying optional transforms T.

Why?
- EL(T) is difficult, costly, without clear standards/frameworks it rapidly becomes messy.
- no "lowcode"/UI/drag and drop shit, made for real data engineers, not business managers
- no vendor lock-in. easy to audit, extend, and run wherever.
- standardized interface/framework - custom logic
- arrow native
  - fast in-memory operations (perfect for OLAP)
  - easy interoperability with DuckDB, Pandas, Polars, Spark, etc.
  - Potential for streaming, GPU acceleration, real-time analytics.
- deployment agnostic (NO LOCK-IN)!!!! YOU RUN IT HOW YOU WANT TO - I COULDN'T CARE LESS
- community potential :)

This is not a replacement for Fivetran or Airbyte - we are offering a **developer-first alternative**
- lightweight
- transparent
- extensible
- free
- high performant

There is no reason to reinvent the wheel for your ETL needs - use evolve!

It might look something like this:

```python
import evolve as ev

from evolve.source import PostgresSource
from evolve.target import Parquet
from evolve.transform import DropNulls

# Pipelines are lazy - only run when told to
pipeline = ev.Pipeline("ingress") \
    .with_source(PostgresSource(...)) \
    .with_target(Parquet(...)) \
    .with_transform(DropNulls(columns=(..., ))

pipeline.run()  # runs the ELT
```


You can configure it with yaml/json/toml!

```yml
source:
  type: postgres
  host: localhost
  db: prod
  user: admin
  password: secret
  schema: sales
  tables: orders

transforms:
  - type: drop_nulls
    columns: ["order_id", "amount"]
  - type: rename_columns
    mapping:
      order_id: id
      amount: total
  - type: filter_rows
    condition: "total > 100"

target:
  type: parquet
  path: s3://prod/sales/orders.parquet
```

```json
{
  "source": {
    ...
  },
  "transforms": {
    ...
  },
  "target": {
    ...
  }
}
```


## Roadmap

### Phase 1: MVP Foundation

**Goal:** build a minimal, working ELT pipeline with Arrow as the backbone.

Core features:
- [ ] `Pipeline` class with source -> transforms -> target
- [ ] Apache Arrow integration (pyarrow.Table)
- [ ] Basic source connectors: PostgreSQL, CSV, Parquet
- [ ] Basic target connectors: PostgreSQL, CSV, Parquet
- [ ] Transform functions: `cast_to`, `drop_nulls`, `rename_columns`, `filter_rows`
- [ ] YAML & JSON config support
- [ ] Config loader and pipeline builder

Dev experience:
- [ ] logging and error handling
- [ ] sample config files and datasets (examples)
- [ ] github repo public with readme and licensing


### Phase 2: Usability & Extensibility

**Goal:** make evolve easy to use, extend, and deploy.

Features:
- [ ] cli tool (`evolve run config.yml`)
- [ ] plugin system for connectors and transforms
- [ ] schema validation and drift detection
- [ ] arrow schema inspection utilities
- [ ] retry and failure handling

Packaging:
- [ ] Publish to PyPI (`pip install evolve-py`)
- [ ] Dockerfile for containerized usage
- [ ] Basic docs site (e.g., GitHub pages or Read The Docs)


### For the future

- [ ] Config encryption
- [ ] metrics and observability hooks
- [ ] parallel extraction and loading
- [ ] streaming mode (Kafka -> Arrow -> Target)
- [ ] Arrow Flight for distributed transport
- [ ] Native support when source = target (no pulling out data unnecessarily).
- [ ] Incremental sync support (CDC or timestamp-based)
- [ ] More sources and targets

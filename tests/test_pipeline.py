import io
from pathlib import Path
from unittest.mock import patch

from evolve.pipeline import Pipeline

DUMMY_YAML = """
    source:
       type: parquet
       path: s3://warehouse/raw/sales/orders.parquet
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
        path: s3:/warehouse/prepared/sales/orders.parquet
    """


def test_from_yaml_str():
    yaml = pipeline = Pipeline.from_yaml_str(DUMMY_YAML)
    print(pipeline)
    assert len(pipeline._transforms) == 3


def test_from_yaml_file():
    with patch.object(Path, "open", return_value=io.StringIO(DUMMY_YAML)):
        pipeline = Pipeline.from_yaml_file("my_pipeline.yml")
        print(pipeline)
        assert len(pipeline._transforms) == 3

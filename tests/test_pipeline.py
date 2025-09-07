from evolve.pipeline import Pipeline


def test_from_yaml_str():
    yaml = """
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
    pipeline = Pipeline.from_yaml_str(yaml)
    print(pipeline)

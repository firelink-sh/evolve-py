from src.evolve.pipeline import Pipeline

if __name__ == "__main__":
    p = Pipeline.from_yaml_file("./example-pipeline.yaml")
    print(p)

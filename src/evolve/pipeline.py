from __future__ import annotations

from .source import Source
from .target import TargetBase
from .transform import Transform


class Pipeline:
    def __init__(self) -> None:
        """Initialize the pipeline."""
        self._transforms = []

    def with_source(self, s: Source) -> Pipeline:
        self._source = s
        return self

    def with_target(self, t: TargetBase) -> Pipeline:
        self._target = t
        return self

    def with_transform(self, t: Transform) -> Pipeline:
        self._transforms.append(t)
        return self

    def run(self) -> None:
        print("Running pipeline")
        print(f"  Loading data from source: '{self._source._name}'")
        ir = self._source.load()
        for transform in self._transforms:
            print(f"  - Applying transform: '{transform.name}'")
            ir = transform.apply(ir)
        print(f"  Writing data to target: '{self._target._name}'")
        self._target.write(ir)

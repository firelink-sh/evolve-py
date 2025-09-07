from __future__ import annotations

import yaml

from .source.base import BaseSource
from .target import TargetBase
from .transform import Transform


class Pipeline:
    """
    Implementation of a `Pipeline` that defines a source to read data
    from, a target to put data to, and optional transforms to apply in-transit.
    """

    @classmethod
    def from_yaml_str(cls, yaml_str: str) -> Pipeline:
        """Create a new `Pipeline` defined in a yaml string."""
        parsed = yaml.safe_load(yaml_str)
        return cls(
            source=parsed["source"],
            target=parsed["target"],
            transforms=parsed["transforms"],
        )

    @classmethod
    def from_yaml_file(cls, yaml_file) -> Pipeline:
        """Create a new `Pipeline` defined in a yaml file."""
        with open(yaml_file, "r") as f:
            yaml_str = f.read()

        parsed = yaml.safe_load(yaml_str)
        return cls(
            source=parsed["source"],
            target=parsed["target"],
            transforms=parsed["transforms"],
        )

    def __init__(self, source=None, target=None, transforms=None) -> None:
        """Initialize the pipeline."""
        if transforms is None:
            transforms = []

        self._source = source
        self._target = target
        self._transforms = transforms

    def __str__(self) -> str:
        s = "Pipeline(\n"
        s += f"  source={self._source}\n"
        s += f"  target={self._target}\n"
        s += "  transforms="
        if self._transforms:
            s += "[\n"
            for t in self._transforms:
                s += f"    {t}\n"
            s += "  ]\n"
        else:
            s += "None\n"
        s += ")"
        return s

    def with_source(self, s: BaseSource) -> Pipeline:
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

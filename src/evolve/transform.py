import abc
from typing import List

from .ir import LazyIR


class Transform(abc.ABC):
    """
    Base class of a transform definition.

    What should a `Transform` do?

    Specify how to modify data (a pyarrow table).
    HOW?

    Apache Arrow tables are immutable by design.

    """

    def __init__(self, name: str) -> None:
        """Initialize a new Transform."""
        self._name = name

    @property
    def name(self) -> str:
        """Get the name of the transform."""
        return self._name

    def apply(self, data: LazyIR) -> LazyIR:
        """Apply the transform on the data."""
        pass


class DropColumns(Transform):
    """Implementation of a DropColumns transform."""

    def __init__(self, columns: str | List[str]) -> None:
        """Initialize a new DropColumns transform."""
        super(DropColumns, self).__init__(name=self.__class__.__name__)
        self._columns = columns

    def apply(self, data: LazyIR) -> LazyIR:
        df = data.get_ir()
        df = df.drop(self._columns, strict=True)
        data.set_ir(df)
        return data

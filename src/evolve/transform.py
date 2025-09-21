import abc

from .ir import IR


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

    def apply(self, data: IR) -> IR:
        """Apply the transform on the data."""
        pass

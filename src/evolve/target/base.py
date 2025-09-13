import abc

from ..ir import (
    IR,
    BaseBackend,
)


class BaseTarget(abc.ABC):
    """Abstract base class for a target (sink)."""

    def __init__(self, name: str, backend: BaseBackend) -> None:
        """Initialize common attributes for the `BaseTarget` object."""
        self._name = name
        self._backend = backend

    @property
    def name(self) -> str:
        """Get the name of the target."""
        return self._name

    @abc.abstractmethod
    def write(self, data: IR) -> None:
        """Write the IR data to the target."""
        pass

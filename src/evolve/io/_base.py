import abc

from ..ir import IR, BaseBackend


class BaseIO(abc.ABC):
    """Abstract base class for an I/O object."""

    def __init__(self, name: str, backend: BaseBackend) -> None:
        """ """
        self._name = name
        self._backend = backend

    @property
    def name(self) -> str:
        """Get the name of the I/O object."""
        return self._name

    @property
    def backend(self) -> BaseBackend:
        """Get the backend of the I/O object."""
        return self._backend

    @abc.abstractmethod
    def load(self) -> IR:
        """Load the data from the source path to the backend IR."""
        pass

    @abc.abstractmethod
    def write(self, data: IR) -> None:
        """Write the backend IR data to the target path.""" 
        pass

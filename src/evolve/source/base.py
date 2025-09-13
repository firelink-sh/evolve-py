import abc

from ..ir import IR, BaseBackend


class BaseSource(abc.ABC):
    """Abstract base class for a source (connector)."""

    def __init__(
        self,
        name: str,
        backend: BaseBackend,
    ) -> None:
        """Initialize common attributes for the `BaseSource` object."""
        self._name = name
        self._backend = backend

    @property
    def name(self) -> str:
        """Get the name of the source."""
        return self._name

    @abc.abstractmethod
    def load(self) -> IR:
        """Load data from the defined source to an ir representation."""
        pass

    @abc.abstractmethod
    def validate_config(self) -> None:
        """
        Validate the provided config for the `BaseSource`.

        Raises
        ------
        InvalidConfigError
            If the provided config was not valid.

        """
        pass

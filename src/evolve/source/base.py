import abc


class BaseSource(abc.ABC):
    """Abstract base class for a connector frontend (source)."""

    def __init__(
        self,
        name: str,
    ) -> None:
        """Initialize base fields for the `BaseSource` object."""
        self._name = name

    @property
    def name(self) -> str:
        """Get the name of the `BaseSource`."""
        return self._name

    @abc.abstractmethod
    def load(self) -> None:
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

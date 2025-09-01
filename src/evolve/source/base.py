import abc


class SourceBase(abc.ABC):
    """Base class for a `Source` connector frontend."""

    def __init__(
        self,
        name: str,
    ) -> None:
        """Initialize base fields for the `Source` object."""
        self._name = name

    @property
    def name(self) -> str:
        """Get the name of the `Source`."""
        return self._name

    @abc.abstractmethod
    def load(self) -> None:
        """Load data from the defined source to an ir representation."""
        pass

    @abc.abstractmethod
    def validate_config(self) -> None:
        """
        Validate the provided config for the `Source`.

        Raises
        ------
        InvalidConfigError
            If the provided config was not valid.

        """
        pass

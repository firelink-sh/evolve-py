class InvalidConfigError(Exception):
    """Raised when a provided config did not pass the defined validation checks."""

    pass


class UnknownUriSchemeError(Exception):
    """Raised when an unsupported URI scheme was encountered."""

    pass

import adbc_driver_sqlite.dbapi as sqlite

from ..ir import IR, BaseBackend, get_global_backend
from ._base import BaseIO


class SQLiteTable(BaseIO):
    """Implementation of an SQLite table."""

    def __init__(
        self, uri: str, *, backend: BaseBackend | None = None, **options
    ) -> None:
        super().__init__(
            name=self.__class__.__name__,
            backend=backend or get_global_backend(),
        )
        connection = sqlite.connect(uri, **options)

        self._uri = uri
        self._connection = connection

    def read(self) -> IR:
        pass

    def write(self, data: IR) -> None:
        pass

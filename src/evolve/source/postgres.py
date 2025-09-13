import duckdb

from ..ir import (
    IR,
    BaseBackend,
    get_global_backend,
)
from .base import BaseSource


class PostgresSource(BaseSource):
    """Implementation of a PostgreSQL table source."""

    def __init__(
        self,
        host: str,
        port: str,
        user: str,
        password: str,
        db: str,
        schema: str,
        table: str,
        backend: BaseBackend | None = None,
    ) -> None:
        """Initialize a new `PostgresSource`."""
        super().__init__(
            name=self.__class__.__name__,
            backend=backend or get_global_backend(),
        )

        conn = duckdb.connect()
        conn.execute("INSTALL postgres_scanner;")
        conn.execute("LOAD postgres_scanner;")

        # SQL injection :)
        query = f"""
        SELECT * FROM postgres_scan(
            'host={host} port={port} dbname={db} user={user} password={password}',
            '{schema}',
            '{table}'
        );
        """

        self._conn = conn
        self._query = query

        self._host = host
        self._port = port
        self._user = user
        self._db = db
        self._schema = schema
        self._table = table

    def load(self) -> IR:
        """Load the PostgreSQL table."""
        if self._backend != get_global_backend():
            raise ValueError("")

        return self._backend.ir_from_arrow_table(
            self._conn.execute(query=self._query).fetch_arrow_table()
        )

    def validate_config(self) -> None:
        pass

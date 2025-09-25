from typing import Iterable

import duckdb

from ..ir import IR, BaseBackend, get_global_backend
from ._base import BaseIO


class PostgresTable(BaseIO):
    """Implementation of a PostgreSQL table."""

    def __init__(
        self,
        host: str,
        port: str,
        user: str,
        password: str,
        db: str,
        schema: str,
        table: str,
        columns: Iterable[str] | None = None,
        backend: BaseBackend | None = None,
    ) -> None:
        """Initialize the `PostgresTable`."""
        super().__init__(
            name=self.__class__.__name__,
            backend=backend or get_global_backend(),
        )

        columns = columns or "*"

        duckdb_pg_secret_name = f"duckdb_postgres_secret_{user}_{db}"
        duckdb_pg_db = f"postgres_{user}_{db}"

        conn = duckdb.connect(database=":memory:")
        conn.execute("INSTALL postgres; LOAD postgres;")

        conn.execute(f"""
        CREATE SECRET {duckdb_pg_secret_name} (
            TYPE postgres,
            HOST '{host}',
            PORT {port},
            DATABASE {db},
            USER '{user}',
            PASSWORD '{password}'
        );
        """)

        conn.execute(f"""
        ATTACH '' AS {duckdb_pg_db} (
            TYPE postgres,
            SECRET {duckdb_pg_secret_name}
        );
        """)

        if not isinstance(columns, str):
            columns = ", ".join(columns)

        read_query = f"""
        SELECT {columns} FROM {duckdb_pg_db}.{schema}.{table};
        """

        print(read_query)

        self._conn = conn
        self._read_query = read_query

        self._host = host
        self._port = port
        self._user = user
        self._db = db
        self._schema = schema
        self._table = table

    def read(self) -> IR:
        """Read the PostgreSQL table."""
        # NOTE: on duckdb backend we dont want to materialize the data
        # immediately - duckdb uses a lazy query execution model - so
        # if we later filter the original select or something we dont
        # want to have to materialize it all in memory beforehand
        # so for duckdb backend we DONT WANT TO ACTUALLY FETCH INTO IR
        # UNLESS WE HAVE TO - BECAUSE DUCKDB IS SMART :) lets see
        # if we can be as smart :)
        return self._backend.ir_from_arrow_table(
            self._conn.execute(self._read_query).fetch_arrow_table()
        )

    def write(self, data: IR) -> None:
        """Write to a postgresql table."""
        pass

    def validate_config(self) -> None:
        """Validate the postgresql config."""
        pass

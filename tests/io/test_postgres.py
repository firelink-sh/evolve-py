import duckdb
from testcontainers.postgres import PostgresContainer

from evolve.io import PostgresTable
from evolve.ir import ArrowBackend, DuckdbBackend


def test_postgres_read_arrow_backend():
    user = "wilhelm"
    password = "123"
    db = "test"

    with PostgresContainer(
        image="postgres:latest",
        username=user,
        password=password,
        dbname=db,
    ) as pg:
        conn = duckdb.connect(database=":memory:")
        conn.execute("INSTALL postgres; LOAD postgres;")

        conn.execute(f"""
        CREATE SECRET postgres_secret (
            TYPE postgres,
            HOST '{pg.get_container_host_ip()}',
            PORT {pg.get_exposed_port(pg.port)},
            DATABASE {db},
            USER '{user}',
            PASSWORD '{password}'
        );
         """)

        conn.execute("""
        ATTACH '' AS pg_db (
            TYPE postgres,
            SECRET postgres_secret
        );
        """)

        conn.execute("CREATE SCHEMA IF NOT EXISTS pg_db.raw; USE pg_db.raw;")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS cool (id uuid, name text, cool boolean);"
        )
        conn.execute(
            "INSERT INTO cool (id, name, cool) VALUES (gen_random_uuid(), 'wilhelm', true);"
        )

        source = PostgresTable(
            host=pg.get_container_host_ip(),
            port=pg.get_exposed_port(pg.port),
            user=user,
            password=password,
            db=db,
            schema="raw",
            table="cool",
            columns=("id", "cool"),
            backend=ArrowBackend(),
        )

        ir = source.read()
        print(ir)


def test_postgres_read_duckdb_backend():
    user = "wilhelm"
    password = "123"
    db = "test"

    with PostgresContainer(
        image="postgres:latest",
        username=user,
        password=password,
        dbname=db,
    ) as pg:
        conn = duckdb.connect(database=":memory:")
        conn.execute("INSTALL postgres; LOAD postgres;")

        conn.execute(f"""
        CREATE SECRET postgres_secret (
            TYPE postgres,
            HOST '{pg.get_container_host_ip()}',
            PORT {pg.get_exposed_port(pg.port)},
            DATABASE {db},
            USER '{user}',
            PASSWORD '{password}'
        );
         """)

        conn.execute("""
        ATTACH '' AS pg_db (
            TYPE postgres,
            SECRET postgres_secret
        );
        """)

        conn.execute("CREATE SCHEMA IF NOT EXISTS pg_db.raw; USE pg_db.raw;")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS cool (id uuid, name text, cool boolean);"
        )
        conn.execute(
            "INSERT INTO cool (id, name, cool) VALUES (gen_random_uuid(), 'wilhelm', true);"
        )

        source = PostgresTable(
            host=pg.get_container_host_ip(),
            port=pg.get_exposed_port(pg.port),
            user=user,
            password=password,
            db=db,
            schema="raw",
            table="cool",
            columns=("name", "id", "cool"),
            backend=DuckdbBackend(),
        )

        ir = source.read()
        print(DuckdbBackend().ir_to_arrow_table(ir))


def test_postgres_read_polars_backend():
    user = "wilhelm"
    password = "123"
    db = "test"

    with PostgresContainer(
        image="postgres:latest",
        username=user,
        password=password,
        dbname=db,
    ) as pg:
        conn = duckdb.connect(database=":memory:")
        conn.execute("INSTALL postgres; LOAD postgres;")

        conn.execute(f"""
        CREATE SECRET postgres_secret (
            TYPE postgres,
            HOST '{pg.get_container_host_ip()}',
            PORT {pg.get_exposed_port(pg.port)},
            DATABASE {db},
            USER '{user}',
            PASSWORD '{password}'
        );
         """)

        conn.execute("""
        ATTACH '' AS pg_db (
            TYPE postgres,
            SECRET postgres_secret
        );
        """)

        conn.execute("CREATE SCHEMA IF NOT EXISTS pg_db.raw; USE pg_db.raw;")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS cool (id uuid, name text, cool boolean);"
        )
        conn.execute(
            "INSERT INTO cool (id, name, cool) VALUES (gen_random_uuid(), 'wilhelm', true);"
        )

        source = PostgresTable(
            host=pg.get_container_host_ip(),
            port=pg.get_exposed_port(pg.port),
            user=user,
            password=password,
            db=db,
            schema="raw",
            table="cool",
            columns=("name", "cool"),
        )

        ir = source.read()
        print(ir.head())

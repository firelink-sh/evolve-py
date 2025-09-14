import sqlalchemy as sa
import duckdb
from testcontainers.postgres import PostgresContainer


def test_pg_duckdb():
    user = "wilhelm"
    password = "darksoul123"
    dbname = "test"
    with PostgresContainer(
        "postgres:latest",
        username=user,
        password=password,
        dbname=dbname,
    ) as pg:
        url = pg.get_connection_url()
        host = pg.get_container_host_ip()
        port = pg.get_exposed_port(pg.port)

        engine = sa.create_engine(url)
        with engine.begin() as conn:
            conn.execute(sa.text("CREATE SCHEMA IF NOT EXISTS raw;"))
            conn.execute(
                sa.text(
                    "CREATE TABLE IF NOT EXISTS raw.users (id uuid, name text, age integer);"
                )
            )
            conn.execute(
                sa.text(
                    "INSERT INTO raw.users (id, name, age) VALUES (gen_random_uuid(), 'wilhelm', 26);"
                )
            )
            conn.commit()

        db = duckdb.connect(database=":memory:")
        db.execute("INSTALL postgres; LOAD postgres;")
        db.execute(
            f"CREATE SECRET pg_secret (TYPE postgres, HOST '{host}', PORT {port}, DATABASE {dbname}, USER '{user}', PASSWORD '{password}');"
        )
        db.execute("ATTACH '' AS pg_db (TYPE postgres, SECRET pg_secret);")
        print(db.execute("SHOW ALL TABLES;").fetch_arrow_table())

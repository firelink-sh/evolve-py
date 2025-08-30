import sqlalchemy
from testcontainers.postgres import PostgresContainer

from evolve.source import PostgresSource


def test_postgres_source():
    with PostgresContainer(
        "postgres:latest", username="xd", password="xdd", dbname="test"
    ) as postgres:
        engine = sqlalchemy.create_engine(postgres.get_connection_url())

        with engine.begin() as conn:
            conn.execute(sqlalchemy.text("CREATE SCHEMA IF NOT EXISTS test;"))
            conn.execute(
                sqlalchemy.text(
                    "CREATE TABLE IF NOT EXISTS test.bananas (name text, size integer);"
                )
            )
            conn.execute(
                sqlalchemy.text(
                    "INSERT INTO test.bananas (name, size) VALUES ('chiquita', 149), ('dark soul', 1894718);"
                )
            )
            conn.commit()

        source = PostgresSource(
            host=postgres.get_container_host_ip(),
            port=postgres.get_exposed_port(postgres.port),
            db="test",
            schema="test",
            table="bananas",
            user="xd",
            password="xdd",
        )

        ir = source.load()
        print(ir.get_ir().head())

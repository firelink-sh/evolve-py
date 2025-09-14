import time
import duckdb
import psycopg2
from testcontainers.postgres import PostgresContainer
from urllib.parse import urlparse

# 1. Spin up a Postgres test container
with PostgresContainer("postgres:latest") as postgres:
    pg_url = (
        postgres.get_connection_url()
    )  # e.g. postgresql://test:test@localhost:5432/test
    print("Postgres URL:", pg_url)

    # Parse connection URL into parts
    parsed = urlparse(pg_url)
    user = parsed.username
    password = parsed.password
    host = parsed.hostname
    port = parsed.port
    dbname = parsed.path.lstrip("/")

    # 2. Create a test table with data
    with psycopg2.connect(
        host=host, port=port, user=user, password=password, dbname=dbname
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE bigtable (id SERIAL PRIMARY KEY, value TEXT)")
            cur.execute(
                "INSERT INTO bigtable (value) SELECT md5(random()::text) FROM generate_series(1, 500000)"
            )
        conn.commit()

    # 3. Open DuckDB connection
    con = duckdb.connect()

    # 4. Benchmark ATTACH + SELECT
    attach_str = (
        f"host={host} port={port} user={user} password={password} dbname={dbname}"
    )
    con.execute(f"ATTACH '{attach_str}' AS pg (TYPE POSTGRES)")

    start = time.time()
    con.execute("SELECT COUNT(*) FROM pg.bigtable").fetchall()
    attach_time = time.time() - start
    print(f"Attach+SELECT COUNT time: {attach_time:.3f}s")

    start = time.time()
    con.execute("SELECT * FROM pg.bigtable").fetchall()  # full table pull
    attach_full_time = time.time() - start
    print(f"Attach+SELECT full-table time: {attach_full_time:.3f}s")

    # 5. Benchmark postgres_scan
    scan_query = f"SELECT * FROM postgres_scan('{attach_str}', 'public', 'bigtable')"

    start = time.time()
    con.execute("SELECT COUNT(*) FROM (" + scan_query + ")").fetchall()
    scan_count_time = time.time() - start
    print(f"postgres_scan COUNT time: {scan_count_time:.3f}s")

    start = time.time()
    con.execute(scan_query).fetchall()
    scan_full_time = time.time() - start
    print(f"postgres_scan full-table time: {scan_full_time:.3f}s")

    # 6. Summary
    print("\nSummary:")
    print(f"  Attach+SELECT full-table: {attach_full_time:.3f}s")
    print(f"  postgres_scan full-table: {scan_full_time:.3f}s")

    # Selective query benchmark
    start = time.time()
    con.execute("SELECT COUNT(*) FROM pg.bigtable WHERE id < 1000").fetchall()
    attach_filtered_time = time.time() - start
    print(f"Attach+SELECT filtered time: {attach_filtered_time:.3f}s")

    start = time.time()
    filtered_scan_query = f"SELECT * FROM postgres_scan('{attach_str}', 'public', 'bigtable') WHERE id < 1000"
    con.execute("SELECT COUNT(*) FROM (" + filtered_scan_query + ")").fetchall()
    scan_filtered_time = time.time() - start
    print(f"postgres_scan filtered time: {scan_filtered_time:.3f}s")

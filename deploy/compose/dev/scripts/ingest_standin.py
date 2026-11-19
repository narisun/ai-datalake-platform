"""Stand-in for the ingest module until dlt ingestion is built.

Copies each sample source table into the raw layer in full, through Trino. It is real
data movement into real Iceberg tables, but it has none of what ingestion needs:
no incremental loads, no watermarks, no schema evolution.
"""

from __future__ import annotations

import trino

TABLES = ["customers", "orders"]


def main() -> None:
    connection = trino.dbapi.connect(host="trino", port=8080, user="svc_pipeline")
    cursor = connection.cursor()
    cursor.execute("create schema if not exists lake.raw")
    cursor.fetchall()
    for table in TABLES:
        cursor.execute(
            f"create or replace table lake.raw.{table} as select * from shop.public.{table}"
        )
        cursor.fetchall()
        cursor.execute(f"select count(*) from lake.raw.{table}")
        print(f"lake.raw.{table}: {cursor.fetchall()[0][0]} rows")


if __name__ == "__main__":
    main()

#!/bin/bash
# Runs once, when the Postgres data volume is first created.
# One server, one database per system, each with its own login.
set -euo pipefail

create() {
  local database="$1" user="$2" password="$3"
  psql -v ON_ERROR_STOP=1 --username postgres <<SQL
create role "$user" login password '$password';
create database "$database" owner "$user";
SQL
}

create openmetadata_db   openmetadata_user "$OPENMETADATA_DB_PASSWORD"
create airflow_db        airflow_user      "$AIRFLOW_DB_PASSWORD"
create polaris           polaris           "$POLARIS_DB_PASSWORD"
create datalake_control  datalake          "$CONTROL_DB_PASSWORD"
create shop              shop              "$SHOP_DB_PASSWORD"

# A small sample source system, so the pipeline has something to ingest.
psql -v ON_ERROR_STOP=1 --username shop --dbname shop <<'SQL'
create table customers (
    customer_id integer primary key,
    full_name   text not null,
    email       text not null,
    country     text not null
);
create table orders (
    order_id    integer primary key,
    customer_id integer not null references customers,
    ordered_at  timestamp not null,
    amount      numeric(18, 2) not null
);
insert into customers values
    (1, 'Ada Lovelace', 'ADA@example.com', 'GB'),
    (2, 'Alan Turing', 'alan@example.com', 'GB'),
    (3, 'Grace Hopper', 'grace@example.com', 'US');
insert into orders values
    (10, 1, '2026-11-01 10:00', 120.50),
    (11, 2, '2026-11-02 11:00', 80.00),
    (12, 1, '2026-11-03 09:30', 42.25),
    (13, 3, '2026-11-04 16:45', 310.00);
SQL

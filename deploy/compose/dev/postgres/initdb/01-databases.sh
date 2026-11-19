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

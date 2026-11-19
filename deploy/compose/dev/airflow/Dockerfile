# OpenMetadata's ingestion image is Airflow plus OpenMetadata's connectors. The platform
# is added in its own virtual environment so its dependencies (dbt, psycopg) can never
# collide with Airflow's. Airflow tasks call it through the `dl` command.
FROM docker.getcollate.io/openmetadata/ingestion:2.0.8

USER root
RUN mkdir -p /opt/datalake/src /opt/datalake/work && chown -R airflow:0 /opt/datalake
USER airflow

COPY --chown=airflow:0 pyproject.toml README.md LICENSE /opt/datalake/src/
COPY --chown=airflow:0 src /opt/datalake/src/src

RUN python -m venv /opt/datalake/venv \
    && /opt/datalake/venv/bin/pip install --no-cache-dir "/opt/datalake/src[dbt,storage]"

# Only `dl` goes on the PATH; the environment's own python must not shadow Airflow's.
USER root
RUN ln -s /opt/datalake/venv/bin/dl /usr/local/bin/dl
USER airflow

ARG GIT_COMMIT=unknown
ENV DL_GIT_COMMIT=${GIT_COMMIT}

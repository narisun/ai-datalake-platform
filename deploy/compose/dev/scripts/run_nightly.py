"""Trigger the nightly pipeline and wait for it. Run inside the airflow container:

    docker compose -f deploy/compose/dev/docker-compose.yml exec airflow \\
        python /opt/datalake/dev/run_nightly.py

Prints each task's final state and exits non-zero if the run failed.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time

import sqlalchemy as sa

URL = "postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{AIRFLOW_DB}".format(
    **os.environ
)
DAG = "nightly"


def main() -> int:
    environment = {**os.environ, "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": URL}
    quiet = {"env": environment, "stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL}
    subprocess.run(["airflow", "dags", "reserialize"], check=False, **quiet)
    subprocess.run(["airflow", "dags", "unpause", DAG], check=False, **quiet)
    run_id = f"manual__{time.strftime('%Y%m%dT%H%M%S')}"
    subprocess.run(["airflow", "dags", "trigger", DAG, "--run-id", run_id], check=True, **quiet)
    print(f"triggered {DAG} as {run_id}")

    engine = sa.create_engine(URL)
    state = "queued"
    for _ in range(180):
        with engine.connect() as connection:
            state = connection.execute(
                sa.text("select state from dag_run where dag_id = :dag and run_id = :run"),
                {"dag": DAG, "run": run_id},
            ).scalar()
        if state in ("success", "failed"):
            break
        time.sleep(5)

    with engine.connect() as connection:
        tasks = connection.execute(
            sa.text(
                "select task_id, state, try_number from task_instance "
                "where dag_id = :dag and run_id = :run order by start_date nulls last"
            ),
            {"dag": DAG, "run": run_id},
        ).fetchall()
    for task_id, task_state, tries in tasks:
        print(f"  {task_id:<16} {task_state}  (try {tries})")
    print(f"run {state}")
    return 0 if state == "success" else 1


if __name__ == "__main__":
    sys.exit(main())

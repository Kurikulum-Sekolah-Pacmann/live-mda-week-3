from pendulum import datetime
from airflow.datasets import Dataset
# from helper.callbacks.slack_notifier import slack_notifier

from cosmos.config import ProjectConfig, ProfileConfig, RenderConfig, ExecutionConfig, ExecutionMode
from cosmos.profiles.postgres import PostgresUserPasswordProfileMapping
from cosmos import DbtDag
from cosmos.constants import TestBehavior
from airflow.decorators import dag

import os

DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/pacbikes_warehouse/pacbikes_warehouse_dbt"

dag = DbtDag(
    dag_id="pacbikes_warehouse",
    schedule='@daily',
    catchup=False,
    start_date=datetime(2024, 10, 1),
    project_config=ProjectConfig(
        dbt_project_path=DBT_PROJECT_PATH,
        project_name="pacbikes_warehouse"
    ),
    profile_config=ProfileConfig(
        profile_name="warehouse",
        target_name="warehouse",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id='warehouse',
            profile_args={"schema": "warehouse"}
        )
    ),
    render_config=RenderConfig(
        dbt_executable_path="/opt/airflow/dbt_venv/bin/dbt",
        emit_datasets=True,
        test_behavior=TestBehavior.AFTER_ALL,
    ),
    operator_args={
        "install_deps": True,  # install any necessary dependencies before running any dbt command
        "full_refresh": True  # used only in dbt commands that support this flag
    }
)
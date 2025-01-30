from pendulum import datetime
from airflow.datasets import Dataset
# from helper.callbacks.slack_notifier import slack_notifier

from cosmos.config import ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles.postgres import PostgresUserPasswordProfileMapping
from cosmos import DbtDag
from cosmos.constants import TestBehavior
from airflow.decorators import dag
from airflow.models import Variable
from cosmos import DbtTaskGroup
from airflow.decorators import dag, task

import os

DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/pacbikes_warehouse/pacbikes_warehouse_dbt"

@dag(
    dag_id='pacbikes_warehouse',
    description='Transform data into warehouse',
    start_date=datetime(2024, 9, 1, tz="Asia/Jakarta"),
    schedule=None,
    catchup=False
)

def pacbikes_warehouse():
    @task.branch
    def check_is_warehouse_init():
        PACBIKES_WAREHOUSE_INIT = Variable.get('PACBIKES_WAREHOUSE_INIT')
        PACBIKES_WAREHOUSE_INIT = eval(PACBIKES_WAREHOUSE_INIT)
        
        if PACBIKES_WAREHOUSE_INIT:
            return "warehouse_init"
        else:
            return "warehouse"
        
    warehouse_init = DbtTaskGroup(
        group_id = "warehouse_init",
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
            test_behavior=TestBehavior.AFTER_ALL
        ),
        operator_args={
            "install_deps": True,  # install any necessary dependencies before running any dbt command
            "full_refresh": True  # used only in dbt commands that support this flag
        }
    )
    
    warehouse = DbtTaskGroup(
        group_id = "warehouse",
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
            exclude = ["dim_date"]
        ),
        operator_args={
            "install_deps": True,  # install any necessary dependencies before running any dbt command
            "full_refresh": True  # used only in dbt commands that support this flag
        }
    )
    
    check_is_warehouse_init() >> [warehouse_init, warehouse]
    
pacbikes_warehouse()
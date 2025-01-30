from airflow.decorators import dag
from pendulum import datetime
from pacbikes_staging.tasks.main import extract, load
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

@dag(
    dag_id='pacbikes_staging',
    description='Extract data and load into staging area',
    start_date=datetime(2024, 9, 1, tz="Asia/Jakarta"),
    schedule="@daily",
    catchup=False
)
def pacbickes_staging():
    incremental_mode = Variable.get('PACBIKES_STAGING_INCREMENTAL_MODE')
    incremental_mode = eval(incremental_mode)
    
    trigger_pacbikes_warehouse = TriggerDagRunOperator(
        task_id='trigger_pacbikes_warehouse',
        trigger_dag_id="pacbikes_warehouse",
        trigger_rule="none_failed"
    )
    
    extract(incremental = incremental_mode) >> load(incremental = incremental_mode) >> trigger_pacbikes_warehouse

pacbickes_staging()
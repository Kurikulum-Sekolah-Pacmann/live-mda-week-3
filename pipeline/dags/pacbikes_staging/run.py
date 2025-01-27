from airflow.decorators import dag
from pendulum import datetime
from pacbikes_staging.tasks.main import extract, load
from airflow.models import Variable

@dag(
    dag_id='pacbikes_staging',
    description='Extract data and load into staging area',
    start_date=datetime(2024, 9, 1, tz="Asia/Jakarta"),
    schedule="@daily",
    catchup=False
)
def pacbickes_staging():
    incremental_mode = Variable.get('PACBIKES_INCREMENTAL_MODE')
    incremental_mode = eval(incremental_mode)
    
    extract(incremental = incremental_mode)

pacbickes_staging()
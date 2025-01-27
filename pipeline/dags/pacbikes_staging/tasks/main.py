from pacbikes_staging.tasks.extract import Extract
from pacbikes_staging.tasks.load import Load

from airflow.decorators import task_group, task
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from airflow.datasets import Dataset

@task_group()
def extract(incremental):
    @task_group()
    def db():
        table_to_extract = eval(Variable.get("PACBIKES_STAGING__table_to_extract_and_load"))
        
        for table_name, info in table_to_extract.items():
            schema = info[0]
            
            current_task = PythonOperator(
                task_id = f"{schema}.{table_name}",
                python_callable=Extract._db,
                op_kwargs={
                    'schema': schema,
                    'table_name': table_name,
                    'incremental': incremental
                }
            )
            
            current_task
            
    @task_group()
    def api():
        url = Variable.get("PACBIKES_API_URL")
        current_task = PythonOperator(
            task_id = "currency_data",
            python_callable=Extract._api,
            op_kwargs={
                'url': url
            }
        )
        
        current_task
        
    db()
    api()
        
@task_group()
def load():
    @task_group()
    def db():
        table_to_load = eval(Variable.get("PACBIKES_STAGING__table_to_extract_and_load"))
        previous_task = None
        
        for table_name, info in table_to_load.items():
            schema = info[0]
            primary_key = info[1]
            
            current_task = PythonOperator(
                task_id = f"staging.{table_name}",
                python_callable=Load.load,
                op_kwargs={
                    'sources': 'db',
                    'schema': schema,
                    'table_name': table_name,
                    'primary_key': primary_key
                }
            )
            
            if previous_task:
                previous_task >> current_task
                
            previous_task = current_task
            
    @task_group
    def api():
        current_task = PythonOperator(
            task_id = "currency_data",
            python_callable=Load.load,
            op_kwargs={
                'sources': 'api',
                'schema': 'staging',
                'table_name': 'currency',
                'primary_key': 'currencycode'
            }
        )
        
        current_task
        
    db()
    api()
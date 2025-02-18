# run.py

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from data_validation.tasks.main import main
from data_validation.tasks.init import create_validation_schema
from helper.callbacks.slack_notifier import slack_notifier
from pendulum import datetime

# For slack alerting
default_args = {
    'on_failure_callback': slack_notifier
}

# Define the DAG with its properties
@dag(
    dag_id='data_validation',
    description='Extract data from warehouse, validate it, and load it for multiple tables',
    start_date=datetime(2024, 9, 1, tz="Asia/Jakarta"),
    schedule="@daily",
    catchup=False,
    default_args=default_args
)
def data_validation():
    """
    DAG function to extract data, validate it and load it to data validation table for multiple tables.
    """

    @task.branch
    def check_is_validation_init() -> str:
        """
        Task to check if the validation schema is initialized.
        """
        PACBIKES_VALIDATION_INIT = Variable.get('PACBIKES_VALIDATION_INIT', default_var="False")
        PACBIKES_VALIDATION_INIT = eval(PACBIKES_VALIDATION_INIT)
        
        if PACBIKES_VALIDATION_INIT:
            return "validation"
        else:
            return "validation_init"

    @task()
    def validation_init():
        create_validation_schema()

    @task()
    def validation(table_details):
        """
        Loop through the table list and run extract, validate, and load for each.
        """
        for table in table_details:
            schema = table['schema']
            table_name = table['table']
            date_columns = table['date_columns']
            unique_column = table['unique_column']
            
            main(schema, table_name, date_columns, unique_column)

    init_check = check_is_validation_init()

    # Fetch the table details from Airflow variable
    table_details = Variable.get("TABLE_LIST", deserialize_json=True)

    # Start tasks
    init = validation_init()
    validation_task = validation(table_details)

    end = EmptyOperator(task_id="end")

    init_check >> [init, validation_task]
    init >> validation_task  # Ensure init runs before validation if needed
    validation_task >> end

# Instantiate the DAG
data_validation()

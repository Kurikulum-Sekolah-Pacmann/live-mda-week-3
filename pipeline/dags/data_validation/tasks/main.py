from airflow.decorators import task_group, task
from data_validation.tasks.extract import Extract
from data_validation.tasks.validate import Validate
from data_validation.tasks.load import Load
from airflow.operators.python import PythonOperator
import pandas as pd

@task_group
def main(table_details: dict):
    """
    Task group to run extract, validate, and load functions.
    """

    @task()
    def extract_data(table):
        """
        Extract data from the database.
        """
        schema = table['schema']
        table_name = table['table']
        return Extract._db(schema, table_name)

    @task()
    def validate_data(data, table):
        """
        Validate the extracted data.
        """
        if data is None:
            return None  # Skip validation if extraction fails

        schema = table['schema']
        table_name = table['table']
        date_columns = table['date_columns']
        unique_column = table['unique_column']

        validation_summary = Validate.run_validation(data, date_columns, unique_column, schema, table_name)
        return validation_summary

    @task()
    def load_data(validation_summary):
        """
        Load the validation summary into the validation table.
        """
        if validation_summary is not None and not validation_summary.empty:
            Load.load(validation_summary, 'validation', 'data_validation')
        else:
            print("No validation errors found.")

    for table in table_details:
        # Use PythonOperator to run the extract, validate, and load functions

        # Extract data for each table using PythonOperator
        extract_task = PythonOperator(
            task_id=f"extract_{table['schema']}_{table['table']}",
            python_callable=extract_data,
            op_kwargs={'table': table}
        )

        # Validate the extracted data using PythonOperator
        validate_task = PythonOperator(
            task_id=f"validate_{table['schema']}_{table['table']}",
            python_callable=validate_data,
            op_kwargs={'data': extract_task.output, 'table': table}
        )

        # Load the validation results using PythonOperator
        load_task = PythonOperator(
            task_id=f"load_{table['schema']}_{table['table']}",
            python_callable=load_data,
            op_kwargs={'validation_summary': validate_task.output}
        )

        # Define task dependencies
        extract_task >> validate_task >> load_task
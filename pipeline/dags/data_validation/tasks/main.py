# data_validation/tasks/main.py

from airflow.decorators import task_group
from airflow.operators.python import PythonOperator
from data_validation.tasks.extract import Extract
from data_validation.tasks.validate import Validate
from data_validation.tasks.load import Load

@task_group()
def main(table_details: list, **kwargs):
    """
    Main task group to extract, validate, and load data for each table.
    """

    for table in table_details:
        schema = table['schema']
        table_name = table['table']
        date_columns = table['date_columns']
        unique_column = table['unique_column']

        # Step 1: Extract Data
        extract_task = PythonOperator(
            task_id=f"extract_{schema}_{table_name}",
            python_callable=Extract._db,
            op_kwargs={
                'schema': schema,
                'table_name': table_name,
                **kwargs
            }
        )

        # Step 2: Validate Data
        validate_task = PythonOperator(
            task_id=f"validate_{schema}_{table_name}",
            python_callable=Validate.run_validation,
            op_kwargs={
                'schema': schema,
                'table_name': table_name,
                'date_columns': date_columns,
                'unique_column': unique_column,
                **kwargs
            }
        )

        # Step 3: Load Validation Results
        load_task = PythonOperator(
            task_id=f"load_{schema}_{table_name}",
            python_callable=Load.load,
            op_kwargs={
                'schema': 'validation',
                'table_name': 'data_validation',
                **kwargs
            }
        )

        # Define task dependencies
        extract_task >> validate_task >> load_task

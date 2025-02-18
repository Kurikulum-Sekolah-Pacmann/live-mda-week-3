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
    def extract_data(table, **context):
        """
        Extract data from the database.
        """
        schema = table['schema']
        table_name = table['table']
        df = Extract._db(schema, table_name)
        
        # Convert DataFrame to dictionary for XCom serialization
        if df is not None:
            return df.to_dict(orient='records')
        return None

    @task()
    def validate_data(table, **context):
        """
        Validate the extracted data.
        """
        # Get data from previous task
        ti = context['task_instance']
        data = ti.xcom_pull(task_ids=f"extract_{table['schema']}_{table['table']}")

        if data is None:
            return None

        # Convert dictionary back to DataFrame
        df = pd.DataFrame(data)
        
        schema = table['schema']
        table_name = table['table']
        date_columns = table['date_columns']
        unique_column = table['unique_column']

        validation_summary = Validate.run_validation(df, date_columns, unique_column, schema, table_name)
        
        if validation_summary is not None:
            return validation_summary.to_dict(orient='records')
        return None

    @task()
    def load_data(validation_summary, **context):
        """
        Load the validation summary into the validation table.
        """
        ti = context['task_instance']
        validation_summary = ti.xcom_pull(task_ids=f"validate_{table['schema']}_{table['table']}")

        if validation_summary is not None:
            # Convert dictionary back to DataFrame
            df = pd.DataFrame(validation_summary)
            if not df.empty:
                Load.load(df, 'validation', 'data_validation')
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
from airflow.decorators import task_group
from airflow.operators.python import PythonOperator
from data_validation.tasks.extract import Extract
from data_validation.tasks.validate import Validate
from data_validation.tasks.load import Load
import pandas as pd

def extract_data(table, **context):
    """
    Extract data from the database.
    """
    schema = table['schema']
    table_name = table['table']
    # Pass the context to Extract._db
    df = Extract._db(schema, table_name, **context)
    
    if df is not None:
        # Convert DataFrame to dictionary for XCom serialization
        df = df.applymap(lambda x: x.isoformat() if isinstance(x, pd.Timestamp) and not pd.isna(x) else (None if pd.isna(x) else x))
        return df.to_dict(orient='records')
    return None

def validate_data(table, **context):
    """
    Validate the extracted data.
    """
    # Get data from previous task
    ti = context['task_instance']
    data = ti.xcom_pull(task_ids=f"validation.extract_{table['schema']}_{table['table']}")
    
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

def load_data(table, **context):
    """
    Load the validation summary into the validation table.
    """
    # Get data from previous task
    ti = context['task_instance']
    validation_summary = ti.xcom_pull(task_ids=f"validation.validate_{table['schema']}_{table['table']}")
    
    if validation_summary is not None:
        # Convert dictionary back to DataFrame
        df = pd.DataFrame(validation_summary)
        if not df.empty:
            Load.load(data=df, schema='data_validation', table_name='data_validation', **context)
    else:
        print("No validation errors found.")

@task_group(group_id='validation')
def main(table_details: dict):
    """
    Task group to run extract, validate, and load functions.
    """
    for table in table_details:
        # Extract data for each table using PythonOperator
        extract_task = PythonOperator(
            task_id=f"extract_{table['schema']}_{table['table']}",
            python_callable=extract_data,
            op_kwargs={'table': table},
            provide_context=True  # Ensure context is provided
        )

        # Validate the extracted data using PythonOperator
        validate_task = PythonOperator(
            task_id=f"validate_{table['schema']}_{table['table']}",
            python_callable=validate_data,
            op_kwargs={'table': table},
            provide_context=True  # Ensure context is provided
        )

        # Load the validation results using PythonOperator
        load_task = PythonOperator(
            task_id=f"load_{table['schema']}_{table['table']}",
            python_callable=load_data,
            op_kwargs={'table': table},
            provide_context=True  # Ensure context is provided
        )

        # Define task dependencies
        extract_task >> validate_task >> load_task
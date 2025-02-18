from airflow.decorators import task_group, task
from data_validation.tasks.extract import Extract
from data_validation.tasks.validate import Validate
from data_validation.tasks.load import Load
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
        extracted_data = extract_data(table)
        validation_summary = validate_data(extracted_data, table)
        load_data(validation_summary)

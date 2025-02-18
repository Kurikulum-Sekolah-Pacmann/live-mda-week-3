# data_validation/tasks/main.py

from data_validation.tasks.extract import Extract
from data_validation.tasks.validate import Validate
from data_validation.tasks.load import Load
import pandas as pd

def main(extract_schema: str, extract_table_name: str, date_columns: list, unique_column: str, **kwargs):
    """
    Main task to run extract, validate, and load functions.
    """

    # Step 1: Extract data
    data = Extract._db(extract_schema, extract_table_name, **kwargs)
    
    if data is not None:
        # Step 2: Run validations
        validation_summary = Validate.run_validation(data, date_columns, unique_column, extract_schema, extract_table_name)
        
        if not validation_summary.empty:
            # Step 3: Load validation summary to validation table
            Load.load(validation_summary, 'validation', 'data_validation', **kwargs)
        else:
            print("No validation errors found.")
    else:
        print("No data extracted to validate.")

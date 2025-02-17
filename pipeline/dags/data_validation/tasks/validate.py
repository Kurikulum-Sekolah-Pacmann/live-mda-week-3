from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from pangres import upsert
from sqlalchemy import create_engine

import pandas as pd
from datetime import datetime

class Validate:
    """
    Class to validate the data extracted.
    """

    @staticmethod
    def validate_missing(data: pd.DataFrame, schema: str, table: str) -> pd.DataFrame:
        """
        Validate missing values in the data and return a summary result.
        """
        missing = data[data.isnull().any(axis=1)]
        total_rows = len(data)
        missing_rows = len(missing)
        percentage_missing = (missing_rows / total_rows) * 100
        status = 'Failed' if missing_rows > 0 else 'Passed'

        return pd.DataFrame([{
            'schema': schema,
            'table_name': table,
            'column': 'N/A',  # No specific column for missing values
            'type_validation': 'Missing Values',
            'percentage': percentage_missing,
            'status': status
        }])

    @staticmethod
    def validate_date_format(data: pd.DataFrame, date_columns: list, schema: str, table: str) -> pd.DataFrame:
        """
        Validate the date format for the given columns and return a summary result.
        """
        date_errors = []
        total_rows = len(data)

        for col in date_columns:
            invalid_date_count = 0
            for idx, value in data[col].iteritems():
                try:
                    datetime.strptime(str(value), "%Y-%m-%d")  # Example format
                except ValueError:
                    invalid_date_count += 1

            percentage_invalid_dates = (invalid_date_count / total_rows) * 100
            status = 'Failed' if invalid_date_count > 0 else 'Passed'
            date_errors.append({
                'schema': schema,
                'table_name': table,
                'column': col,
                'type_validation': 'Date Format',
                'percentage': percentage_invalid_dates,
                'status': status
            })

        return pd.DataFrame(date_errors)

    @staticmethod
    def validate_unique(data: pd.DataFrame, unique_column: str, schema: str, table: str) -> pd.DataFrame:
        """
        Validate uniqueness of a specific column and return a summary result.
        """
        duplicates = data[data.duplicated(subset=[unique_column])]
        total_rows = len(data)
        duplicate_count = len(duplicates)
        percentage_duplicates = (duplicate_count / total_rows) * 100
        status = 'Failed' if duplicate_count > 0 else 'Passed'

        return pd.DataFrame([{
            'schema': schema,
            'table_name': table,
            'column': unique_column,
            'type_validation': 'Uniqueness',
            'percentage': percentage_duplicates,
            'status': status
        }])

    @staticmethod
    def run_validation(data: pd.DataFrame, date_columns: list, unique_column: str, schema: str, table: str) -> pd.DataFrame:
        """
        Run all validation checks on the data and return a summary.
        """
        missing_validation = Validate.validate_missing(data, schema, table)
        date_format_validation = Validate.validate_date_format(data, date_columns, schema, table)
        uniqueness_validation = Validate.validate_unique(data, unique_column, schema, table)

        # Combine all validation results
        all_validations = pd.concat([missing_validation, date_format_validation, uniqueness_validation], ignore_index=True)
        return all_validations

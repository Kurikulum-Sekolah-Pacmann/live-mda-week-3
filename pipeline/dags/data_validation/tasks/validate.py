from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
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
        result = []
        column_list = data.columns
        for col in column_list:
            missing_rows = data[col].isnull().sum()
            total_rows = len(data)
            percentage_missing = (missing_rows / total_rows) * 100 if total_rows > 0 else 0
            status = 'Bad' if percentage_missing >= 10 else 'Good'

            v_result =  {
                'schema': schema,
                'table_name': table,
                'column': col, 
                'type_validation': 'Missing Values',
                'percentage': percentage_missing,
                'status': status
            }
            result.append(v_result)
        return pd.DataFrame(result)

    @staticmethod
    def validate_date_format(data: pd.DataFrame, date_columns: list, schema: str, table: str) -> pd.DataFrame:
        """
        Validate the date format for the given columns and return a summary result.
        Excludes NULL values from the calculation.
        """
        date_errors = []
        
        for col in date_columns:
            valid_values = data[col].dropna()  # Exclude null values
            total_valid_rows = len(valid_values)

            if total_valid_rows == 0:
                percentage_invalid_dates = 0
            else:
                invalid_date_count = sum(
                    not isinstance(value, pd.Timestamp) and pd.to_datetime(value, errors='coerce') is pd.NaT
                    for value in valid_values
                )
                percentage_invalid_dates = (invalid_date_count / total_valid_rows) * 100

            status = 'Bad' if percentage_invalid_dates >= 5 else 'Good'

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
        duplicate_count = data.duplicated(subset=[unique_column]).sum()
        total_rows = len(data)
        percentage_duplicates = (duplicate_count / total_rows) * 100 if total_rows > 0 else 0
        status = 'Bad' if percentage_duplicates > 0 else 'Good'

        return pd.DataFrame([{
            'schema': schema,
            'table_name': table,
            'column': unique_column,
            'type_validation': 'Uniqueness',
            'percentage': percentage_duplicates,
            'status': status
        }])

    @staticmethod
    def validate_negative_values(data: pd.DataFrame, schema: str, table: str) -> pd.DataFrame:
        """
        Validate if any numeric column contains negative values.
        """
        negative_value_errors = []
        total_rows = len(data)

        for col in data.select_dtypes(include=['number']).columns:
            negative_count = (data[col] < 0).sum()
            percentage_negative = (negative_count / total_rows) * 100 if total_rows > 0 else 0
            status = 'Bad' if percentage_negative > 0 else 'Good'

            negative_value_errors.append({
                'schema': schema,
                'table_name': table,
                'column': col,
                'type_validation': 'Negative Values',
                'percentage': percentage_negative,
                'status': status
            })

        return pd.DataFrame(negative_value_errors)

    @staticmethod
    def run_validation(data: pd.DataFrame, date_columns: list, unique_column: str, schema: str, table: str) -> pd.DataFrame:
        """
        Run all validation checks on the data and return a summary.
        """
        missing_validation = Validate.validate_missing(data, schema, table)
        date_format_validation = Validate.validate_date_format(data, date_columns, schema, table)
        uniqueness_validation = Validate.validate_unique(data, unique_column, schema, table)
        negative_values_validation = Validate.validate_negative_values(data, schema, table)

        # Combine all validation results
        all_validations = pd.concat([
            missing_validation, 
            date_format_validation, 
            uniqueness_validation, 
            negative_values_validation
        ], ignore_index=True)
        
        return all_validations

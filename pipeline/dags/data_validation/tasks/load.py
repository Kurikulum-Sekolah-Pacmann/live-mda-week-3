from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from sqlalchemy import create_engine
import pandas as pd

class Load:
    """
    Class to handle loading data to validation table.
    """
    
    @staticmethod
    def load(data: pd.DataFrame, schema: str, table_name: str, **context) -> None:
        """
        Load validation summary data into the PostgreSQL validation table.

        :param data: Dataframe containing validation summary result.
        :param schema: Database schema name.
        :param table_name: Table name in the database.
        :param kwargs: Additional keyword arguments.
        """
        ti = context['task_instance']
        execution_date = context['execution_date']
        extract_info = ti.xcom_pull(key=f"extract_info-{schema}.{table_name}")
        
        # Skip if no new data
        if extract_info and extract_info.get("status") == "skipped":
            raise AirflowSkipException(f"There is no data for '{schema}.{table_name}'. Skipped...")
        
        # Check if validation data is empty
        if data.empty:
            print(f"No validation errors for '{schema}.{table_name}'. Nothing to load.")
            return

        # Create PostgreSQL engine
        postgres_uri = PostgresHook(postgres_conn_id='warehouse').get_uri()
        engine = create_engine(postgres_uri)

        # Ensure the validation summary has the necessary columns (schema, table_name, column, type_validation, percentage, status)
        data['schema'] = schema
        data['table_name'] = table_name

        # Insert data into the validation table
        data.to_sql('data_validation', con=engine, schema=schema, if_exists='append', index=False)

        # Dispose the engine
        engine.dispose()

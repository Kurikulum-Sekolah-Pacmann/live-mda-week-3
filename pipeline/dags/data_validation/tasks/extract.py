from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException, AirflowException
from datetime import timedelta
import pandas as pd
import pytz

class Extract:
    """
    A class used to extract data from a database or an API and push it to S3.
    """

    @staticmethod
    def _db(schema: str, table_name: str, **context) -> pd.DataFrame:
        """
        Extract data from a PostgreSQL database and push it to S3.

        Parameters:
        schema (str): The schema name.
        table_name (str): The table name.
        context: Airflow context containing task instance and other metadata.
        """
        try:
            # Get task instance from context
            ti = context['task_instance']
            execution_date = context['execution_date']

            execution_date = execution_date.isoformat()
  
            # Connect to PostgreSQL database
            pg_hook = PostgresHook(postgres_conn_id='warehouse')
            connection = pg_hook.get_conn()
            cursor = connection.cursor()

            # Formulate the extract query
            extract_query = f"SELECT * FROM {schema}.{table_name}"
                
            # Execute the query and fetch results
            cursor.execute(extract_query)
            result = cursor.fetchall()
            column_list = [desc[0] for desc in cursor.description]
            cursor.close()
            connection.commit()
            connection.close()

            # Convert results to DataFrame
            df = pd.DataFrame(result, columns=column_list)

            # Check if DataFrame is empty and handle accordingly
            if df.empty:
                ti.xcom_push(
                    key=f"extract_info-{schema}.{table_name}", 
                    value={"status": "skipped", "data_date": execution_date}
                )
                raise AirflowSkipException(f"Table '{schema}.{table_name}' doesn't have data. Skipped...")
            else:
                ti.xcom_push(
                    key=f"extract_info-{schema}.{table_name}", 
                    value={"status": "success", "data_date": execution_date}
                )
                
                return df
        except AirflowSkipException as e:
            raise e
        
        except Exception as e:
            raise AirflowException(f"Error when extracting {schema}.{table_name} : {str(e)}")
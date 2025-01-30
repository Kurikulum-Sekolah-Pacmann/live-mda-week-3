from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from airflow import AirflowException
from helper.s3 import S3
import pandas as pd
import pytz
import requests
from datetime import timedelta

class Extract:
    @staticmethod
    def _db(schema, table_name, incremental, **kwargs):
        try:
            ti = kwargs['ti']
            execution_date = ti.execution_date
            tz = pytz.timezone('Asia/Jakarta')
            execution_date = execution_date.astimezone(tz)
            data_date = (pd.to_datetime(execution_date) - timedelta(days=1)).strftime("%Y-%m-%d")
            
            pg_hook = PostgresHook(postgres_conn_id='pacbikes-db')
            connection = pg_hook.get_conn()
            cursor = connection.cursor()

            extract_query = f"SELECT * FROM {schema}.{table_name}"
            if incremental:
                extract_query += f" WHERE modifieddate::DATE = '{data_date}'::DATE;"                
                object_name = f"pacbikes-db/{schema}/{table_name}/{data_date}.csv"
            
            else:
                object_name = f"pacbikes-db/{schema}/{table_name}/full_data.csv"
                
            cursor.execute(extract_query)
            result = cursor.fetchall()
            cursor.close()
            connection.commit()
            connection.close()

            column_list = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(result, columns=column_list)

            
            if df.empty:
                ti.xcom_push(
                    key = f"extract_info-{schema}.{table_name}", 
                    value = {
                        "status": "skipped",
                        "data_date":data_date
                    }
                )
                raise AirflowSkipException(f"Table '{schema}.{table_name}' doesn't have new data. Skipped...")
            
            else:
                ti.xcom_push(
                    key = f"extract_info-{schema}.{table_name}", 
                    value = {
                        "status": "success",
                        "data_date":data_date
                    }
                )            
                
                S3.push(
                    aws_conn_id = 's3-conn',
                    bucket_name = 'pacbikes',
                    key = object_name,
                    string_data = df.to_csv(index=False)
                )
            
        except AirflowSkipException as e:
            raise e
        
        except AirflowException as e:
            raise AirflowException(f"Error when extracting {schema}.{table_name} : {str(e)}")
    
    @staticmethod
    def _api(url):
        data_json =  requests.get(url).json()
        df = pd.DataFrame(data_json)
        
        S3.push(
            aws_conn_id='s3-conn',
            bucket_name='pacbikes',
            key='pacbikes-api/data.csv',
            string_data=df.to_csv(index=False)
        )
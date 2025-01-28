from helper.s3 import S3
from airflow.exceptions import AirflowException, AirflowSkipException
from pangres import upsert
from sqlalchemy import create_engine
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import timedelta

import pandas as pd

class Load:
    @staticmethod
    def load(sources, schema, table_name, primary_key, incremental, **kwargs):
        ti = kwargs['ti']
        
        if sources == 'db':
            execution_date = ti.execution_date
            last_extract = ti.xcom_pull(key=f"last_extract-{schema}.{table_name}")
            
            if last_extract is None:
                raise AirflowException(f"Last extract date for {schema}.{table_name} is not found")
            
            if incremental:
                key = f"pacbikes-db/{schema}/{table_name}/{(pd.to_datetime(execution_date) - timedelta(days=1)).strftime("%Y-%m-%d")}.csv"
                
            else:
                key = f"pacbikes-db/{schema}/{table_name}/full_data.csv"
            
            df = S3.pull(
                aws_conn_id='s3-conn',
                bucket_name='pacbikes',
                key = key
            )
        else:
            df = S3.pull(
                aws_conn_id='s3-conn',
                bucket_name='pacbikes',
                key=f"pacbikes-api/data.csv"
            )
        
        df = df.set_index(primary_key)
        postgres_uri = PostgresHook(postgres_conn_id='warehouse').get_uri()
        engine = create_engine(postgres_uri)
        
        upsert(
            con=engine,
            df=df,
            table_name=table_name,
            schema='pacbikes_staging',
            if_row_exists='update'
        )
        
        engine.dispose()
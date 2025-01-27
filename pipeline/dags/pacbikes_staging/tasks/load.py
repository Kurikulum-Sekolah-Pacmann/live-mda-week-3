from helper.s3 import S3
from airflow.exceptions import AirflowException, AirflowSkipException
import pandas as pd
from pangres import upsert
from sqlalchemy import create_engine
from airflow.providers.postgres.hooks.postgres import PostgresHook

class Load:
    @staticmethod
    def load(sources, schema, table_name, primary_key, **kwargs):
        ti = kwargs['ti']
        
        if sources == 'db':
            execution_date = ti.execution_date
            last_extract = ti.xcom_pull(key=f"last_extract-{schema}.{table_name}")
            print("LAST EXTRACT DATE:", last_extract)
            
            if last_extract is None:
                raise AirflowException(f"Last extract date for {schema}.{table_name} is not found")
            
            df = S3.pull(
                aws_conn_id='s3-conn',
                bucket_name='pacbikes',
                key=f"pacbikes-db/{schema}/{table_name}/{last_extract.strftime('%Y-%m-%d')}.csv"
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
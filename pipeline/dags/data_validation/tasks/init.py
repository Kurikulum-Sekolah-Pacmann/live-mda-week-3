from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine

def create_validation_schema():
    """
    This function will create the validation schema and tables.
    """
    
    # Create PostgreSQL engine
    postgres_uri = PostgresHook(postgres_conn_id='warehouse').get_uri()
    engine = create_engine(postgres_uri)
    sql = """
    CREATE SCHEMA IF NOT EXISTS data_validation;
    
    CREATE TABLE IF NOT EXISTS validation.data_validation (
        validation_id SERIAL PRIMARY KEY,
        schema VARCHAR(100),
        table_name VARCHAR(100),
        "column" VARCHAR(100),
        type_validation VARCHAR(50),
        percentage DECIMAL(5,2),
        status VARCHAR(20),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    """
    with engine.connect() as connection:
        connection.execute(sql)
    
    # Dispose the engine
    engine.dispose()

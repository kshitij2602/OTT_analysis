from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import pandas as pd
from sqlalchemy import create_engine

# PostgreSQL connection details
POSTGRES_CONFIG = {
    "host": "localhost",
    "dbname": "media_data_lake",
    "user": "postgres",  # Replace with your PostgreSQL username
    "password": "1234",  # Replace with your PostgreSQL password
    "port": 5432
}

# Path to the main data lake folder (Update this path as per your WSL setup)
DATA_LAKE_PATH = "/home/dishant/de_project/airflow_venv/Data_lake"  # WSL Linux path

def process_data_lake():
    """
    Dynamically process all subfolders and Excel files in the data lake and store data in PostgreSQL.
    """
    # Check if the data lake folder exists
    if not os.path.exists(DATA_LAKE_PATH):
        raise FileNotFoundError(f"Data lake path does not exist: {DATA_LAKE_PATH}")
    
    print(f"Data lake path exists: {DATA_LAKE_PATH}")
    
    # Connect to PostgreSQL
    try:
        engine = create_engine(f"postgresql://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}@"
                               f"{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['dbname']}")
        print("Successfully connected to PostgreSQL.")
    except Exception as e:
        raise ConnectionError(f"Error connecting to PostgreSQL: {e}")

    # Iterate over each subfolder in the data lake
    for folder in os.listdir(DATA_LAKE_PATH):
        folder_path = os.path.join(DATA_LAKE_PATH, folder)
        
        if os.path.isdir(folder_path):  # Check if it's a directory
            # Process 'credits.xls'
            credits_path = os.path.join(folder_path, "credits.xls")
            if os.path.exists(credits_path):
                try:
                    credits_df = pd.read_excel(credits_path)
                    credits_table = f"{folder.lower()}_credits"  # Table name, e.g., amazon_credits
                    credits_df.to_sql(credits_table, engine, if_exists='replace', index=False)
                    print(f"Loaded data into table: {credits_table}")
                except Exception as e:
                    print(f"Error processing {credits_path}: {e}")
            
            # Process 'titles.xls'
            titles_path = os.path.join(folder_path, "titles.xls")
            if os.path.exists(titles_path):
                try:
                    titles_df = pd.read_excel(titles_path)
                    titles_table = f"{folder.lower()}_titles"  # Table name, e.g., amazon_titles
                    titles_df.to_sql(titles_table, engine, if_exists='replace', index=False)
                    print(f"Loaded data into table: {titles_table}")
                except Exception as e:
                    print(f"Error processing {titles_path}: {e}")

# Define the Airflow DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='data_lake_to_postgres_main',
    default_args=default_args,
    schedule_interval='@daily',  
    catchup=False
) as dag:

    # Define the task to process the data lake
    load_data_task = PythonOperator(
        task_id='process_data_lake',
        python_callable=process_data_lake,
    )

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import csv
import os

# Define the function to read the CSV
def read_csv_file():
    csv_path = "/home/dishant/de_project/Data_lake/amazon_data/titles.csv"  # Update this to your file path
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"The file {csv_path} does not exist.")

    with open(csv_path, mode='r') as file:
        # reader = csv.reader(file)
        # print("CSV Content:")
        # for row in reader:
        #     print(row)
        #     with open(csv_path, mode='r') as file:
         reader = csv.reader(file)
         print("CSV Content:")
         count = 0
         for row in reader:
            if count < 5:
               print(row)
               count += 1
            else:
              break

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='read_csv_example',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    read_csv_task = PythonOperator(
        task_id='read_csv_task',
        python_callable=read_csv_file,
    )

    read_csv_task

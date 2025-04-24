from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import pandas as pd

# Function to merge and save titles files
def merge_titles_files():
    base_folder = '/home/dishant/de_project/airflow_venv/Data_lake'
    subfolders = ['amazon_data', 'paramount_data', 'hulu_data', 'hbo_data', 'netflix_data', 'disney_data']
    output_folder = os.path.join(base_folder, 'release_year_genre')
    output_file_path = os.path.join(output_folder, 'merged_titles.xlsx')

    # Create output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)

    combined_data = []

    for subfolder in subfolders:
        subfolder_path = os.path.join(base_folder, subfolder)
        titles_file_path = os.path.join(subfolder_path, 'titles.csv')

        if os.path.exists(titles_file_path):
            try:
                # Read the CSV file
                df = pd.read_csv(titles_file_path)

                # Check if necessary columns exist
                if 'release_year' in df.columns and 'genres' in df.columns:
                    # Append the required columns
                    combined_data.append(df[['release_year', 'genres']])
                else:
                    print(f"Required columns missing in {titles_file_path}")
            except Exception as e:
                print(f"Error processing {titles_file_path}: {e}")
        else:
            print(f"File not found: {titles_file_path}")

    if combined_data:
        # Combine all data into a single DataFrame
        merged_df = pd.concat(combined_data, ignore_index=True)

        # Save to an Excel file
        merged_df.to_excel(output_file_path, index=False)
        print(f"Merged file saved at {output_file_path}")
    else:
        print("No data found to merge.")

# Define the DAG
with DAG(
    dag_id='merge_titles_to_excel',
    description='Merge all titles.csv files and save as a single Excel file with release_year and genres',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2025, 1, 8),
    catchup=False,
) as dag:

    # Define the PythonOperator
    merge_files_task = PythonOperator(
        task_id='merge_titles_files',
        python_callable=merge_titles_files,
    )

    merge_files_task

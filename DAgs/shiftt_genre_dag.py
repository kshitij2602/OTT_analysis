from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging
from datetime import datetime

def analyze_tmdb_popularity():
    base_folder = '/home/dishant/de_project/airflow_venv/Data_lake'
    excel_files = []

    # Subfolders with the data
    subfolders = ['amazon_data', 'paramount_data', 'hbo_data', 'netflix_data', 'disney_data']
    
    for subfolder in subfolders:
        subfolder_path = os.path.join(base_folder, subfolder)
        files_to_read = ['credits.csv', 'titles.csv']
        
        for file_name in files_to_read:
            file_path = os.path.join(subfolder_path, file_name)
            
            if os.path.exists(file_path):
                try:
                    df = pd.read_csv(file_path)
                    excel_files.append(df)
                    logging.info(f"Successfully read {file_path} with columns: {df.columns.tolist()}")
                except Exception as e:
                    logging.error(f"Error reading {file_path}: {e}")
            else:
                logging.warning(f"File {file_name} does not exist in {subfolder_path}")

    if excel_files:
        df = pd.concat(excel_files, ignore_index=True)
        required_columns = ['genres', 'release_year', 'tmdb_popularity']
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            logging.warning(f"Missing columns {missing_columns} in the file. Skipping this file.")
        else:
            df = df.dropna(subset=required_columns)
            df['release_year'] = pd.to_numeric(df['release_year'], errors='coerce')
            df = df.dropna(subset=['release_year'])

            # Normalize genres by sorting them alphabetically
            def normalize_genres(genres_str):
                try:
                    genres_list = eval(genres_str)
                    return str(sorted(genres_list))
                except:
                    return genres_str
            
            if 'genres' in df.columns:
                df['genres'] = df['genres'].apply(normalize_genres)

            # Line Chart
            plt.figure(figsize=(14, 8))
            sns.lineplot(data=df, x='release_year', y='tmdb_popularity', hue='genres', palette='tab20', linewidth=2)
            plt.title('TMDB Popularity Over Time by Genre', fontsize=16)
            plt.xlabel('Release Year', fontsize=14)
            plt.ylabel('TMDB Popularity', fontsize=14)
            plt.xticks(rotation=45)
            plt.tight_layout()
            line_chart_path = '/home/dishant/de_project/airflow_venv/tmdb_popularity_over_time.png'
            plt.savefig(line_chart_path)
            plt.close()
            logging.info(f"Line chart saved at: {line_chart_path}")

            # # Histogram
            # plt.figure(figsize=(14, 8))
            # sns.histplot(data=df, x='tmdb_popularity', hue='genres', multiple='stack', palette='tab20', kde=True)
            # plt.title('Distribution of TMDB Popularity by Genre', fontsize=16)
            # plt.xlabel('TMDB Popularity', fontsize=14)
            # plt.ylabel('Frequency', fontsize=14)
            # plt.tight_layout()
            # histogram_path = '/home/dishant/de_project/airflow_venv/tmdb_popularity_distribution.png'
            # plt.savefig(histogram_path)
            # plt.close()
            # logging.info(f"Histogram saved at: {histogram_path}")
    else:
        logging.warning("No data to process. No files were read.")

# Define the DAG
with DAG(
    dag_id='analyze_tmdb_popularity_dag',
    description='DAG to analyze tmdb popularity by genre over time from CSV data',
    schedule_interval=None,  # Set the schedule for the DAG, use `None` for manual trigger
    start_date=datetime(2025, 1, 6),
    catchup=False,
) as dag:

    # Task to analyze tmdb popularity
    analyze_tmdb_popularity_task = PythonOperator(
        task_id='analyze_tmdb_popularity',
        python_callable=analyze_tmdb_popularity,
    )

    analyze_tmdb_popularity_task

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import pandas as pd
import ast
import matplotlib.pyplot as plt
import seaborn as sns

# Function to extract the first genre
def extract_first_genre(genres_str):
    try:
        genres_list = ast.literal_eval(genres_str)
        if isinstance(genres_list, list) and genres_list:
            return genres_list[0]
        else:
            return "Unknown"
    except (ValueError, SyntaxError):
        return "Unknown"

# Function to process and visualize average runtime per platform
def process_and_visualize_average_runtime():
    base_folder = '/home/dishant/de_project/airflow_venv/Data_lake'
    subfolders = ['amazon_data', 'paramount_data',  'hbo_data', 'netflix_data', 'disney_data']
    all_data = []

    for subfolder in subfolders:
        subfolder_path = os.path.join(base_folder, subfolder)
        titles_file_path = os.path.join(subfolder_path, 'titles.csv')

        if os.path.exists(titles_file_path):
            try:
                # Read the CSV file
                df = pd.read_csv(titles_file_path)

                # Check if 'runtime' and platform columns exist
                if 'runtime' in df.columns and 'platform' in df.columns:
                    # Keep only relevant columns
                    df = df[['platform', 'runtime']].dropna()

                    # Append to the combined dataset
                    all_data.append(df)
            except Exception as e:
                print(f"Error processing {titles_file_path}: {e}")

    # Combine all data
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)

        # Group by platform and calculate average runtime
        platform_runtime = combined_df.groupby('platform')['runtime'].mean().sort_values(ascending=False)

        # Plot the data
        plt.figure(figsize=(12, 8))
        bar_plot = sns.barplot(x=platform_runtime.index, y=platform_runtime.values, palette="viridis")
        plt.title('Average Runtime by Platform', fontsize=16)
        plt.xlabel('Platform', fontsize=14)
        plt.ylabel('Average Runtime (minutes)', fontsize=14)
        plt.xticks(rotation=45)
        plt.tight_layout()

        # Annotate each bar with its value
        for index, value in enumerate(platform_runtime.values):
            bar_plot.text(index, value + 1, f'{value:.2f}', ha='center', fontsize=12, color='black')

        # Corrected output path
        output_path = '/home/dishant/de_project/airflow_venv/average_runtime_by_platform.png'
        plt.savefig(output_path)
        plt.close()
        print(f"Plot saved at {output_path}")
    else:
        print("No data found to process.")

# Define the DAG
with DAG(
    dag_id='average_runtime_by_platform',
    description='Analyze and visualize average runtime by platform',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2025, 1, 8),
    catchup=False,
) as dag:

    # Define the PythonOperator
    analyze_average_runtime = PythonOperator(
        task_id='analyze_and_visualize_runtime',
        python_callable=process_and_visualize_average_runtime,
    )

    analyze_average_runtime

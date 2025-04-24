from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Function to generate a single image containing scatter plots for runtime vs average_score
def process_and_visualize_runtime_vs_average_score():
    base_folder = '/home/dishant/de_project/airflow_venv/Data_lake'
    subfolders = ['amazon_data', 'paramount_data', 'hulu_data', 'hbo_data', 'netflix_data', 'disney_data']

    # Set up the grid for subplots (2 rows, 3 columns since we have 6 subfolders)
    fig, axes = plt.subplots(nrows=2, ncols=3, figsize=(18, 10))
    axes = axes.flatten()  # Flatten the 2D array of axes into 1D for easier access

    for i, subfolder in enumerate(subfolders):
        subfolder_path = os.path.join(base_folder, subfolder)
        titles_file_path = os.path.join(subfolder_path, 'titles.csv')

        if os.path.exists(titles_file_path):
            try:
                # Read the CSV file
                df = pd.read_csv(titles_file_path)

                # Check if 'runtime' and 'average_score' columns exist
                if 'runtime' in df.columns and 'average_score' in df.columns:
                    # Drop rows with missing data in 'runtime' or 'average_score'
                    df = df.dropna(subset=['runtime', 'average_score'])

                    # Create scatter plot on the respective subplot axis
                    sns.scatterplot(data=df, x='runtime', y='average_score', alpha=0.7, color='blue', ax=axes[i])
                    axes[i].set_title(f'Scatter Plot: Runtime vs Average Score ({subfolder})', fontsize=12)
                    axes[i].set_xlabel('Runtime (minutes)', fontsize=10)
                    axes[i].set_ylabel('Average Score', fontsize=10)
                    axes[i].grid(True)

                else:
                    print(f"'runtime' or 'average_score' column not found in {titles_file_path}")
            except Exception as e:
                print(f"Error processing {titles_file_path}: {e}")
        else:
            print(f"File not found: {titles_file_path}")

    # Adjust layout for better spacing
    plt.tight_layout()

    # Save the combined plot
    plot_output_path = os.path.join(base_folder, "runtime_vs_average_score_all_subfolders.png")
    plt.savefig(plot_output_path)
    plt.close()
    print(f"Combined scatter plot saved at {plot_output_path}")

# Define the DAG
with DAG(
    dag_id='runtime_vs_average_score',
    description='Analyze and visualize runtime vs average_score',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2025, 1, 8),
    catchup=False,
) as dag:

    # Define the PythonOperator
    analyze_runtime_vs_average_score = PythonOperator(
        task_id='analyze_and_visualize_runtime_vs_average_score',
        python_callable=process_and_visualize_runtime_vs_average_score,
    )

    analyze_runtime_vs_average_score

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Function to process and create line charts for release_year vs average_score
def process_and_visualize_release_year_vs_average_score():
    base_folder = '/home/dishant/de_project/airflow_venv/Data_lake'
    subfolders = ['amazon_data', 'paramount_data', 'hulu_data', 'hbo_data', 'netflix_data', 'disney_data']
    
    # List to store data from all platforms
    all_data = []

    for subfolder in subfolders:
        subfolder_path = os.path.join(base_folder, subfolder)
        titles_file_path = os.path.join(subfolder_path, 'titles.csv')

        if os.path.exists(titles_file_path):
            try:
                # Read the CSV file
                df = pd.read_csv(titles_file_path)

                # Check if required columns exist
                if 'release_year' in df.columns and 'average_score' in df.columns and 'genres' in df.columns:
                    # Keep only necessary columns and drop NaN values
                    df = df[['release_year', 'average_score', 'genres']].dropna()
                    
                    # Extract the first genre from 'genres'
                    df['genre'] = df['genres'].str.extract(r"\['([^']+)")
                    
                    # Append data to the combined dataset
                    all_data.append(df)
            except Exception as e:
                print(f"Error processing {titles_file_path}: {e}")

    # Combine all data
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)

        # Convert release_year to numeric for sorting
        combined_df['release_year'] = pd.to_numeric(combined_df['release_year'], errors='coerce')

        # Drop rows with invalid release_year
        combined_df = combined_df.dropna(subset=['release_year'])

        # Group by genre for visualization
        unique_genres = combined_df['genre'].unique()

        # Set up a grid for subplots
        num_genres = len(unique_genres)
        cols = 3  # Number of columns for subplots
        rows = (num_genres + cols - 1) // cols  # Calculate rows needed
        fig, axes = plt.subplots(rows, cols, figsize=(15, rows * 5), constrained_layout=True)

        # Flatten axes for easy iteration
        axes = axes.flatten()

        for idx, genre in enumerate(unique_genres):
            genre_data = combined_df[combined_df['genre'] == genre]
            genre_data = genre_data.groupby('release_year')['average_score'].mean().reset_index()

            # Plot for each genre
            ax = axes[idx]
            sns.lineplot(data=genre_data, x='release_year', y='average_score', ax=ax, marker='o')
            ax.set_title(f'Genre: {genre}', fontsize=14)
            ax.set_xlabel('Release Year', fontsize=12)
            ax.set_ylabel('Average Score', fontsize=12)
            ax.grid(True)

        # Hide unused subplots
        for i in range(idx + 1, len(axes)):
            axes[i].set_visible(False)

        # Save the combined image
        output_path = '/home/dishant/de_project/airflow_venv/release_year_vs_average_score_line_charts.png'
        plt.savefig(output_path)
        plt.close()
        print(f"Line charts saved at {output_path}")
    else:
        print("No data found to process.")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 8),
    'retries': 1,
}

# DAG definition
with DAG(
    dag_id='release_year_vs_average_score_line_charts_dag',
    default_args=default_args,
    description='A simple DAG to visualize release year vs average score for different genres',
    schedule_interval=None,  # Set to None to run manually or specify interval
    catchup=False,
) as dag:

    # Task to process and visualize release_year vs average_score line charts
    visualize_release_year_task = PythonOperator(
        task_id='process_and_visualize_release_year_vs_average_score',
        python_callable=process_and_visualize_release_year_vs_average_score,
        dag=dag,
    )

    # Set the task sequence (if any)
    visualize_release_year_task

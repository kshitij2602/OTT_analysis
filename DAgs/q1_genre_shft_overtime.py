from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Function to generate histograms for total number of movie releases by genre per year
def process_and_visualize_movie_releases_by_genre():
    base_folder = '/home/dishant/de_project/airflow_venv/Data_lake'
    subfolders = ['amazon_data', 'paramount_data', 'hulu_data', 'hbo_data', 'netflix_data', 'disney_data']
    combined_data = []

    for subfolder in subfolders:
        subfolder_path = os.path.join(base_folder, subfolder)
        titles_file_path = os.path.join(subfolder_path, 'titles.csv')

        if os.path.exists(titles_file_path):
            try:
                print(f"Processing file: {titles_file_path}")
                df = pd.read_csv(titles_file_path)
                print(f"DataFrame shape before filtering: {df.shape}")

                if 'release_year' in df.columns and 'genres' in df.columns:
                    df = df.dropna(subset=['release_year', 'genres'])
                    print(f"DataFrame shape after filtering: {df.shape}")

                    df['genre'] = df['genres'].str.extract(r"\['([^']+)")
                    combined_data.append(df[['release_year', 'genre']])
                else:
                    print(f"Required columns missing in {titles_file_path}")
            except Exception as e:
                print(f"Error processing {titles_file_path}: {e}")
        else:
            print(f"File not found: {titles_file_path}")

    if combined_data:
        combined_df = pd.concat(combined_data, ignore_index=True)
        print(f"Number of combined entries: {len(combined_df)}")
        print(f"Unique genres extracted: {combined_df['genre'].unique()}")

        output_dir = os.path.join(base_folder, 'Question_1_Genres')
        os.makedirs(output_dir, exist_ok=True)
        print(f"Output directory created: {output_dir}")

        unique_genres = combined_df['genre'].dropna().unique()
        for genre in unique_genres:
            genre_data = combined_df[combined_df['genre'] == genre]
            print(f"Processing genre: {genre}, Number of entries: {len(genre_data)}")

            plt.figure(figsize=(14, 8))
            sns.histplot(data=genre_data, x='release_year', bins=30, kde=False)
            plt.title(f'Total Number of Movie Releases per Year - Genre: {genre}', fontsize=16)
            plt.xlabel('Release Year', fontsize=14)
            plt.ylabel('Number of Movies Released', fontsize=14)
            plt.grid(True)

            genre_file_name = f"movie_releases_per_year_{genre.replace(' ', '_')}.png"
            output_path = os.path.join(output_dir, genre_file_name)
            plt.savefig(output_path)
            plt.close()
            print(f"Saved histogram for genre '{genre}' at {output_path}")
    else:
        print("No data available for visualization.")

# Define the DAG
with DAG(
    dag_id='q1_movie_releases_by_genre',
    description='Analyze and visualize total number of movie releases by genre per year',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2025, 1, 8),
    catchup=False,
) as dag:

    # Define the PythonOperator
    analyze_movie_releases_by_genre = PythonOperator(
        task_id='analyze_and_visualize_movie_releases_by_genre',
        python_callable=process_and_visualize_movie_releases_by_genre,
    )

    analyze_movie_releases_by_genre

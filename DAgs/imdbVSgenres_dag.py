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

# Function to process and visualize genre vs IMDb score and TMDB score
def process_and_visualize_genre_vs_scores():
    base_folder = '/home/dishant/de_project/airflow_venv/Data_lake'
    subfolders = ['amazon_data', 'paramount_data', 'hulu_data', 'hbo_data', 'netflix_data', 'disney_data']
    all_data_imdb = []
    all_data_tmdb = []

    for subfolder in subfolders:
        subfolder_path = os.path.join(base_folder, subfolder)
        titles_file_path = os.path.join(subfolder_path, 'titles.csv')

        if os.path.exists(titles_file_path):
            try:
                # Read the CSV file
                df = pd.read_csv(titles_file_path)

                # Check if 'genres', 'imdb_score', and 'tmdb_score' columns exist
                if 'genres' in df.columns and 'imdb_score' in df.columns and 'tmdb_score' in df.columns:
                    # Extract the first genre
                    df['genre'] = df['genres'].apply(extract_first_genre)

                    # Keep only relevant columns
                    df_imdb = df[['genre', 'imdb_score']].dropna()
                    df_tmdb = df[['genre', 'tmdb_score']].dropna()

                    # Append to the combined dataset for both IMDb and TMDB
                    all_data_imdb.append(df_imdb)
                    all_data_tmdb.append(df_tmdb)
            except Exception as e:
                print(f"Error processing {titles_file_path}: {e}")

    # Combine all data
    if all_data_imdb and all_data_tmdb:
        combined_df_imdb = pd.concat(all_data_imdb, ignore_index=True)
        combined_df_tmdb = pd.concat(all_data_tmdb, ignore_index=True)

        # Group by genre and calculate average IMDb and TMDB scores
        genre_imdb_score = combined_df_imdb.groupby('genre')['imdb_score'].mean().sort_values(ascending=False)
        genre_tmdb_score = combined_df_tmdb.groupby('genre')['tmdb_score'].mean().sort_values(ascending=False)

        # Plot IMDb scores
        plt.figure(figsize=(12, 8))
        bar_plot_imdb = sns.barplot(x=genre_imdb_score.index, y=genre_imdb_score.values, palette="viridis")
        plt.title('Average IMDb Score by Genre', fontsize=16)
        plt.xlabel('Genre', fontsize=14)
        plt.ylabel('Average IMDb Score', fontsize=14)
        plt.xticks(rotation=45)
        plt.tight_layout()

        # Annotate each bar with its value for IMDb score
        for index, value in enumerate(genre_imdb_score.values):
            bar_plot_imdb.text(index, value + 0.05, f'{value:.2f}', ha='center', fontsize=12, color='black')

        # Save the IMDb plot
        output_path_imdb = r'\\wsl.localhost\Ubuntu-22.04\home\dishant\de_project\airflow_venv\genre_vs_imdb_score.png'
        plt.savefig(output_path_imdb)
        plt.close()
        print(f"IMDb plot saved at {output_path_imdb}")

        # Plot TMDB scores
        plt.figure(figsize=(12, 8))
        bar_plot_tmdb = sns.barplot(x=genre_tmdb_score.index, y=genre_tmdb_score.values, palette="viridis")
        plt.title('Average TMDB Score by Genre', fontsize=16)
        plt.xlabel('Genre', fontsize=14)
        plt.ylabel('Average TMDB Score', fontsize=14)
        plt.xticks(rotation=45)
        plt.tight_layout()

        # Annotate each bar with its value for TMDB score
        for index, value in enumerate(genre_tmdb_score.values):
            bar_plot_tmdb.text(index, value + 0.05, f'{value:.2f}', ha='center', fontsize=12, color='black')

        # Save the TMDB plot
        output_path_tmdb = 'home\dishant\de_project\ airflow_venv\genre_vs_tmdb_score.png'
        plt.savefig(output_path_tmdb)
        plt.close()
        print(f"TMDB plot saved at {output_path_tmdb}")

    else:
        print("No data found to process.")

# Define the DAG
with DAG(
    dag_id='genre_vs_scores',
    description='Analyze and visualize genre vs IMDb and TMDB scores',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2025, 1, 8),
    catchup=False,
) as dag:

    # Define the PythonOperator
    analyze_genre_vs_scores = PythonOperator(
        task_id='analyze_and_visualize',
        python_callable=process_and_visualize_genre_vs_scores,
    )

    analyze_genre_vs_scores

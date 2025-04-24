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

# Function to process and visualize genre vs TMDB score
def process_and_visualize_genre_vs_tmdb_score():
    base_folder = '/home/dishant/de_project/airflow_venv/Data_lake'
    subfolders = ['amazon_data', 'paramount_data', 'hulu_data', 'hbo_data', 'netflix_data', 'disney_data']
    all_data = []

    for subfolder in subfolders:
        subfolder_path = os.path.join(base_folder, subfolder)
        titles_file_path = os.path.join(subfolder_path, 'titles.csv')

        if os.path.exists(titles_file_path):
            try:
                # Read the CSV file
                df = pd.read_csv(titles_file_path)

                # Check if 'genres' and 'tmdb_score' columns exist
                if 'genres' in df.columns and 'tmdb_score' in df.columns:
                    # Extract the first genre
                    df['genre'] = df['genres'].apply(extract_first_genre)

                    # Keep only relevant columns
                    df = df[['genre', 'tmdb_score']].dropna()

                    # Append to the combined dataset
                    all_data.append(df)
            except Exception as e:
                print(f"Error processing {titles_file_path}: {e}")

    # Combine all data
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)

        # Group by genre and calculate average TMDB score
        genre_tmdb_score = combined_df.groupby('genre')['tmdb_score'].mean().sort_values(ascending=False)

        # Plot the data
        plt.figure(figsize=(12, 8))
        bar_plot = sns.barplot(x=genre_tmdb_score.index, y=genre_tmdb_score.values, palette="viridis")
        plt.title('Average TMDB Score by Genre', fontsize=16)
        plt.xlabel('Genre', fontsize=14)
        plt.ylabel('Average TMDB Score', fontsize=14)
        plt.xticks(rotation=45)
        plt.tight_layout()

        # Annotate each bar with its value
        for index, value in enumerate(genre_tmdb_score.values):
            bar_plot.text(index, value + 0.05, f'{value:.2f}', ha='center', fontsize=12, color='black')

        # Save the plot
        output_path = 'home\dishant\de_project\ airflow_venv\genre_vs_tmdb_score.png'
        plt.savefig(output_path)
        plt.close()
        print(f"Plot saved at {output_path}")
    else:
        print("No data found to process.")

# Define the DAG
with DAG(
    dag_id='genre_vs_tmdb_score',
    description='Analyze and visualize genre vs TMDB score',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2025, 1, 8),
    catchup=False,
) as dag:

    # Define the PythonOperator
    analyze_genre_vs_tmdb_score = PythonOperator(
        task_id='analyze_and_visualize',
        python_callable=process_and_visualize_genre_vs_tmdb_score,
    )

    analyze_genre_vs_tmdb_score

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import pandas as pd

# Function to calculate platform popularity
def calculate_platform_popularity():
    base_folder = '/home/dishant/de_project/airflow_venv/Data_lake'
    subfolders = ['amazon_data', 'paramount_data', 'hulu_data', 'hbo_data', 'netflix_data', 'disney_data']
    popularity_data = []

    for subfolder in subfolders:
        subfolder_path = os.path.join(base_folder, subfolder)
        titles_file_path = os.path.join(subfolder_path, 'titles.csv')

        if os.path.exists(titles_file_path):
            try:
                # Read the CSV file
                df = pd.read_csv(titles_file_path)

                # Check if necessary columns exist
                if {'tmdb_popularity', 'imdb_score', 'imdb_votes'}.issubset(df.columns):
                    # Drop rows with missing data in the required columns
                    df = df.dropna(subset=['tmdb_popularity', 'imdb_score', 'imdb_votes'])

                    # Calculate weighted average popularity and user engagement
                    df['weighted_popularity'] = (df['tmdb_popularity'] * df['imdb_score'] * df['imdb_votes']) / df['imdb_votes'].sum()

                    # Calculate the platform popularity score
                    platform_popularity_score = df['weighted_popularity'].sum()
                    popularity_data.append({'subfolder': subfolder, 'platform_popularity_score': platform_popularity_score})
                else:
                    print(f"Necessary columns not found in {titles_file_path}")
            except Exception as e:
                print(f"Error processing {titles_file_path}: {e}")
        else:
            print(f"File not found: {titles_file_path}")

    # Create a DataFrame from the popularity data
    popularity_df = pd.DataFrame(popularity_data)

    # Find the subfolder with the highest platform popularity score
    most_popular_subfolder = popularity_df.loc[popularity_df['platform_popularity_score'].idxmax()]

    print("\nPlatform Popularity Scores by Subfolder:")
    print(popularity_df)
    print(f"\nThe most popular platform is {most_popular_subfolder['subfolder']} with a popularity score of {most_popular_subfolder['platform_popularity_score']}.")

    # Save the popularity data to a CSV file
    popularity_data_path = os.path.join(base_folder, 'platform_popularity_data.csv')
    popularity_df.to_csv(popularity_data_path, index=False)
    print(f"Platform popularity data saved at {popularity_data_path}")

# Define the DAG
with DAG(
    dag_id='calculate_platform_popularity',
    description='Calculate platform popularity score for each subfolder',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2025, 1, 8),
    catchup=False,
) as dag:

    # Define the PythonOperator
    analyze_platform_popularity = PythonOperator(
        task_id='calculate_platform_popularity',
        python_callable=calculate_platform_popularity,
    )

    analyze_platform_popularity

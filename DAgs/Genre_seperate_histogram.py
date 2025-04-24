from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Function to generate histograms for all genres
def generate_genre_histograms():
    input_file = '/home/dishant/de_project/airflow_venv/Data_lake/release_year_genre/merged_titles.xlsx'
    output_folder = '/home/dishant/de_project/airflow_venv/Data_lake/release_year_genre'
    output_image_path = os.path.join(output_folder, 'genre_histograms.png')

    # Ensure the output folder exists
    os.makedirs(output_folder, exist_ok=True)

    try:
        # Load the merged Excel file
        df = pd.read_excel(input_file)

        # Check if the required columns exist
        if 'release_year' in df.columns and 'genres' in df.columns:
            # Drop NaN values
            df = df.dropna(subset=['release_year', 'genres'])

            # Extract the first genre from the 'genres' column
            df['primary_genre'] = df['genres'].str.extract(r"\['([^']+)'?")

            # Get unique genres
            unique_genres = df['primary_genre'].dropna().unique()

            # Set up a grid for subplots
            num_genres = len(unique_genres)
            cols = 3  # Number of columns in the subplot grid
            rows = (num_genres + cols - 1) // cols
            fig, axes = plt.subplots(rows, cols, figsize=(15, rows * 5), constrained_layout=True)

            # Flatten axes for easy iteration
            axes = axes.flatten()

            for idx, genre in enumerate(unique_genres):
                genre_data = df[df['primary_genre'] == genre]

                # Plot histogram for each genre
                ax = axes[idx]
                sns.histplot(data=genre_data, x='release_year', bins=30, kde=False, ax=ax)
                ax.set_title(f'Genre: {genre}', fontsize=14)
                ax.set_xlabel('Release Year', fontsize=12)
                ax.set_ylabel('Count', fontsize=12)
                ax.grid(True)

            # Hide unused subplots
            for i in range(idx + 1, len(axes)):
                axes[i].set_visible(False)

            # Save the combined image
            plt.savefig(output_image_path)
            plt.close()
            print(f"Histograms saved at {output_image_path}")
        else:
            print("Required columns 'release_year' and 'genres' not found in the input file.")
    except Exception as e:
        print(f"Error generating histograms: {e}")

# Define the DAG
with DAG(
    dag_id='generate_genre_histograms',
    description='Create histograms of all genres separately in one image',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2025, 1, 8),
    catchup=False,
) as dag:

    # Define the PythonOperator
    create_histograms_task = PythonOperator(
        task_id='create_genre_histograms',
        python_callable=generate_genre_histograms,
    )

    create_histograms_task

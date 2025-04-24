from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Function to generate bar graphs for each genre with data points
def process_and_visualize_genre_histograms_with_data_points():
    # Path to the Excel file with 'Decade' and 'genres' columns
    excel_file_path = '/home/dishant/de_project/airflow_venv/Data_lake/release_year_genre/Decade.xlsx'

    # Load the Excel file
    if os.path.exists(excel_file_path):
        try:
            df = pd.read_excel(excel_file_path)

            # Check if the required columns exist
            if 'Decade' in df.columns and 'genres' in df.columns:
                # Group by 'Decade' and 'genres' and count occurrences
                genre_count_by_decade = df.groupby(['Decade', 'genres']).size().reset_index(name='count')

                # Get unique genres
                unique_genres = genre_count_by_decade['genres'].unique()

                # Set up a directory for saving the graphs
                output_dir = '/home/dishant/de_project/airflow_venv/Data_lake/release_year_genre/genre_histograms'
                os.makedirs(output_dir, exist_ok=True)

                # Generate and save the bar graph for each genre
                for genre in unique_genres:
                    genre_data = genre_count_by_decade[genre_count_by_decade['genres'] == genre]

                    plt.figure(figsize=(10, 6))
                    ax = sns.barplot(data=genre_data, x='Decade', y='count', palette='viridis')

                    # Adding data points (count values) on top of the bars
                    for p in ax.patches:
                        ax.annotate(f'{int(p.get_height())}', 
                                    (p.get_x() + p.get_width() / 2., p.get_height()), 
                                    ha='center', va='center', 
                                    fontsize=12, color='black', 
                                    xytext=(0, 5), textcoords='offset points')

                    # Set titles and labels
                    plt.title(f'Count of {genre} by Decade', fontsize=16)
                    plt.xlabel('Decade', fontsize=14)
                    plt.ylabel('Count', fontsize=14)
                    plt.xticks(rotation=45)
                    plt.tight_layout()

                    # Save the plot for this genre
                    plot_filename = f"{genre}_decade_histogram.png"
                    plot_path = os.path.join(output_dir, plot_filename)
                    plt.savefig(plot_path)
                    plt.close()
                    print(f"Saved bar graph for genre '{genre}' at {plot_path}")
            else:
                print("Required columns 'Decade' or 'genres' not found in the Excel file.")
        except Exception as e:
            print(f"Error processing the Excel file: {e}")
    else:
        print(f"Excel file not found at {excel_file_path}")

# Define the DAG
with DAG(
    dag_id='q1_genre_histograms_by_decade',
    description='Generate bar graphs for each genre by Decade from the Excel file with data points',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2025, 1, 8),
    catchup=False,
) as dag:

    # Define the PythonOperator
    generate_genre_histograms_with_data_points = PythonOperator(
        task_id='generate_genre_histograms_with_data_points',
        python_callable=process_and_visualize_genre_histograms_with_data_points,
    )

    generate_genre_histograms_with_data_points

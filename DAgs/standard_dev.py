from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

# Function to calculate the standard deviation of the difference
def calculate_std_difference():
    # Updated data
    data = {
        'Genre': ['documentation', 'war', 'history', 'sport', 'music', 'animation', 'drama', 'crime', 'scifi', 'european', 
                  'reality', 'comedy', 'Unknown', 'family', 'fantasy', 'action', 'romance', 'western', 'thriller', 'horror'],
        'IMDb Score': [7.1, 7.05, 6.86, 6.85, 6.74, 6.62, 6.52, 6.47, 6.43, 6.42, 6.3, 6.29, 6.24, 6.16, 6.15, 6.11, 6.08, 
                       5.84, 5.78, 4.83],
        'TMDB Score': [6.98, 6.96, 6.85, 6.73, 6.81, 7.05, 6.56, 5.59, 6.81, 5.88, 7.09, 6.39, 6.75, 6.84, 6.41, 6.55, 
                       6.15, 5.49, 5.94, 5.29]
    }

    # Create a DataFrame
    df = pd.DataFrame(data)

    # Calculate the difference between IMDb and TMDB scores
    df['Difference (IMDb - TMDB)'] = df['IMDb Score'] - df['TMDB Score']

    # Calculate the standard deviation of the differences
    std_difference = df['Difference (IMDb - TMDB)'].std()

    print(f"Standard Deviation of the Difference (IMDb - TMDB): {std_difference}")
    return std_difference

# Define the DAG
with DAG(
    dag_id='imdb_tmdb_std_dev',
    description='Calculate and print the standard deviation of the difference between IMDb and TMDB scores',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2025, 1, 8),
    catchup=False,
) as dag:

    # Define the PythonOperator
    calculate_std_dev_task = PythonOperator(
        task_id='calculate_std_difference',
        python_callable=calculate_std_difference,
    )

    calculate_std_dev_task

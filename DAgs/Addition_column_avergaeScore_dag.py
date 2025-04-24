import os
import pandas as pd
import ast

# Function to limit genres in the 'genres' column
def limit_genres(genres_str):
    try:
        genres_list = ast.literal_eval(genres_str)
        if isinstance(genres_list, list):
            return str(genres_list[:2])  # Limit to at most 2 genres
        else:
            return str([])  # Handle cases where the genre format is invalid
    except (ValueError, SyntaxError):
        return str([])  # Handle invalid genre entries gracefully

# Function to calculate the average score
def calculate_average_score(imdb_score, tmdb_score):
    try:
        imdb_score = float(imdb_score) if not pd.isnull(imdb_score) else None
        tmdb_score = float(tmdb_score) if not pd.isnull(tmdb_score) else None
        if imdb_score is not None and tmdb_score is not None:
            return (imdb_score + tmdb_score) / 2
        elif imdb_score is not None:
            return imdb_score
        elif tmdb_score is not None:
            return tmdb_score
        else:
            return None
    except ValueError:
        return None

# Function to process all titles files in subfolders
def process_titles_files(base_folder):
    subfolders = ['amazon_data', 'paramount_data', 'hulu_data', 'hbo_data', 'netflix_data', 'disney_data']

    for subfolder in subfolders:
        subfolder_path = os.path.join(base_folder, subfolder)
        titles_file_path = os.path.join(subfolder_path, 'titles.csv')

        # Check if titles.csv exists
        if os.path.exists(titles_file_path):
            try:
                print(f"Processing {titles_file_path}...")
                # Read the CSV file
                df = pd.read_csv(titles_file_path)

                # Check if 'genres' column exists
                if 'genres' in df.columns:
                    df['genres'] = df['genres'].apply(limit_genres)
                
                # Calculate the average score and add a new column
                if 'imdb_score' in df.columns and 'tmdb_score' in df.columns:
                    df['average_score'] = df.apply(
                        lambda row: calculate_average_score(row['imdb_score'], row['tmdb_score']), axis=1
                    )
                else:
                    print(f"'imdb_score' or 'tmdb_score' column not found in {titles_file_path}")

                # Save the updated file back to its original location
                df.to_csv(titles_file_path, index=False)
                print(f"Updated file saved at {titles_file_path}")
            except Exception as e:
                print(f"Error processing {titles_file_path}: {e}")
        else:
            print(f"File not found: {titles_file_path}")

# Define the base folder path
base_folder = '/home/dishant/de_project/airflow_venv/Data_lake'

# Process all titles files
process_titles_files(base_folder)

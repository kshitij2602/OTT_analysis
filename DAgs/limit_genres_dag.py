import os
import pandas as pd
import ast

# Function to limit genres in the 'genres' column
def limit_genres(genres_str):
    try:
        # Convert string representation of a list to an actual list
        genres_list = ast.literal_eval(genres_str)
        if isinstance(genres_list, list) and genres_list:
            return genres_list[0]  # Return only the first genre
        else:
            return "Unknown"  # Handle cases where the genre format is invalid or empty
    except (ValueError, SyntaxError):
        return "Unknown"  # Handle invalid genre entries gracefully

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
                    # Apply the transformation
                    df['genres'] = df['genres'].apply(limit_genres)

                    # Save the updated file back to its original location
                    df.to_csv(titles_file_path, index=False)
                    print(f"Updated file saved at {titles_file_path}")
                else:
                    print(f"'genres' column not found in {titles_file_path}")
            except Exception as e:
                print(f"Error processing {titles_file_path}: {e}")
        else:
            print(f"File not found: {titles_file_path}")

# Define the base folder path
base_folder = '/home/dishant/de_project/airflow_venv/Data_lake'

# Process all titles files
process_titles_files(base_folder)

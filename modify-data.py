import pandas as pd
import numpy as np


def main():
   pd.set_option('display.max_rows', 10)
   # Open movies_metadata
   movie_file = pd.read_csv('movies_metadata.csv')

   #Remove the tt from all rows of imdb_id
   movie_file['imdb_id'] = movie_file['imdb_id'].apply(lambda id: str(id)[2:])
   #Rename id column to tmdb_id
   movie_file.rename(columns={'id': 'tmdb_id'}, inplace=True)
   #Exprt it to a new file movies_data.csv
   movie_file.to_csv('movies_data.csv', index=False)

   # Open the new movies_data.csv file, convert all 'n' value into numpy NaN
   movie_file = pd.read_csv('movies_data.csv',converters={'imdb_id' : conv_imdb})

   # Combine movies_data.csv with links.csv
   # Perform a left join on imdb_id
   # This concatenates a movieId field into movies_data.csv
   link_file = pd.read_csv('links.csv')
   link_columns = ['movieId', 'imdbId']
   link_file = link_file[link_columns]
   movie_file['imdb_id'] = movie_file['imdb_id'].astype(float)
   link_file.rename(columns={'imdbId': 'imdb_id'}, inplace=True)
   movie_file = pd.merge(left=movie_file, right=link_file, how='left', on='imdb_id')
   movie_file.to_csv('movies_data.csv', index=False)

   # Combine movies_data.csv with keywords.csv

   movie_file = pd.read_csv('movies_data.csv', converters={'tmdb_id': conv_tmdb})
   keywords_file = pd.read_csv('keywords.csv')
   keywords_file.rename(columns={'id': 'tmdb_id'}, inplace=True)
   print(movie_file['tmdb_id'])
   movie_file = pd.merge(left=movie_file, right=keywords_file, how='left', on='tmdb_id')


   credits_file = pd.read_csv('credits.csv')
   credits_file.rename(columns={'id': 'tmdb_id'}, inplace=True)
   print(credits_file['tmdb_id'])
   movie_file = pd.merge(left=movie_file,right=credits_file, how='left', on='tmdb_id')
   print(movie_file['cast'])

   movie_file.to_csv('movies_data.csv', index=False)




   #file.to_csv('movies_data.csv')

def conv_imdb(val):
    if val == 'n' or val == '':
        return np.nan # or whatever else you want to represent your NaN with
    return val

def conv_tmdb(val):
    try:
        v = int(val)
        return v
    except:
        return np.nan

if __name__ == "__main__":
   main()

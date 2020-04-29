import pymongo
from pymongo import MongoClient
import pandas as pd

import csv

# Connect through localhost:27117
connection = MongoClient('localhost', 27117)
db = connection.movie
movie_data = db.movie_data
ratings = db.ratings

df = pd.read_csv("movies_data.csv")

print("Start inserting movie data")
record = df.to_dict(orient='records')
movie_data.insert_many(record)

print("Finished inserting movie data")

print("Start inserting ratings")
chunksize = 10 ** 5
size = 0;
for chunk in pd.read_csv("ratings.csv", chunksize=chunksize):
    size += len(chunk)
    print("Size: ", size)
    record = chunk.to_dict(orient='records')
    ratings.insert_many(record)

print("Finish inserting ratings")

import pymongo
from pymongo import MongoClient
import datetime

# Connect through localhost:27117
connection = MongoClient('localhost', 27117)

# Get the db
db = connection.movie # Change the DB name if you need to

global movie_collection
# Get the movie_data collection
movie_collection = db['movie_data']
global ratings_collection
# Get the ratings collection
ratings_collection = db['ratings']
# Use this function to return the
# corresponding function for each choice
def choices(choice):
    switch={
                1: average_rating_by_title,
                2: number_of_movies_by_genre,
                3: all_movies_by_year,
                4: find_movies_by_genre,
                5: find_movies_by_language,
                6: find_average_revenue_by_genre,
                7: top_10_movies_by_budget,
                8: top_10_movies_by_ratings,
                9: find_movie_by_title,
                10: find_movie_by_cast_member,
                11: insert_new_movie,
                12: add_rating_by_title,
                13: update_movie_by_title,
                14: delete_movie_by_title,
                15: delete_rating_by_userid_and_movieid,
                16: number_of_cast_in_a_movie,
                17: average_budget_by_genre,
                18: find_movies_made_by_production_company,
                19: find_movies_made_by_production_countries,
                20: find_most_popular_movies_by_a_country,
                21: top_10_movies_by_popularity,
                22: find_cast_members_by_title,
                23: update_user_rating,
             }
    return switch.get(choice,"Invalid function choice")


def main():
    print("Welcome to our movies database")
    exit = False

    while(not exit):
        print("Please choose one of the options below:")
        print("1. Find average ratings of a movie")
        print("2. Find the number of movies in a genre")
        print("3. Find all movies given a year")
        print("4. Find all movies given a genre")
        print("5. Find all movies given a language")
        print("6. Find the average revenue given a genre")
        print("7. Find the top 10 movies with highest budget")
        print("8. Find the top 10 movies with highest ratings")
        print("9. Find the data of a movie given a title")
        print("10. Find all movies given a cast member")
        print("11. Insert a new movie ")
        print("12. Add a user rating given movie title")
        print("13. Update a movie given the movie title")
        print("14. Delete a movie given the movie title")
        print("15. Delete a user rating")
        print("16. Find the number of cast members in a movie")
        print("17. Find average budget given a genre")
        print("18. Find movies made by a production company")
        print("19. Find movies made by a production country")
        print("20. Find the most popular movies made by a country")
        print("21. Find the top 10 movies by average popularity")
        print("22. Find all cast members given the movie title")
        print("23. Update a user's rating")
        choice = input("Enter a number from 1-15: ")
        if int(choice) > 23 or int(choice) < 1:
            print("Invalid input, please choose a valid choice")
        else:
            # Get the corresponding function
            func = choices(int(choice))
            # Executes the function
            func()

            ans = input("Would you like to continue navigating the application? [y/n] ")
            while(ans != 'y' and ans != 'n'):
                ans = input('Invalid input, please answer y for yes and n for no: ')
            if ans == 'n':
                exit = True

    print("Goodbye")
    return

# Find average ratings of a movie given movie title
def average_rating_by_title():
    return

# Find the number of movies in a genre
def number_of_movies_by_genre():
    return

# Find all movies released in a year
def all_movies_by_year():
    return

# Find all movies in a genre
def find_movies_by_genre():
    return

# Find all movies given a language
def find_movies_by_language():
    return

# Find the average revenue of movies in a genre
def find_average_revenue_by_genre():
    return

# Find the top 10 movies with highest budget
def top_10_movies_by_budget():
    return

# Find the top 10 movies by user ratings
def top_10_movies_by_ratings():
    return

# Find top 10 movies with highest popularity
def top_10_movies_by_popularity():
    cursor = movie_collection.aggregate([{'$group': {'_id': "$movieId", 'movie_title': {'$first': '$original_title'},\
                'avg_popularity': {'$avg': "$popularity"}}}, {'$sort': {'avg_popularity': -1}}, {'$limit': 10}])
    for result in cursor:
        print(result)
    return

# Find a movie given the title of the movie
def find_movie_by_title():
    return

# Find all movies given name of a cast member
def find_movie_by_cast_member():
    return

# Insert a new movie into the database
def insert_new_movie():
    movie_title = input("Insert the title of the new movie: ")

    # For genre just insert genres separated by spaces
    # i.e. Adventure Comedy Horror
    genre = input("Insert the genres of the new movie (separated by spaces if multiples): ")

    # Split the genre string into a list containing the genres
    genre = genre.split(' ')
    if len(genre) == 1:
        genre = genre[0]
    original_language = input("Insert the language of the movie: ")
    overview = input("Insert the description of the movie: ")
    production_company = input("Insert the production company: ")
    production_country = input("Insert the production country: ")
    insert_query = {'original_title': movie_title, 'genre': genre, 'original_language': original_language, \
                    'overview': overview, 'production_company': production_company, 'production_country': production_country}
    inserted_movie_id = movie_collection.insert_one(insert_query).inserted_id

    # This part is just in case you want to print out the movie we just added
    """
    for result in collection.find('_id': inserted_movie_id):
        print(resukt)
    """
    return

# Add a user rating given the movie title
def add_rating_by_title():
    user = input("Insert your username: ")
    movie_title = input("Insert the title of the movie: ")
    rating = input("Insert your rating for the movie (1-5): ")
    query = {'original_title': movie_title}
    movie_id = movie_collection.find(query)[0]['movieId']
    print('Movie ID: ' + movieId)
    insert_query = {'userId' : user, 'movieId': movie_id, 'rating': 3.5, 'timestamp': datetime.datetime.now()}

    inserted_rating_id = ratings_collection.insert_one(insert_query).inserted_id

    # This part is just in case you want to print out the new rating we just added
    """
    for result in collection.find({'_id': inserted_rating_id}):
        print(result)
    """
    return

# Update movie data given a movie title
def update_movie_by_title():
    return

# Delete a movie from database given a title
def delete_movie_by_title():
    movie_title = input("Insert the title of the movie you wish to delete: ")
    query = {'original_title': movie_title}
    movie_collection.remove(query)
    return

# Delete a rating given userId and movieId
def delete_rating_by_userid_and_movieid():
    return

# Find the number of cast members in a movie
def number_of_cast_in_a_movie():
    return

# Find the average budget by genre
def average_budget_by_genre():
    return

# Find the movies made by a production company
def find_movies_made_by_production_company():
    return

# Find the movies made by a production country
def find_movies_made_by_production_countries():
    return

# Find the most popular movies made by a country
def find_most_popular_movies_by_a_country():
    return

# Find all cast members of a movie given the movie title
def find_cast_members_by_title():
    return

def update_user_rating():
    user = input("Input your username: ")
    movie_title = input("Insert the movie title: ")
    query = {'original_title': movie_title}
    movieId = movie_collection.find(query)[0]['movieId']
    print('Movie ID: ' + str(movieId))
    update_query = {'userId': user, 'movieId': movieId}
    set_query = {"$set": {"rating": 5.0}}
    ratings_collection.update_one(update_query, set_query)

    # This part is for incase you want to print out the ratings after update
    """
    for cursor in collection.find(update_query):
        print("Rating after update: " +str(cursor['rating']))
    """
    return

if __name__ == "__main__":
    main()

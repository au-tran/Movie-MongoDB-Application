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
        choice = input("Enter a number from 1-23: ")
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
    movie_title = input("Enter a movie title: ")
    cursor =  movie_collection.find({'original_title': {'$regex': '.*' + movie_title + '.*', '$options': 'i'}})
    movie_id = set()
    for result in cursor:
        movie_id.add((result['movieId'], result['original_title']))
    for id in movie_id:
        movie_id,title = id
        print(movie_id, title)
        cursor = ratings_collection.aggregate([{'$match':{'_id': movie_id}},{'$group': {'_id': "$movieId", 'title': {'$first': title}, 'avg_rating': {'$avg': "$rating"}}}])
        for result in cursor:
            print(result)
        return
    return

# Find the number of movies in a genre
def number_of_movies_by_genre():
    cursor = movie_collection.aggregate([{ '$unwind' : "$genres" },{'$group':{'_id': "$genres.name", 'genre':{'$first':"$genres.name"}, 'count': {'$sum': 1}}}])
    for result in cursor:
        print(result)
    return

# Find all movies released in a year
def all_movies_by_year():
    # db.movie_data.aggregate([{'$project':{'year_rel':{'$split':["$release_date","/"]}}},{'$unwind':"$year_rel"},{'$match' : { 'year_rel' : {'$regex':'/[0-9]{4}/'}}},{'$group':{'_id':{"year":"$year_rel"},'count':{"$sum":1}}}])
    year = input("Enter the release year: ")
    cursor = movie_collection.find({"release_date": {'$regex': year +'$'}},{'_id':0,'name':1, 'release_date':1})
    for result in cursor:
        print(result)
    return

# Find all movies in a genre
def find_movies_by_genre():
    print("Please enter any of the following genres: Horror, Foreign, Music, History, Thriller, Crime, Action, "
          "Fantasy, Science Fiction, Romance, War, Adventure, Family, Animation, Drama, Documentary, Mystery, Comedy ")
    genre = input("Enter a genre: ")
    cursor = movie_collection.find({'genres':{'$elemMatch':{'name':genre}}},{'_id':0,'original_title':1 ,'genres.name':1})
    for result in cursor:
        print(result)
    return

# Find all movies given a language
def find_movies_by_language():
    language = input("Enter a language: ")
    cursor = movie_collection.find({'spoken_languages': {'$elemMatch': {'name': language}}}, {'_id': 0, 'original_title': 1, 'spoken_languages.name': 1})
    for result in cursor:
        print(result)
    return

# Find the average revenue of movies in a genre
def find_average_revenue_by_genre():
    genre = input("Enter a genre: ")
    cursor = movie_collection.aggregate([{'$unwind': "$genres"}, {'$group': {'_id': "$genres.name", 'genres': {'$first': '$genres.name'}, 'avg_revenue':{'$avg':"$revenue"}}},{'$match': {'_id': genre}}])
    for result in cursor:
        print(result)
    return

# Find the top 10 movies with highest budget
def top_10_movies_by_budget():
    cursor = movie_collection.find({},{'original_title': 1, 'budget': 1}).sort('budget', -1).limit(10)
    for result in cursor:
        print(result)
    return

# Find the top 10 movies by user ratings
def top_10_movies_by_ratings():
    cursor = ratings_collection.aggregate([{'$group':{'_id': "$movieId", 'avg_rating': {'$avg': "$rating"}}}, {'$sort':{'avg_rating':-1}},{'$limit':10}])
    movie_id = set();
    for result in cursor:
        movie_id.add(result['_id'])
    for id in movie_id:
        movie_collection.find({'movieId': id}, {'original_title': 1, 'release_date': 1, 'runtime': 1})
        for result in cursor:
            print(result)
        return
    return

# Find top 10 movies with highest popularity
def top_10_movies_by_popularity():
    cursor = movie_collection.find({},{'original_title':1 ,'popularity':1}).sort('popularity',-1).limit(10)
    for result in cursor:
        print(result)
    return

# Find a movie given the title of the movie
def find_movie_by_title():
    title = input("Enter the movie title: ")
    cursor = movie_collection.find({'original_title': {'$regex': '.*'+title+'.*', '$options': 'i'}}, {'_id': 0, 'genres.name':1, 'original_title': 1,'original_language':1 ,'budget': 1, })
    for result in cursor:
        print(result)
    return

# Find all movies given name of a cast member
def find_movie_by_cast_member():
    cast_member = input("Enter name of a cast member: ")
    cursor = movie_collection.find({'cast': {'$elemMatch': {'name':{'$regex': '.*'+cast_member+'.*', '$options': 'i'}}}}, {'_id': 0, 'original_title': 1, 'cast.name': 1})
    for result in cursor:
        print(result)
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
    print('Movie ID: ' + str(movie_id))
    insert_query = {'userId' : user, 'movieId': movie_id, 'rating': rating, 'timestamp': datetime.datetime.now()}

    inserted_rating_id = ratings_collection.insert_one(insert_query).inserted_id

    # This part is just in case you want to print out the new rating we just added
    """
    for result in collection.find({'_id': inserted_rating_id}):
        print(result)
    """
    return

# Update movie data given a movie title
def update_movie_by_title():
    movie_title = input("Insert the movie title: ")
    overview = input("Insert the new description: ")
    update_query = {'original_title': movie_title}
    set_query = {"$set": {"overview": overview}}
    movie_collection.update_one(update_query, set_query)
    return

# Delete a movie from database given a title
def delete_movie_by_title():
    movie_title = input("Insert the title of the movie you wish to delete: ")
    query = {'original_title': movie_title}
    movie_collection.remove(query)
    return

# Delete a rating given userId and movieId
def delete_rating_by_userid_and_movieid():
    user = input("Insert your username: ")
    movie_title = input("Insert the title of the movie: ")
    query = {'original_title': movie_title}
    movie_id = movie_collection.find(query)[0]['movieId']
    print('Movie ID: ' + str(movie_id))
    delete_query = {'userId': user, 'movieId':movie_id}
    ratings_collection.delete_one(delete_query)
    return

# Find the number of cast members in a movie
def number_of_cast_in_a_movie():
    movie_title = input("Insert the title of the movie: ")
    query = {'original_title': movie_title}
    cursor = movie_collection.find(query)
    for c in cursor:
        print("Number of cast members: " + str(len(c['cast'])))
    return

# Find the average budget by genre
def average_budget_by_genre():
    genre = input("Enter a genre: ")
    cursor = movie_collection.aggregate([{'$unwind': "$genres"}, {'$group': {'_id': "$genres.name", 'genres': {'$first': '$genres.name'}, 'avg_budget':{'$avg':"$budget"}}},{'$match': {'_id': genre}}])
    for result in cursor:
        print(result)
    return

# Find the movies made by a production company
def find_movies_made_by_production_company():
    production_company = input("Insert name of production company: ")
    query = {'production_companies.name': {'$regex': '.*' + production_company + '.*', '$options': 'i'}}
    cursor = movie_collection.find(query, {'original_title': 1})
    for c in cursor:
        print(c)
    return

# Find the movies made by a production country
def find_movies_made_by_production_countries():
    production_country = input("Insert name of production country: ")
    query = {'production_countries.name': {'$regex': '.*' + production_country + '.*', '$options': 'i'}}
    cursor = movie_collection.find(query, {'original_title': 1})
    for c in cursor:
        print(c)
    return

# Find the most popular movies made by a country
def find_most_popular_movies_by_a_country():
    return

# Find all cast members of a movie given the movie title
def find_cast_members_by_title():
    movie_title = input("Insert the movie title: ")
    query = {'original_title': movie_title}
    cursor = movie_collection.find(query, {'original_title': 1, 'cast': 1})
    for c in cursor:
        print(c)
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

from starbase import Connection

c = Connection("localhost", "8000")

# Create table and structure
ratings = c.table("ratings")
if ratings.exists():
    ratings.drop()

ratings.create('rating')  # Create a ratings column family

# Populate with data
batch = ratings.batch()

with open("ml-100k/u.data", "r") as rating_file:
    for line in rating_file:
        (user_id, movie_id, rating, timestamp) = line.split()
        # Create row with user_id as key,
        # and in the 'rating' column family, the column movie_id with value rating
        batch.update(user_id, {'rating': {movie_id: rating}})

batch.commit(finalize=True)

# Query data
print(ratings.fetch("1"))  # user_id=1

ratings.drop()

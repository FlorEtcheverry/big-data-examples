from pyspark.sql import SparkSession
from pyspark.sql import Row


# Spark 2 with DataFrames

def load_movie_names():
    movie_names = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movie_names[int(fields[0])] = fields[1]
    return movie_names


def parse_input(line):
    fields = line.split()
    return Row(movieID=int(fields[1]), rating=float(fields[2]))


if __name__ == "__main__":
    # Create a SparkSession (the config bit is only for Windows!)
    spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

    lines = spark.sparkContext.textFile("ml-100k/u.data")
    # Convert it to a RDD of Row objects with (movieID, rating)
    movies = lines.map(parse_input)
    movie_dataset = spark.createDataFrame(movies)

    # Compute average rating for each movieID
    average_ratings = movie_dataset.groupBy("movieID").avg("rating")

    # Compute count of ratings for each movieID
    counts = movie_dataset.groupBy("movieID").count()

    # Join the two together (We now have movieID, avg(rating), and count columns)
    averagesAndCounts = counts.join(average_ratings, "movieID").filter("count >= 10")

    topTen = averagesAndCounts.orderBy("avg(rating)").take(10)

    movieNames = load_movie_names()
    for movie in topTen:
        print(movieNames[movie[0]], movie[1], movie[2])

    # Stop the session
    spark.stop()

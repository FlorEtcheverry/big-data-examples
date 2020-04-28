from pyspark import SparkConf, SparkContext


# Spark 1 with RDD


def load_movie_names():
    movie_names = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movie_names[int(fields[0])] = fields[1]
    return movie_names


# Converts each line to (movieID, (rating, 1.0))
def parse_input(line):
    fields = line.split()
    return int(fields[1]), (float(fields[2]), 1.0)


if __name__ == "__main__":
    conf = SparkConf().setAppName("WorstMovies")
    sc = SparkContext(conf=conf)
    lines = sc.textFile("ml-100k/u.data")

    # Convert to (movieID, (rating, 1.0))
    movieRatings = lines.map(parse_input)

    # Reduce to (movieID, (sumOfRatings, totalRatings))
    # Filter totalRatings >= 10
    ratingTotalsAndCount = movieRatings \
        .reduceByKey(lambda movie1, movie2: (movie1[0] + movie2[0], movie1[1] + movie2[1])) \
        .filter(lambda t: t[1][1] >= 10)

    # Map to (rating, averageRating)
    averageRatings = ratingTotalsAndCount.mapValues(lambda total_and_count: total_and_count[0] / total_and_count[1])

    # Sort by average rating
    sortedMovies = averageRatings.sortBy(lambda x: x[1])

    # Take the top 10 results
    results = sortedMovies.take(10)

    movieNames = load_movie_names()
    for result in results:
        print(movieNames[result[0]], result[1])

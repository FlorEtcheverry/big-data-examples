from pyspark.sql import SparkSession, Row


def parse_input(line):
    fields = line.split('|')
    return Row(user_id=int(fields[0]), age=int(fields[1]), gender=fields[2], occupation=fields[3], zip=fields[4])


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("MongoDBIntegration") \
        .getOrCreate()

    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.user")
    users = lines.map(parse_input)
    users_dataset = spark.createDataFrame(users)

    # Write dataframe into MongoDB
    users_dataset.write \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .option("uri", "mongodb://127.0.0.1/movielens.users") \
        .mode('append') \
        .save()

    # Read it back from MongoDB into a new Dataframe
    read_users = spark.read \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .option("uri", "mongodb://127.0.0.1/movielens.users") \
        .load()

    read_users.createOrReplaceTempView("users")

    sqlDF = spark.sql("SELECT * FROM users WHERE age < 20")
    sqlDF.show()

    # Stop Spark session
    spark.stop()

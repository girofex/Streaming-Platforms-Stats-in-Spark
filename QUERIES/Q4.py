from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time as t

spark = SparkSession \
    .builder \
    .appName("Streaming Platforms Stats") \
    .getOrCreate()

netflixPath = "hdfs:/user/user_dc_11/netflix.csv"    
primePath = "hdfs:/user/user_dc_11/prime.csv"

start = t.time()

netflix = spark.read.csv(netflixPath, header=True, inferSchema=True)
prime = spark.read.csv(primePath, header=True, inferSchema=True)
print("Most popular movie present on both platforms")

join = netflix.join(prime, on=["title", "imdbAverageRating", "type"], how="inner")
movies = join.select("title", "imdbAverageRating").filter(col("type") == "movie")
most_popular = movies.orderBy(col("imdbAverageRating").desc()).limit(1)
most_popular.show(truncate=False)

finish = t.time()

time = finish - start
print(f"Time spent: {time}")

spark.stop()
from pyspark.sql import SparkSession, functions
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
print("The tv show(s) that is (are) most distributed")

netflix_tv = netflix.filter(col("type") == "tv").select("title", "availableCountries")
prime_tv = prime.filter(col("type") == "tv").select("title", "availableCountries")
tv = netflix_tv.join(prime_tv, on="title", how="inner")
tv = tv.select("title", netflix_tv["imdbAverageRating"])
most_popular = tv.orderBy(col("imdbAverageRating").desc()).limit(1)
most_popular.show(truncate=False)

finish = t.time()

time = finish - start
print(f"Time spent: {time}")

spark.stop()
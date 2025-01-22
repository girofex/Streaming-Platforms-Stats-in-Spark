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
print("The tv show(s) that is (are) most distributed")

netflix_tv = netflix.select("title", "availableCountries").filter(col("type") == "tv")
prime_tv = prime.select("title", "availableCountries").filter(col("type") == "tv")
tv = netflix_tv.union(prime_tv).distinct()

country_array = tv.select("title", "availableCountries", split(col("availableCountries"), ", ").alias("availableCountriesArray"))
country_count = country_array.select("title", "availableCountries", size(col("availableCountriesArray")).alias("count"))
max_countries = country_count.select(max("count")).first()[0]
most_distributed = country_count.filter(col("count") == max_countries)

truncated = most_distributed.select("title", substring(col("availableCountries"), 1, 100).alias("availableCountries"), "count")
truncated.show(truncate=False)

finish = t.time()

time = finish - start
print(f"Time spent: {time}")

spark.stop()
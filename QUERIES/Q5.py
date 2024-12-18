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
tv = netflix_tv.union(prime_tv).distinct()

country_array = tv.withColumn("availableCountriesArray", split(col("availableCountries"), ", "))
country_count = country_array.select("title", "availableCountries", size(col("availableCountriesArray")).alias("count"))
max_countries = country_count.select(max("count").alias("max")).collect()[0]["max"]
most_distributed = country_count.filter(col("count") == max_countries)

truncated = most_distributed.withColumn("availableCountries", substring(col("availableCountries"), 1, 100))
truncated.show(truncate=False)

finish = t.time()

time = finish - start
print(f"Time spent: {time}")

spark.stop()
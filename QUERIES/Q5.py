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

country_array = tv.select("title", "availableCountries", size(split(col("availableCountries"), ", ")).alias("count"))
most_distributed = country_array.orderBy(col("count").desc()).limit(1)
most_distributed_with_truncate = most_distributed.withColumn("availableCountries", substring("availableCountries", 1, 20))

most_distributed_with_truncate.show(truncate=False)

finish = t.time()

time = finish - start
print(f"Time spent: {time}")

spark.stop()
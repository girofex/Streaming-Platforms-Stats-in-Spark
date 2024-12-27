from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time as t

spark = SparkSession \
    .builder \
    .appName("Streaming Platforms Stats") \
    .getOrCreate()
    
netflixPath = "hdfs:/user/user_dc_11/netflix.csv"

start = t.time()

netflix = spark.read.csv(netflixPath, header=True, inferSchema=True)
print("Top 20 highest rated titles on Netflix")

content = netflix.select("title", "imdbAverageRating")
top_titles = content.orderBy(col("imdbAverageRating").desc())
top_titles.show(20, truncate=False);

finish = t.time()

time = finish - start
print(f"Time spent: {time}")

spark.stop()
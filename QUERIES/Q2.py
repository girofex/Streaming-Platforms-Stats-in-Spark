from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time as t

spark = SparkSession \
    .builder \
    .appName("Streaming Platforms Stats") \
    .getOrCreate()
    
primePath = "hdfs:/user/user_dc_11/prime.csv"

start = t.time()

prime = spark.read.csv(primePath, header=True, inferSchema=True)
print("Top 10 most popular genres on Prime Video")

#based on the recurrence of the genre
genres = prime.withColumn("genre", explode(split(col("genres"), ", ")))
count = genres.groupBy("genre").count()
top_genres = count.orderBy(col("count").desc())
top_genres.show(10, truncate=False)

finish = t.time()

time = finish - start
print(f"Time spent: {time}")

spark.stop()
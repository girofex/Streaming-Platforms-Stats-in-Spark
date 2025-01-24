from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("Streaming Platforms Stats") \
    .getOrCreate()
    
primePath = "hdfs:/user/user_dc_11/prime.csv"

prime = spark.read.csv(primePath, header=True, inferSchema=True)
print("Top 10 most popular genres on Prime Video")

genres = prime.select("title", explode(split(col("genres"), ", ")).alias("genre"))
count = genres.groupBy("genre").count()
top_genres = count.orderBy(col("count").desc(), col("genre").asc())
top_genres.show(10, truncate=False)

spark.stop()
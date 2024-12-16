from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime

spark = SparkSession \
    .builder \
    .appName("Streaming Platforms Stats") \
    .getOrCreate()
    
netflixPath = "hdfs:/user/user_dc_11/netflix.csv"

now = datetime.now()
start = now.strftime("%H:%M:%S")
print("Started on =", start)

netflix = spark.read.csv(netflixPath, header=True, inferSchema=True)
print(netflix.columns)
print("Top 20 highest rated titles on Netflix")

content = netflix.select("Title", "IMDB Score")
top_titles = content.orderBy(col("IMDB Score").desc())
top_titles.show(20);

now = datetime.now()
finish = now.strftime("%H:%M:%S")
print("Ended on =", finish)

time_spent = finish - start
print(f"Time spent: {time_spent}")

spark.stop()
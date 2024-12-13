from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime

spark = SparkSession \
    .builder \
    .appName("Streaming Platforms Stats") \
    .getOrCreate()
    
netflixPath = "hdfs:/user/user_dc_11/netflix.csv"

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Started on =", current_time)

listensDF = spark.read.csv(netflixPath, header=True, inferSchema=True)
listensDF.printSchema()

print("The 20 most viewed movies on Netflix")

#####

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Ended on =", current_time)

spark.stop()
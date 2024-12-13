from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime

spark = SparkSession \
    .builder \
    .appName("Streaming Platforms Stats") \
    .getOrCreate()
    
primePath = "hdfs:/user/user_dc_11/prime.csv"

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Started on =", current_time)

listensDF = spark.read.csv(netflixPath, header=True, inferSchema=True)
listensDF.printSchema()

print("The 10 most popular genres on Prime Video")

#####

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Ended on =", current_time)

spark.stop()
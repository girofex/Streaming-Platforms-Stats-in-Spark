from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("Streaming Platforms Stats") \
    .getOrCreate()

netflixPath = "hdfs:/user/user_dc_11/netflix.csv"    
primePath = "hdfs:/user/user_dc_11/prime.csv"

netflix = spark.read.csv(netflixPath, header=True, inferSchema=True)
prime = spark.read.csv(primePath, header=True, inferSchema=True)
print("The tv show(s) that is (are) most distributed")

union = netflix.union(prime).distinct()
tv = union.select("title", "availableCountries", size(split(col("availableCountries"), ", ")).alias("count")).filter(col("type") == "tv")

most_distributed = tv.orderBy(col("count").desc(), col("title").asc()).limit(1)
most_distributed_with_truncate = most_distributed.withColumn("availableCountries", substring("availableCountries", 1, 20))

most_distributed_with_truncate.show(truncate=False)

spark.stop()
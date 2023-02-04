import pyspark
import pyspark.sql.functions as f
import os, sys, time
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Q1").getOrCreate()

df = spark.read.parquet("hdfs:///parquet/")
c_df = spark.read.format("csv").option("separator", ",").option("header", True).option("inferSchema", "true").load("hdfs:///taxi+_zone_lookup.csv")

#print()
#print()
#print(f"dataframe size : {df.count()}")
#df.printSchema()
#print()
#print()

start = time.time()
df = df.filter((f.month(f.col("tpep_pickup_datetime")) == 3) & (f.col("PULocationID") == (c_df.filter(f.col("Zone") == "Battery Park").collect()[0][0]))).agg(f.max("tip_amount"))
df = df.withColumnRenamed("max(tip_amount)", "max_tip")
#df.write.parquet("hdfs:///Q/Q1.parquet")
df.show()
end = time.time()

print()
print()
print(f'Time taken: {end-start} seconds.')
print()
print()

#df.filter((f.month(f.col("tpep_pickup_datetime")) == 3) & (f.col("PULocationID") == 12)).sort(f.col("tip_amount"), ascending=False).show(30)

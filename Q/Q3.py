import pyspark
import pyspark.sql.functions as f
import os, sys, time
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import datetime

spark=SparkSession.builder.appName("Q3").getOrCreate()

df=spark.read.parquet("hdfs:///dataframe_yellow.parquet")
start=time.time()
df = df.filter(df.PULocationID != df.DOLocationID)
df = df.filter(f.year(f.col("tpep_pickup_datetime")) == 2022)
w = df.groupBy(f.window("tpep_pickup_datetime", "15 days",startTime="2 days 22 hours")).agg(f.avg("total_amount").alias('avg_total_amount'),f.avg("trip_distance").alias('avg_trip_distance'))
w = w.select(w.window.start.cast("string").alias("start"), w.window.end.cast("string").alias("end"), "avg_total_amount","avg_trip_distance")
w = w.sort(f.col("start"))
w.write.parquet("hdfs:///Q/Q3.parquet")
#w.collect()
end=time.time()
#w.show(50)

print()
print()
print(f'Time taken: {end-start} seconds.')
print()
print()

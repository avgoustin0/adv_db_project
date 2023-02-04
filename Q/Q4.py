import pyspark
import pyspark.sql.functions as f
import os, sys, time
from pyspark.sql import SparkSession
from pyspark.sql.functions import rank
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
spark=SparkSession.builder.appName("Q4").getOrCreate()

df=spark.read.parquet("hdfs:///parquet/")

start=time.time()
df = df.withColumn("month",f.month(f.col("tpep_pickup_datetime")))
df=df.withColumn("week_day_full", f.date_format(f.col("tpep_pickup_datetime"), "EEEE"))

windowDept = Window.partitionBy("month","week_day_full").orderBy(col("passenger_count").desc())
df=df.withColumn("rank",rank().over(windowDept)) 
df=df.filter(col("rank")<=3)
df=df.select(f.col("month"),f.col("week_day_full"),f.col("tpep_pickup_datetime"),f.col("tpep_dropoff_datetime"),f.col("Passenger_count"))
df = df.withColumn('Hour', f.hour(df.tpep_pickup_datetime))
#df.write.parquet("hdfs:///Q/Q4.parquet")
end=time.time()
df.show()

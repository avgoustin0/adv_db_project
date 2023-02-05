import pyspark
import pyspark.sql.functions as f
import os, sys, time
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
#from pyspark.sql.functions import rank

spark=SparkSession.builder.appName("Q5").getOrCreate()

df=spark.read.parquet("hdfs:///dataframe_yellow.parquet")
c_df=spark.read.format("csv").option("seperator",",").option("header",True).option("inferSchema","true").load("hdfs:///taxi+_zone_lookup.csv")

start=time.time()
df = df.withColumn("Percentage",(f.col("tip_amount")/f.col("fare_amount")*100))
df = df.withColumn("month",f.month(f.col("tpep_pickup_datetime")))
window = Window.partitionBy("month").orderBy(f.col("Percentage").desc())
df=df.withColumn("rank",f.rank().over(window))
df = df.filter(f.col("rank")<=5).select(f.col("month"), f.col("tpep_pickup_datetime"),f.col("tpep_dropoff_datetime"),f.col("tip_amount"),f.col("fare_amount"),f.col("Percentage"))
df.write.parquet("hdfs:///Q/Q5.parquet")
#df.collect()
end=time.time()

#df.show(100)

print()
print()
print(f'Time taken: {end-start} seconds.')
print()
print()


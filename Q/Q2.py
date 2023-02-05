import pyspark
import pyspark.sql.functions as f
import os, sys,time
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Q2").getOrCreate()
df = spark.read.parquet("hdfs:///dataframe_yellow.parquet")

start = time.time()
df = df.groupBy(f.month(f.col("tpep_pickup_datetime"))).agg(f.max(f.col("Tolls_amount"))).filter(f.col("max(Tolls_amount)")!=0)
df = df.withColumnRenamed("month(tpep_pickup_datetime)", "month").withColumnRenamed("max(Tolls_amount)", "max_Tolls")
df.write.parquet("hdfs:///Q/Q2.parquet")
#df.write.format("csv").options(separator=',').save("hdfs:///Q/Q2.csv")
#df.collect()
end = time.time()
#df.show(50)

print()
print()
print(f'Time taken: {end-start} seconds.')
print()
print()

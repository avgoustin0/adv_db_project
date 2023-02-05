import pyspark
import calendar
import os, sys, time
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Q3_rdd").getOrCreate()

df=spark.read.parquet("hdfs:///dataframe_yellow.parquet")
rdd = df.rdd

def convert(row):
    day = row.tpep_pickup_datetime.day
    month = row.tpep_pickup_datetime.month
  
    if abs(day) <= 15:
        key = calendar.month_name[month] + " 0 - 15"
    else:
        key = calendar.month_name[month] + " 15 - end month"
    total = abs(row.total_amount)
    distance = abs(row.trip_distance) 
    return (str(key), (total,1,distance))
start=time.time()

rdd = rdd.filter(lambda x: x.PULocationID != x.DOLocationID)
rdd = rdd.filter(lambda x: x.tpep_pickup_datetime.year == 2022)
rdd1 = rdd.map(convert)
#for y in rdd1.take(20):
#        print(y)
rdd1 = rdd1.reduceByKey(lambda x,y : (x[0]+y[0], x[1] + y[1], x[2] + y[2])).mapValues(lambda x: (x[0]/x[1], x[2]/x[1]))
rdd1.saveAsTextFile("hdfs:///Q/Q3_rdd.txt")
#rdd1.collect()
end=time.time()
#for y in rdd1.take(20):
#        print(y)

print()
print()
print(f'Time taken: {end-start} seconds.')
print()
print()

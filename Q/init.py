import spark 
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("init").getOrCreate()

df = spark.read.parquet("hdfs:///parquet/")
df.write.parquet("hdfs:///dataframe_yellow.parquet")
rdd = df.rdd
rdd.saveAsTextFile("hdfs:///rdd_yellow.txt")
c_df = spark.read.format("csv").option("separator", ",").option("header", True).option("inferSchema", "true").load("hdfs:///taxi+_zone_lookup.csv")
c_df = c_df.write.parquet("hdfs:///dataframe_lookup.parquet")
c_df = spark.read.parquet("hdfs:///dataframe_lookup.parquet")
c_rdd = c_df.rdd
c_rdd.saveAsTextFile("hdfs:///rdd_lookup.txt")
